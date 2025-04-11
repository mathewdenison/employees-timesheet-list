import os
import json
import logging
import base64

from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging

from timesheet_common_timesheet_mfdenison_hopkinsep.models import TimeLog
from timesheet_common_timesheet_mfdenison_hopkinsep.serializers import TimeLogSerializer
from timesheet_common_timesheet_mfdenison_hopkinsep.utils.dashboard import send_dashboard_update

client = cloud_logging.Client()
client.setup_logging()

logger = logging.getLogger("employee_timelog_list")
logger.setLevel(logging.INFO)

PROJECT_ID = os.environ.get("PROJECT_ID", "hopkinstimesheetproj")
DASHBOARD_TOPIC = f"projects/{PROJECT_ID}/topics/dashboard-queue"
# (The publisher is not used directly in this function because we are assuming that send_dashboard_update handles it.)
# But if needed, you can also instantiate one:
publisher = pubsub_v1.PublisherClient()


def employee_timelog_list(event, context):
    """
    Cloud Function to perform bulk timesheet lookup triggered by a Pub/Sub message.

    The function performs the following steps:
      1. Decodes the incoming Pub/Sub message (handling potential double JSON encoding).
      2. Retrieves all TimeLog objects via the shared TimeLog model.
      3. Serializes the list using TimeLogSerializer.
      4. Groups the timelogs by employee_id.
      5. Sends a dashboard update with the grouped timelogs using the shared utility.

    If the function completes without errors, the message is automatically acknowledged.

    Args:
        event (dict): The Pub/Sub event payload (with base64-encoded "data").
        context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        # Decode the Pub/Sub message data from base64.
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        # First, decode the JSON payload.
        first_pass = json.loads(raw_data)
        # If the decoded object is still a string (double-encoded), parse it again.
        data = json.loads(first_pass) if isinstance(first_pass, str) else first_pass

        logger.info("Fetching all employee timesheet objects...")
        # Retrieve all TimeLog objects (using your shared model's query manager, which may be implemented similarly to Django ORM).
        logs = TimeLog.objects.all()
        serializer = TimeLogSerializer(logs, many=True)
        msg_str = f"Bulk Timesheet Lookup: found {len(serializer.data)} timelog records."
        logger.info(msg_str)

        # Group the timelogs by employee_id (or 'employee' if that key exists).
        grouped_logs = {}
        for timesheet in serializer.data:
            emp_id = timesheet.get("employee_id") or timesheet.get("employee")
            if emp_id is not None:
                emp_id = str(emp_id)
                if emp_id not in grouped_logs:
                    grouped_logs[emp_id] = []
                grouped_logs[emp_id].append(timesheet)

        # Send the dashboard update using the shared utility.
        # This utility function is responsible for publishing to the dashboard Pub/Sub topic.
        send_dashboard_update("all", "bulk_timelog_lookup", {"timelogs": grouped_logs, "message": msg_str})
        logger.info("Sent dashboard update with grouped employee timelogs.")

    except Exception as e:
        logger.exception(f"Error processing employee_timelog_list message: {str(e)}")
        # Raising an exception causes the Cloud Function to fail and trigger a retry.
        raise
