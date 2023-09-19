import os
from notifications_python_client.notifications import NotificationsAPIClient
import base64
import json


def gc_notify_log(
    api_key, base_url, email_address, template_id, file_path, personalisation
):
    """
    Send an email notification with a log file attachment using GC Notify.

    Args:
        api_key (str): Your GC Notify API key.
        base_url (str): The base URL of the GC Notify API.
        email_address (str): The recipient's email address.
        template_id (str): The ID of the email template to use.
        file_path (str): The path to the log file to attach.
        personalisation (dict): Personalization data for the email.

    Returns:
        dict: The response from the GC Notify API.
    """
    try:
        # Create a NotificationsAPIClient instance
        notifications_client = NotificationsAPIClient(
            api_key=api_key, base_url=base_url
        )

        # Read and base64 encode the file
        with open(file_path, "rb") as f:
            file_data = f.read()
            encoded_file = base64.b64encode(file_data).decode("utf-8")

        # Create a dictionary for the file attachment
        file_attachment = {
            "file": encoded_file,
            "filename": os.path.basename(file_path),
            "sending_method": "attach",
        }

        # Add the file attachment to personalisation data
        personalisation["link_to_file"] = file_attachment

        # Send the email notification
        response = notifications_client.send_email_notification(
            email_address=email_address,
            template_id=template_id,
            personalisation=personalisation,
        )

        return response

    except Exception as e:
        # Handle any exceptions (e.g., API errors) here
        raise e


def run(api_key, base_url, email_address, template_id, file_path, personalisation):
    try:
        # Send the email notification
        response = gc_notify_log(
            api_key, base_url, email_address, template_id, file_path, personalisation
        )

        # Pretty print the JSON response
        print(json.dumps(response, indent=4))

    except Exception as e:
        # Handle any exceptions (e.g., API errors) here
        raise e


if __name__ == "__main__":
    # Define your API key, base URL, email address, template ID, file path, and personalization data
    api_key = "YOUR_API_KEY"
    base_url = "https://api.notification.canada.ca"
    email_address = "recipient@example.com"
    template_id = "YOUR_TEMPLATE_ID"
    file_path = "/path/to/logfile.log"
    personalisation = {"name": "Amala", "value": "2018-01-01"}

    # Run the email notification process with the specified parameters
    run(api_key, base_url, email_address, template_id, file_path, personalisation)
