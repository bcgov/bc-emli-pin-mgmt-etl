from notifications_python_client.notifications import NotificationsAPIClient
from notifications_python_client import prepare_upload

def gc_notify_log(api_key, base_url, email_address, template_id, file_path, personalisation):
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
        notifications_client = NotificationsAPIClient(api_key=api_key, base_url=base_url)

        # Open the file for uploading
        with open(file_path, 'rb') as f:
            # Attach the log file to personalisation data
            personalisation['link_to_file'] = prepare_upload(f)

        # Send the email notification
        response = notifications_client.send_email_notification(
            email_address=email_address,
            template_id=template_id,
            personalisation=personalisation
        )

        return response

    except Exception as e:
        # Handle any exceptions (e.g., API errors) here
        raise e

def run(api_key, base_url, email_address, template_id, file_path, personalisation):
    """
    Run the email notification process with provided parameters.

    Args:
        api_key (str): Your GC Notify API key.
        base_url (str): The base URL of the GC Notify API.
        email_address (str): The recipient's email address.
        template_id (str): The ID of the email template to use.
        file_path (str): The path to the log file to attach.
        personalisation (dict): Personalization data for the email.
    """
    # Send the email notification
    response = gc_notify_log(api_key, base_url, email_address, template_id, file_path, personalisation)
    print(response)  # Print the response from GC Notify

if __name__ == "__main__":
    # Define your API key, base URL, email address, template ID, file path, and personalization data
    api_key = 'your_api_key'
    base_url = 'https://api.notification.canada.ca'
    email_address = 'recipient@example.com'
    template_id = 'your_template_id'
    file_path = '/path/to/your/logfile.log'
    personalisation = {
        'first_name': 'Amala',
        'Status': '2018-01-01'
    }

    # Run the email notification process with the specified parameters
    run(api_key, base_url, email_address, template_id, file_path, personalisation)
