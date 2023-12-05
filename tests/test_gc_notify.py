import os
from unittest.mock import patch
import notifications_python_client
import pytest
from utils.gc_notify import run, gc_notify_log


api_key = "gcntfy-vhers_test-bf3f9c9f-fcfe-45e3-aea9-bae75f93d741-ff417991-77fc-4e67-9e20-853363ef6a7e"
base_url = "https://api.notification.canada.ca"
email_address = "test@test.ca"
template_id = "templateId"
file_path = "testfile.txt"
personalisation = {
    "file": "file",
    "filename": "filename",
    "sending_method": "attach",
}

file = "file contents"


@patch(
    "notifications_python_client.notifications.NotificationsAPIClient.send_email_notification",
    return_value="gc_notify_response",
)
def test_gc_notify_log(sendEmailNotification_mock):
    with open("testfile.txt", "w") as f:
        f.write("File contents")
    response = gc_notify_log(
        api_key, base_url, email_address, template_id, file_path, personalisation
    )
    print(response)
    assert sendEmailNotification_mock.called_once()
    assert response == "gc_notify_response"
    os.remove("testfile.txt")


# without mocking send_email_notification, call should fail due to incorrect template_id
def test_gc_notify_log_error():
    with open("testfile.txt", "w") as f:
        f.write("File contents")
    with pytest.raises(notifications_python_client.errors.HTTPError):
        response = gc_notify_log(
            api_key,
            base_url,
            email_address,
            "incorrectTemplateId",
            file_path,
            personalisation,
        )
        assert response == None
    os.remove("testfile.txt")


@patch("utils.gc_notify.gc_notify_log", return_value="gc_notify_response")
def test_run(gcNotify_mock):
    run(api_key, base_url, email_address, template_id, file_path, personalisation)
    assert gcNotify_mock.called_once()


@patch(
    "utils.gc_notify.gc_notify_log",
    return_value=notifications_python_client.errors.HTTPError,
)
def test_run_error(gcNotify_mock):
    with pytest.raises(TypeError):
        run(api_key, base_url, email_address, template_id, file_path, personalisation)
    assert gcNotify_mock.called_once()
