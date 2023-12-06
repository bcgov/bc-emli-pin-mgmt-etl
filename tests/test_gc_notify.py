import os
from unittest.mock import patch
import notifications_python_client
import pytest
from utils.gc_notify import run, gc_notify_log


api_key = "test-api-key"
base_url = "test-base-url"
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
    "notifications_python_client.notifications.NotificationsAPIClient.__init__",
    return_value=None,
)
@patch(
    "notifications_python_client.notifications.NotificationsAPIClient.send_email_notification",
    return_value="gc_notify_response",
)
def test_gc_notify_log(notificationsAPIClient_mock, sendEmailNotification_mock):
    with open("testfile.txt", "w") as f:
        f.write("File contents")
    response = gc_notify_log(
        api_key, base_url, email_address, template_id, file_path, personalisation
    )
    print(response)
    assert sendEmailNotification_mock.called_once()
    assert notificationsAPIClient_mock.called_once()
    assert response == "gc_notify_response"
    os.remove("testfile.txt")


@patch(
    "notifications_python_client.notifications.NotificationsAPIClient.__init__",
    return_value=None,
)
def test_gc_notify_log_error(notificationsAPIClient_mock):
    with open("testfile.txt", "w") as f:
        f.write("File contents")
    with pytest.raises(AttributeError):
        response = gc_notify_log(
            api_key,
            base_url,
            email_address,
            "incorrectTemplateId",
            file_path,
            personalisation,
        )
        assert response == None
        assert notificationsAPIClient_mock.called_once()
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
