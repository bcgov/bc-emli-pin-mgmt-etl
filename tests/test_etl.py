from pathlib import Path
from unittest.mock import patch
import argparse
from etl import (
    send_email_notification,
    get_status_from_etl_log_table,
    insert_status_into_etl_log_table,
    update_status_in_etl_log_table,
    delete_rows_with_job_id,
    main,
)
from sqlalchemy import create_engine
import pytest

apiKey = "api-key"
baseUrl = "base-url"
emailAddress = "test@test.ca"
templateId = "template-id"
logFolder = ""
logFilename = "filename.log"
startTime = "100"
status = "Success"
errorMessage = ""

db = create_engine("sqlite:///:memory:")
folder = "folder"

tableColumn = {"table": "column"}


@patch("utils.gc_notify.gc_notify_log")
def test_send_email_notification(gcNotify_mock):
    send_email_notification(
        apiKey,
        baseUrl,
        emailAddress,
        templateId,
        logFolder,
        logFilename,
        startTime,
        status,
        errorMessage,
    )
    assert gcNotify_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
def test_get_status_from_etl_log_table(connect_mock):
    get_status_from_etl_log_table(db, folder)
    assert connect_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
def test_insert_status_into_etl_log_table(connect_mock):
    insert_status_into_etl_log_table(db, status)
    assert connect_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
def test_update_status_in_etl_log_table_without_folder(connect_mock):
    update_status_in_etl_log_table(db, 123, status)
    assert connect_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
def test_update_status_in_etl_log_table_with_folder(connect_mock):
    update_status_in_etl_log_table(db, 123, status, "folder")
    assert connect_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
def test_delete_rows_with_job_id(connect_mock):
    delete_rows_with_job_id(db, tableColumn, 123)
    assert connect_mock.called_once()


@patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        log_folder="log_folder",
        db_username="username",
        db_password="password",
        db_host="host",
        db_port=1234,
        db_name="name",
        sftp_host="sftp_host",
        sftp_port=1235,
        sftp_username="sftp_username",
        sftp_password="sftp_password",
        sftp_remote_path="sftp_remote_path",
        sftp_local_path="sftp_local_path",
        processed_data_path="processed_data_path",
        data_rules_url="data_rules_url",
        db_write_batch_size=100,
        expire_api_url="expire_api_url",
        vhers_api_key="vhers_api_key",
        api_key="api_key",
        base_url="base_url",
        email_address="emailAddress",
        template_id="templateId",
    ),
)
@patch("sqlalchemy.engine.Engine.connect")
@patch("utils.logging_config.setup_logging")
@patch("utils.logging_config.LoggerStream")
@patch("utils.sftp_downloader.run")
def test_main_run_status_success(
    parser_mock, connect_mock, loggingSetup_mock, loggerStream_mock, sftpDownloader_mock
):
    main()
    assert connect_mock.called_once()
    assert connect_mock.called_once()
    assert parser_mock.called_once()
    assert connect_mock.called_once()
    assert loggingSetup_mock.called_once()
    assert loggerStream_mock.called_once()
    assert sftpDownloader_mock.called_once()


@patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(
        log_folder="log_folder",
        db_username="username",
        db_password="password",
        db_host="host",
        db_port=1234,
        db_name="name",
        sftp_host="sftp_host",
        sftp_port=1235,
        sftp_username="sftp_username",
        sftp_password="sftp_password",
        sftp_remote_path="sftp_remote_path",
        sftp_local_path="sftp_local_path",
        processed_data_path="processed_data_path",
        data_rules_url="data_rules_url",
        db_write_batch_size=100,
        expire_api_url="expire_api_url",
        vhers_api_key="vhers_api_key",
        api_key="api_key",
        base_url="base_url",
        email_address="emailAddress",
        template_id="templateId",
    ),
)
@patch("sqlalchemy.engine.Engine.connect")
@patch("utils.logging_config.setup_logging")
@patch("utils.logging_config.LoggerStream")
@patch("utils.sftp_downloader.run")
@patch("etl.get_status_from_etl_log_table", return_value=None)
@patch("utils.ltsa_parser.run")
@patch("utils.postgres_writer.run")
@patch("utils.pin_expirer.run")
def test_main_run_status_none(
    parser_mock,
    connect_mock,
    loggingSetup_mock,
    loggerStream_mock,
    sftpDownloader_mock,
    getStatus_mock,
    ltsaParser_mock,
    postgresWriter_mock,
    pinExpirer_mock,
):
    main()
    assert connect_mock.called_once()
    assert parser_mock.called_once()
    assert connect_mock.called_once()
    assert loggingSetup_mock.called_once()
    assert loggerStream_mock.called_once()
    assert sftpDownloader_mock.called_once()
    assert getStatus_mock.called_once()
    assert ltsaParser_mock.called_once()
    assert postgresWriter_mock.called_once()
    assert pinExpirer_mock.called_once()
