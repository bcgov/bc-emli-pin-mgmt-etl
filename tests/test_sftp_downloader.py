from argparse import Namespace
from unittest.mock import patch
import paramiko
from utils.sftp_downloader import (
    run,
    set_sftp_conn,
    get_files_to_download_from_sftp,
    download_files_from_sftp,
)
import pytest

host = "host"
port = 1234
username = "username"
password = "password"
sftp = paramiko.SFTPClient
remotePath = ""
localPath = ""
filePathDict = {
    "file": "file.txt",
    "file_path": "file_path",
    "folder_path": "folder_path",
}


@patch("paramiko.Transport")
@patch("paramiko.SFTPClient.from_transport", return_value=sftp)
def test_set_sftp_conn(transport_mock, fromTransport_mock):
    sftp_connection = set_sftp_conn(host, port, username, password)
    assert sftp_connection == sftp
    assert transport_mock.called_once()
    assert fromTransport_mock.called_once()


@patch("paramiko.Transport")
def test_set_sftp_conn_error(transport_mock):
    with pytest.raises(TypeError):
        set_sftp_conn(host, port, username, password)
        assert transport_mock.called_once()


@patch("paramiko.Transport")
@patch("paramiko.SFTPClient.from_transport", return_value=sftp)
@patch(
    "paramiko.SFTPClient.listdir_attr",
    return_value=[Namespace(st_mtime=12345, filename=None)],
)
def test_get_files_to_download_from_sftp(
    transport_mock, fromTransport_mock, listdir_mock
):
    get_files_to_download_from_sftp(sftp, remotePath)
    assert transport_mock.called_once()
    assert fromTransport_mock.called_once()
    assert listdir_mock.called_once()


@patch("paramiko.Transport")
def test_get_files_to_download_from_sftp_error(transport_mock):
    with pytest.raises(AttributeError):
        get_files_to_download_from_sftp(sftp, remotePath)


@patch("paramiko.SFTPClient.get")
def test_download_files_from_sftp(get_mock):
    download_files_from_sftp(sftp, filePathDict, localPath)
    assert get_mock.called_once()


def test_download_files_from_sftp_error():
    with pytest.raises(KeyError):
        download_files_from_sftp(sftp, filePathDict, localPath)


@patch("utils.sftp_downloader.set_sftp_conn")
@patch("utils.sftp_downloader.get_files_to_download_from_sftp")
@patch("utils.sftp_downloader.download_files_from_sftp")
def test_run(set_mock, get_mock, download_mock):
    run(host, port, username, password, remotePath, localPath)
    assert set_mock.called_once()
    assert get_mock.called_once()
    assert download_mock.called_once()


@patch("utils.sftp_downloader.set_sftp_conn")
def test_run_error(set_mock):
    with pytest.raises(KeyError):
        run(host, port, username, password, remotePath, localPath)
    assert set_mock.called_once()
