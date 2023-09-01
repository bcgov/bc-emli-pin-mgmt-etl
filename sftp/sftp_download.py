import paramiko
from datetime import datetime, timedelta
import traceback


def set_sftp_conn(host, port, username, password):
    """set sftp connection to get the files, using config.py"""
    # connect to sftp
    transport = paramiko.Transport((host, port))
    print("connecting to SFTP...")
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("CONNECTION ESTABLISHED...")
    return sftp


def get_files_to_download_from_sftp(sftp, remote_path):
    """get the list of files to download from the SFTP server"""
    # get files in directory
    files_to_download = sftp.listdir(remote_path)
    if len(files_to_download) <= 0:
        print("NOW NEW FILES UPLOADED...")
    return files_to_download


def download_files_from_sftp(sftp, files_list, remote_path, local_path):
    for file in files_list:
        sftp.get(remote_path + file, local_path + file)
        print(f"DOWNLOADED----------------{remote_path+file}")


def run(host, port, username, password, remote_path, local_path):

    # establish sftp connection
    sftp_conn = set_sftp_conn(host, port, username, password)

    files_to_download = get_files_to_download_from_sftp(sftp_conn, remote_path)

    # download the files
    download_files_from_sftp(sftp_conn, files_to_download, remote_path, local_path)
