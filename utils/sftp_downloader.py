import paramiko


def set_sftp_conn(host, port, username, password):
    """
    Set up an SFTP connection.

    Args:
        host (str): The hostname or IP address of the SFTP server.
        port (int): The port number to connect to the SFTP server.
        username (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        paramiko.SFTPClient: An SFTP client object.
    """
    try:
        transport = paramiko.Transport((host, port))
        print("Connecting to SFTP...")
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("Connection established...")
        return sftp

    except Exception as e:
        raise e


def get_files_to_download_from_sftp(sftp, remote_path):
    """
    Get the list of files to download from the SFTP server.

    Args:
        sftp (paramiko.SFTPClient): An SFTP client object.
        remote_path (str): The remote directory path on the SFTP server.

    Returns:
        list of str: A list of filenames to download.
    """
    try:
        files_to_download = sftp.listdir(remote_path)
        if not files_to_download:
            print("No new files uploaded...")
        return files_to_download
    except Exception as e:
        raise e


def download_files_from_sftp(sftp, files_list, remote_path, local_path):
    """
    Download files from SFTP server to the local directory.

    Args:
        sftp (paramiko.SFTPClient): An SFTP client object.
        files_list (list of str): A list of filenames to download.
        remote_path (str): The remote directory path on the SFTP server.
        local_path (str): The local directory path where files will be downloaded.
    """
    try:
        for file in files_list:
            remote_file_path = remote_path + file
            local_file_path = local_path + file
            sftp.get(remote_file_path, local_file_path)
            print(f"Downloaded: {remote_file_path} -> {local_file_path}")

    except Exception as e:
        raise e


def run(host, port, username, password, remote_path, local_path):
    """
    Main function to establish an SFTP connection and download files.

    Args:
        host (str): The hostname or IP address of the SFTP server.
        port (int): The port number to connect to the SFTP server.
        username (str): The username for authentication.
        password (str): The password for authentication.
        remote_path (str): The remote directory path on the SFTP server.
        local_path (str): The local directory path where files will be downloaded.
    """
    try:
        # Establish SFTP connection
        sftp_conn = set_sftp_conn(host, port, username, password)

        files_to_download = get_files_to_download_from_sftp(sftp_conn, remote_path)

        # Download the files
        download_files_from_sftp(sftp_conn, files_to_download, remote_path, local_path)

    except Exception as e:
        print(f"Error downloding LTSA files: {str(e)}")
        raise e


if __name__ == "__main__":
    host = "your_host"
    port = 22  # Replace with your port
    username = "your_username"
    password = "your_password"
    remote_path = "/remote/path/"
    local_path = "/local/path/"

    run(host, port, username, password, remote_path, local_path)
