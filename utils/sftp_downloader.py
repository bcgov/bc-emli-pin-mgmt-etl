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

    Returns:
        list of str: A list of filenames to download.
    """
    try:
        latest = 0
        latestfolder = None
        file_path_dict = {}

        for fileattr in sftp.listdir_attr(remote_path):
            # Compare folder timestamps for each file in directory to find latest folder
            if fileattr.st_mtime > latest:
                latest = fileattr.st_mtime
                latestfolder = fileattr.filename

        if latestfolder is not None:
            innerLatestFolder = latestfolder.replace("_Weekly", "")
            files_to_download = sftp.listdir(
                f"{remote_path}{latestfolder}/"
            )

            for file in files_to_download:
                file_path_dict[file] = (
                    f"{remote_path}{latestfolder}/{file}"
                )

            file_path_dict["folder_path"] = (
                f"{remote_path}{latestfolder}/"
            )

        else:
            print("No new files uploaded...")

        return file_path_dict
    except Exception as e:
        raise e


def download_files_from_sftp(sftp, file_path_dict, local_path):
    """
    Download files from SFTP server to the local directory.

    Args:
        sftp (paramiko.SFTPClient): An SFTP client object.
        file_path_dict (dict): Key is file name to download, value is full file path to download.
        local_path (str): The local directory path where files will be downloaded.
    """
    try:
        folder_path = file_path_dict["folder_path"]
        del file_path_dict["folder_path"]

        for file, file_path in file_path_dict.items():
            remote_file_path = file_path
            local_file_path = local_path + file
            sftp.get(remote_file_path, local_file_path)
            print(f"Downloaded: {remote_file_path} -> {local_file_path}")

        return folder_path

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

    Returns:
        folder_path (str): Name of latest remote folder in SFTP server.
    """
    try:
        # Establish SFTP connection
        sftp_conn = set_sftp_conn(host, port, username, password)

        file_path_dict = get_files_to_download_from_sftp(sftp_conn, remote_path)

        # Download the files
        folder_path = download_files_from_sftp(sftp_conn, file_path_dict, local_path)

        return folder_path

    except Exception as e:
        print(f"Error downloding LTSA files: {str(e)}")
        raise e


if __name__ == "__main__":  # pragma: no cover
    host = "your_host"
    port = 22  # Replace with your port
    username = "your_username"
    password = "your_password"
    remote_path = "/remote/path/"
    local_path = "/local/path/"

    run(host, port, username, password, remote_path, local_path)
