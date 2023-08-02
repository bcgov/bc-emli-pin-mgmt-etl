import paramiko
from datetime import datetime, timedelta
import traceback
import config as cn

def set_sftp_conn(host, port, username, password):
    """ set sftp connection to get the files, using config.py """
    # connect to sftp
    transport = paramiko.Transport((host, port))
    print("connecting to SFTP...")
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("connection established.")
    return sftp

def get_files_to_download_from_sftp(sftp, date_limit, remote_path):
    """ get the list of files to download from the SFTP server"""
    # get files in directory
    all_files = sftp.listdir(remote_path)
    files_to_download = []
    # check for new files above date limit
    try:
        for f in all_files:
            last_modified = sftp.stat(remote_path + f).st_mtime
            last_modified_date = datetime.fromtimestamp(last_modified).date()   
            if last_modified_date > date_limit:  # check limit
                if sftp.stat(remote_path + f).st_size > 500:  # check if file is empty (in this case larger than 1MB)
                    files_to_download.append(f)
        return files_to_download
    except:
        trace_error = traceback.format_exc()
        print('SOMETHING IS WRONG DIDNT GET THE FILES\n' + trace_error)

def download_files_from_sftp(sftp, files_list, remote_path, local_path = "/Users/anayanapalli/test/"):
    for file in files_list:
        sftp.get(remote_path+file, local_path+file)
        print(f"DOWNLOADED----------------{remote_path+file}")

def main():    
    # set date limit for files
    days_back = cn.days_back
    date_limit = datetime.date(datetime.now()) - timedelta(days=days_back)

    # set from & to paths for files to download
    remote_path = cn.remote_path
    local_path = cn.local_path
    
    # establish sftp connection
    sftp_conn = set_sftp_conn(cn.host, cn.port, cn.username, cn.password)    
    files_to_download = get_files_to_download_from_sftp(sftp_conn, date_limit, remote_path)
    
    # download the files
    download_files_from_sftp(sftp_conn, files_to_download, remote_path, local_path)

if __name__ == "__main__":
    main()