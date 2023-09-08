from sftp import sftp_download
from sftp import sftp_parse
import argparse

parser = argparse.ArgumentParser(
    prog="BC PVS ETL Job",
    description="This is the ETL job that expires PINS.",
    epilog="Please check the repo: https://github.com/bcgov/bc-emli-pin-mgmt-etl",
)

# Host argument
parser.add_argument("--host", type=str, help="Host address.")

# Port argument
parser.add_argument("--port", type=int, help="Port number.")

# Username argument
parser.add_argument("--username", type=str, help="Username of the SFTP login.")

# Password argument
parser.add_argument("--password", type=str, help="Password of the SFTP login.")

# Password argument
parser.add_argument(
    "--remote_path",
    type=str,
    help="Remote path of the SFTP folder to copy the files from.",
)

# Password argument
parser.add_argument(
    "--local_path", type=str, help="Local folder path to download the files to."
)

# Remote Output Path argument
parser.add_argument(
    "--processed_data_path",
    type=str,
    help="Remote folder path to download the processed output data files to.",
)


args = parser.parse_args()

# Step 1: Download the SFTP files to the PVC
sftp_download.run(
    args.host,
    args.port,
    args.username,
    args.password,
    args.remote_path,
    args.local_path,
)

# Step 2: Process the downloaded SFTP files and write to output folder
sftp_parse.parse_sftp_files(args.local_path, args.processed_data_path)
