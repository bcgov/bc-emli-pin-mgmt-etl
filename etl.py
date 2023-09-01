from sftp import sftp_download
import config as cn
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


args = parser.parse_args()

print(
    f"DAYS_BACK: {cn.days_back}, REMOTE_PATH: {cn.remote_path}, LOCAL_PATH: {cn.local_path}"
)

# Step 1: Download the SFTP files to the PVC
sftp_download.run(
    args.host,
    args.port,
    args.username,
    args.password,
    cn.days_back,
    cn.remote_path,
    cn.local_path,
)
