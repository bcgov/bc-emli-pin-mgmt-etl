from sftp import sftp
import argparse

parser = argparse.ArgumentParser(
    prog="BC PVS ETL Job",
    description="This is the ETL job that expires PINS.",
    epilog="Please check the repo: https://github.com/bcgov/bc-emli-pin-mgmt-etl",
)

args = parser.parse_args()

sftp.main()

# host = "test.rebex.net"
# port = 22
# username = "demo"
# password = "password"
# days_back = 20 * 365
# remote_path = "/pub/example/"
# local_path = "./data/"
