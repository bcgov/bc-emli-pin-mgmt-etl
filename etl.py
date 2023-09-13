from utils import sftp_downloader, postgres_writer, sftp_parser
import argparse

parser = argparse.ArgumentParser(
    prog="BC PVS ETL Job",
    description="This is the ETL job that expires PINS.",
    epilog="Please check the repo: https://github.com/bcgov/bc-emli-pin-mgmt-etl",
)

# Host argument
parser.add_argument("--sftp_host", type=str, help="Host address.")

# Port argument
parser.add_argument("--sftp_port", type=int, help="Port number.")

# Username argument
parser.add_argument("--sftp_username", type=str, help="Username of the SFTP login.")

# Password argument
parser.add_argument("--sftp_password", type=str, help="Password of the SFTP login.")

# Remote path argument
parser.add_argument(
    "--sftp_remote_path",
    type=str,
    help="Remote path of the SFTP folder to copy the files from.",
)

# Local path argument
parser.add_argument(
    "--sftp_local_path", type=str, help="Local folder path to download the files to."
)

# Output folder for the processed csv files
parser.add_argument(
    "--processed_data_path",
    type=str,
    help="Local output folder for the processed csv files.",
)


# db_host argument
parser.add_argument("--db_host", type=str, help="Host name of the PostgresDB.")

# db_port argument
parser.add_argument(
    "--db_port", type=int, default=5432, help="Port number of the Postgres DB."
)

# db_username argument
parser.add_argument("--db_username", type=str, help="Username of the SFTP login.")

# db_password argument
parser.add_argument("--db_password", type=str, help="Password of the SFTP login.")

# db_name argument
parser.add_argument("--db_name", type=str, help="Name of the DB in the Postgres DB.")

# db_password argument
parser.add_argument(
    "--db_write_batch_size", type=int, default=1000, help="Password of the SFTP login."
)

args = parser.parse_args()

# Step 1: Download the SFTP files to the PVC
sftp_downloader.run(
    args.sftp_host,
    args.sftp_port,
    args.sftp_username,
    args.sftp_password,
    args.sftp_remote_path,
    args.sftp_local_path,
)

# Step 2: Process the downloaded SFTP files and write to output folder
sftp_parser.parse_sftp_files(args.local_path, args.processed_data_path)


# Step 3: Write the above processed data to the PostgreSQL database
postgres_writer.run(
    input_directory=args.processed_data_path,
    database_name=args.db_name,
    batch_size=args.db_write_batch_size,
    host=args.db_host,
    port=args.db_port,
    user=args.db_username,
    password=args.db_password,
)