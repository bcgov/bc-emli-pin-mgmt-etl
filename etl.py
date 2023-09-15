from utils import ltsa_parser, sftp_downloader, postgres_writer
import argparse

parser = argparse.ArgumentParser(
    prog="BC PVS ETL Job",
    description="This is the ETL job that expires PINS.",
    epilog="Please check the repo: https://github.com/bcgov/bc-emli-pin-mgmt-etl",
)

# sftp_host argument
parser.add_argument(
    "--sftp_host", type=str, help="Host address of the LTSA SFTP server."
)

# sftp_port argument
parser.add_argument(
    "--sftp_port", type=int, default=22, help="Port number of the LTSA sftp server."
)

# sftp_username argument
parser.add_argument("--sftp_username", type=str, help="Username of the SFTP login.")

# sftp_password argument
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
    "--db_write_batch_size",
    type=int,
    default=1000,
    help="Number of records to be written to the db in one batch.",
)

# data_rules_url argument
parser.add_argument(
    "--data_rules_url",
    type=str,
    help="URL to the data_rules.json file in a public GitHub repository.",
)

args = parser.parse_args()

# Step 1: Download the SFTP files to the PVC
sftp_downloader.run(
    host=args.sftp_host,
    port=args.sftp_port,
    username=args.sftp_username,
    password=args.sftp_password,
    remote_path=args.sftp_remote_path,
    local_path=args.sftp_local_path,
)

# Step 2: Process the downloaded SFTP files and write to output folder
ltsa_parser.run(
    input_directory=args.sftp_local_path,
    output_directory=args.processed_data_path,
    data_rules_url=args.data_rules_url,
)

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
