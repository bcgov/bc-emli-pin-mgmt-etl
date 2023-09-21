import argparse
import logging
import os
import sys
from datetime import datetime
from utils import ltsa_parser, sftp_downloader, postgres_writer, pin_expirer
from utils.gc_notify import gc_notify_log


def setup_logging(log_folder, log_filename):
    """
    Set up logging to redirect stdout and stderr to log files.

    Args:
        log_folder (str): The folder where the log file should be created.
        log_filename (str): The name of the log file.

    Returns:
        None
    """
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    log_path = os.path.join(log_folder, log_filename)
    logging.basicConfig(filename=log_path, level=logging.INFO)
    stdout_logger = logging.getLogger("STDOUT")
    stderr_logger = logging.getLogger("STDERR")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stderr_handler = logging.StreamHandler(sys.stderr)

    stdout_logger.addHandler(stdout_handler)
    stderr_logger.addHandler(stderr_handler)

    sys.stdout = stdout_logger
    sys.stderr = stderr_logger


def send_email_notification(
    api_key,
    base_url,
    email_address,
    template_id,
    log_folder,
    log_filename,
    status,
    error_message=None,
):
    """
    Send an email notification with a log file attachment.

    Args:
        api_key (str): Your GC Notify API key.
        base_url (str): The base URL of the GC Notify API.
        email_address (str): The recipient's email address.
        template_id (str): The ID of the email template to use.
        log_folder (str): The folder where the log file is located.
        log_filename (str): The name of the log file.
        status (str): The status of the job (e.g., "Success" or "Failure").
        error_message (str): The error message (if applicable).

    Returns:
        None
    """
    try:
        log_path = os.path.join(log_folder, log_filename)
        personalisation = {
            "log_filename": log_filename,
            "job_status": status,
            "error_message": error_message,
        }

        gc_notify_log(
            api_key,
            base_url,
            email_address,
            template_id,
            log_path,
            personalisation,
        )

    except Exception as e:
        logging.error(f"Error sending email notification: {str(e)}")


def main():
    parser = argparse.ArgumentParser(
        prog="BC PVS ETL Job",
        description="This is the ETL job that expires PINS.",
        epilog="Please check the repo: https://github.com/bcgov/bc-emli-pin-mgmt-etl",
    )

    # Add back the previously deleted command-line arguments with comments
    parser.add_argument(
        "--sftp_host", type=str, help="Host address of the LTSA SFTP server."
    )
    parser.add_argument(
        "--sftp_port", type=int, default=22, help="Port number of the LTSA sftp server."
    )
    parser.add_argument("--sftp_username", type=str, help="Username of the SFTP login.")
    parser.add_argument("--sftp_password", type=str, help="Password of the SFTP login.")
    parser.add_argument(
        "--sftp_remote_path",
        type=str,
        help="Remote path of the SFTP folder to copy the files from.",
    )
    parser.add_argument(
        "--sftp_local_path",
        type=str,
        help="Local folder path to download the files to.",
    )
    parser.add_argument(
        "--processed_data_path",
        type=str,
        help="Local output folder for the processed csv files.",
    )
    parser.add_argument("--db_host", type=str, help="Host name of the PostgresDB.")
    parser.add_argument(
        "--db_port", type=int, default=5432, help="Port number of the Postgres DB."
    )
    parser.add_argument("--db_username", type=str, help="Username of the SFTP login.")
    parser.add_argument("--db_password", type=str, help="Password of the SFTP login.")
    parser.add_argument(
        "--db_name", type=str, help="Name of the DB in the Postgres DB."
    )
    parser.add_argument(
        "--db_write_batch_size",
        type=int,
        default=1000,
        help="Number of records to be written to the db in one batch.",
    )
    parser.add_argument(
        "--data_rules_url",
        type=str,
        help="URL to the data_rules.json file in a public GitHub repository.",
    )

    # Add command-line arguments for GC Notify with comments
    parser.add_argument("--api_key", type=str, help="Your GC Notify API key.")
    parser.add_argument(
        "--base_url", type=str, help="The base URL of the GC Notify API."
    )
    parser.add_argument(
        "--email_address", type=str, help="The recipient's email address."
    )
    parser.add_argument(
        "--template_id", type=str, help="The ID of the email template to use."
    )

    # Add command-line arguments for PIN Expiration
    parser.add_argument("--expire_api_url", type=str, help="The Expire PIN API url")
    
    # Add a new command-line argument for log folder
    parser.add_argument(
        "--log_folder",
        type=str,
        default="./data/log/",
        help="Folder where the log file should be created.",
    )

    args = parser.parse_args()
    log_filename = f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    try:
        # Set up logging with the specified log folder and filename
        setup_logging(args.log_folder, log_filename)

        # Step 1: Download the SFTP files to the PVC
        sftp_downloader.run(
            host=args.sftp_host,
            port=args.sftp_port,
            username=args.sftp_username,
            password=args.sftp_password,
            remote_path=args.sftp_remote_path,
            local_path=args.sftp_local_path,
        )

        # Step 2: Process the downloaded SFTP files and write to the output folder
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

        # Step 4: Expire PINs of cancelled titles
        pin_expirer.run(
            input_directory=args.sftp_local_path,
            output_directory=args.processed_data_path,
            expire_api_url=args.expire_api_url,
            database_name=args.db_name,
            host=args.db_host,
            port=args.db_port,
            user=args.db_username,
            password=args.db_password,
        )

        logging.info("ETL job completed successfully")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

    finally:
        personalisation = {
            "log_filename": os.path.basename(log_filename),
            "job_status": "Success" if "e" not in locals() else "Failure",
            "error_message": str(e) if "e" in locals() else None,
        }

        # Send an email with the log file attachment regardless of success or error
        # send_email_notification(
        #     args.api_key,
        #     args.base_url,
        #     args.email_address,
        #     args.template_id,
        #     log_filename,
        #     personalisation["job_status"],
        #     personalisation.get("error_message"),
        # )


if __name__ == "__main__":
    main()
