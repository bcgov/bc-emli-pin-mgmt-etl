import argparse
import logging
import os
from datetime import datetime
from utils import (
    ltsa_parser,
    sftp_downloader,
    postgres_writer,
    pin_expirer,
)
from utils.gc_notify import gc_notify_log
from utils.logging_config import setup_logging
import time


def send_email_notification(
    api_key,
    base_url,
    email_address,
    template_id,
    log_folder,
    log_filename,
    start_time,
    status,
    message,
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
            # "log_filename": log_filename,
            "start_time": start_time,
            "status": status,
            "message": message,
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
        print(f"Error sending email notification: {str(e)}")


def main():
    start_time = datetime.now().strftime("%a %d %b %Y, %I:%M%p")

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
        default="/data/",
    )
    parser.add_argument(
        "--processed_data_path",
        type=str,
        help="Local output folder for the processed csv files.",
        default="/data/output/",
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
        default="/data/log/",
        help="Folder where the log file should be created.",
    )

    args = parser.parse_args()
    log_filename = f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    try:
        # Set up logging with the specified log folder and filename
        setup_logging(args.log_folder, log_filename)
        logger = logging.getLogger(__name__)

        # Step 1: Download the SFTP files to the PVC

        downloader_start_time = time.time()
        print('------\nSTEP 1: DOWNLOADING LTSA FILES\n------')

        sftp_downloader.run(
            host=args.sftp_host,
            port=args.sftp_port,
            username=args.sftp_username,
            password=args.sftp_password,
            remote_path=args.sftp_remote_path,
            local_path=args.sftp_local_path,
        )
        
        downloader_elapsed_time = time.time() - downloader_start_time
        print(f'------\nSTEP 1 COMPLETED: DOWNLOADED LTSA FILES. Elapsed Time: {downloader_elapsed_time:.2f} seconds')

        # Step 2: Process the downloaded SFTP files and write to the output folder

        parser_start_time = time.time()
        print('------\nSTEP 2: PARSING LTSA FILES\n------')

        ltsa_parser.run(
            input_directory=args.sftp_local_path,
            output_directory=args.processed_data_path,
            data_rules_url=args.data_rules_url,
        )

        parser_elapsed_time = time.time() - parser_start_time
        print(f'------\nSTEP 2 COMPLETED: PARSED LTSA FILES. Elapsed Time: {parser_elapsed_time:.2f} seconds')

        # Step 3: Write the above processed data to the PostgreSQL database

        writer_start_time = time.time()
        print('------\nSTEP 3: WRITING PARSED FILES TO DATABASE\n------')

        postgres_writer.run(
            input_directory=args.processed_data_path,
            database_name=args.db_name,
            batch_size=args.db_write_batch_size,
            host=args.db_host,
            port=args.db_port,
            user=args.db_username,
            password=args.db_password,
        )

        writer_elapsed_time = time.time() - writer_start_time
        print(f'------\nSTEP 3 COMPLETED: WROTE PARSED FILES TO DATABASE. Elapsed Time: {writer_elapsed_time:.2f} seconds')

        # Step 4: Expire PINs of cancelled titles

        expier_start_time = time.time()
        print('------\nSTEP 4: EXPIRING PINS\n------')

        pin_expirer.run(
            input_directory=args.sftp_local_path,
            expire_api_url=args.expire_api_url,
            database_name=args.db_name,
            host=args.db_host,
            port=args.db_port,
            user=args.db_username,
            password=args.db_password,
        )

        expier_elapsed_time = time.time() - expier_start_time
        print(f'------\nSTEP 4 COMPLETED: EXPIRED PINS. Elapsed Time: {expier_elapsed_time:.2f} seconds')

        logger.info("------\nETL JOB COMPLETED SUCCESSFULLY\n------")

    except Exception as e:
        logger.info(f"An error occurred: {str(e)}")

    finally:
        personalisation = {
            "start_time": start_time,
            "status": "Success" if "e" not in locals() else "Failure",
            "message": str(e) if "e" in locals() else "ETL job completed successfully",
        }

        logger.info(personalisation)

        # Send an email with the log file attachment regardless of success or error
        send_email_notification(
            args.api_key,
            args.base_url,
            args.email_address,
            args.template_id,
            args.log_folder,
            log_filename,
            personalisation["start_time"],
            personalisation["status"],
            personalisation["message"],
        )


if __name__ == "__main__":
    main()
