import argparse
import logging
import os
import sys
from datetime import datetime
from utils import ltsa_parser, sftp_downloader, postgres_writer
from utils.gc_notify import gc_notify_log


def setup_logging(log_filename):
    """
    Set up logging to redirect stdout and stderr to log files.

    Args:
        log_filename (str): The name of the log file.

    Returns:
        None
    """
    logging.basicConfig(filename=log_filename, level=logging.INFO)
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
        log_filename (str): The path to the log file to attach.
        status (str): The status of the job (e.g., "Success" or "Failure").
        error_message (str): The error message (if applicable).

    Returns:
        None
    """
    try:
        personalisation = {
            "log_filename": os.path.basename(log_filename),
            "job_status": status,
            "error_message": error_message,
        }

        gc_notify_log(
            api_key,
            base_url,
            email_address,
            template_id,
            log_filename,
            personalisation,
        )

    except Exception as e:
        logging.error(f"Error sending email notification: {str(e)}")


def main():
    log_filename = f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    setup_logging(log_filename)

    parser = argparse.ArgumentParser(
        prog="BC PVS ETL Job",
        description="This is the ETL job that expires PINS.",
        epilog="Please check the repo: https://github.com/bcgov/bc-emli-pin-mgmt-etl",
    )

    args = parser.parse_args()

    try:
        sftp_downloader.run(
            host=args.sftp_host,
            port=args.sftp_port,
            username=args.sftp_username,
            password=args.sftp_password,
            remote_path=args.sftp_remote_path,
            local_path=args.sftp_local_path,
        )

        ltsa_parser.run(
            input_directory=args.sftp_local_path,
            output_directory=args.processed_data_path,
            data_rules_url=args.data_rules_url,
        )

        postgres_writer.run(
            input_directory=args.processed_data_path,
            database_name=args.db_name,
            batch_size=args.db_write_batch_size,
            host=args.db_host,
            port=args.db_port,
            user=args.db_username,
            password=args.db_password,
        )

        logging.info("ETL job completed successfully")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

    finally:
        api_key = "YOUR_API_KEY"
        base_url = "https://api.notification.canada.ca"
        email_address = "recipient@example.com"
        template_id = "YOUR_TEMPLATE_ID"

        personalisation = {
            "log_filename": os.path.basename(log_filename),
            "job_status": "Success" if "e" not in locals() else "Failure",
            "error_message": str(e) if "e" in locals() else None,
        }

        send_email_notification(
            api_key,
            base_url,
            email_address,
            template_id,
            log_filename,
            personalisation["job_status"],
            personalisation.get("error_message"),
        )


if __name__ == "__main__":
    main()
