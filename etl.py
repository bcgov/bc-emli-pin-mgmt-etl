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
from sqlalchemy import text, create_engine


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


def get_status_from_etl_log_table(engine, folder):
    """
    Get status from etl_log table.

    Args:
        folder (str): Name of latest data folder from LTSA.
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.

    Returns:
        status (str): Status from etl_log table OR status (None): If folder does not exist in etl_log table.
    """
    try:
        select_sql = f"SELECT folder, status FROM etl_log WHERE folder = '{folder}'"
        with engine.begin() as conn:
            result = conn.execute(text(select_sql)).fetchall()
            if result:
                if "Success" in str(result):
                    status = "Success"
                else:
                    status = result[0][1]
            else:
                status = None
        return status

    except Exception as e:
        raise e


def insert_status_into_etl_log_table(engine, status):
    """
    Insert row into etl_log table with provided status.

    Args:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.
        status (str): Status to be inserted into etl_log table

    Returns:
        job_id (UUID): Job_id from etl_log table.
    """
    try:
        insert_sql = f"INSERT INTO etl_log (status) VALUES ('{status}')"
        select_sql = f"SELECT job_id FROM etl_log WHERE status = '{status}' ORDER BY updated_at DESC LIMIT 1"
        with engine.begin() as conn:
            conn.execute(text(insert_sql))
            job_id = conn.execute(text(select_sql)).fetchone()[0]
        return job_id

    except Exception as e:
        raise e


def update_status_in_etl_log_table(engine, job_id, status, folder=None):
    """
    Update row in etl_log table with provided status.

    Args:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.
        job_id (UUID): Job_id from etl_log table.
        status (str): Status to update row to in etl_log table.
        folder (str): Folder name to be inserted

    Returns:
        None
    """
    try:
        if folder:
            update_sql = f"UPDATE etl_log SET folder = '{folder}', status = '{status}' WHERE job_id = '{job_id}'"
        else:
            update_sql = (
                f"UPDATE etl_log SET status = '{status}' WHERE job_id = '{job_id}'"
            )

        with engine.begin() as conn:
            conn.execute(text(update_sql))

    except Exception as e:
        raise e


def delete_rows_with_job_id(engine, table_column, job_id):
    """
    Delete rows in provided tables where job_id is a foreign key in the provided row.

    Args:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.
        table_column (dict): Key is table name, value is column that contains foreign key with etl_log.job_id.
        job_id (UUID): Job_id from etl_log table.

    Returns:
        None
    """
    try:
        for table_name, column_name in table_column.items():
            delete_sql = f"""
                DELETE FROM {table_name} WHERE {column_name} = '{job_id}';
            """
            with engine.begin() as conn:
                conn.execute(text(delete_sql))
    except Exception as e:
        raise e


def main():
    """
    Sets parser arguments and runs modules for ETL job:
        - Download the SFTP files to the PVC
        - Process the downloaded SFTP files and write to the output folder
        - Write processed data to the PostgreSQL database
        - Expire PINs of cancelled titles
        - Send an email with the log file attachment regardless of success or error

    Returns:
        None
    """
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
    parser.add_argument(
        "--vhers_api_key", type=str, help="API Key for Expire PIN API endpoint"
    )

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

        # Create a connection to the PostgreSQL database
        conn_str = f"postgresql://{args.db_username}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}"
        engine = create_engine(conn_str)

        # Add entry to etl_log table
        etl_log_start_time = time.time()
        print("------\nSTEP 0: CREATING INITIAL ENTRY IN ETL_LOG TABLE")

        job_id = insert_status_into_etl_log_table(engine, "In Progress")

        etl_log_elapsed_time = time.time() - etl_log_start_time

        print(
            f"------\nSTEP 0 COMPLETE: ROW ADDED TO ETL_LOG TABLE. Elapsed Time: {etl_log_elapsed_time:.2f} seconds"
        )

        # Step 1: Download the SFTP files to the PVC

        downloader_start_time = time.time()
        print("------\nSTEP 1: DOWNLOADING LTSA FILES\n------")
        folder = sftp_downloader.run(
            host=args.sftp_host,
            port=args.sftp_port,
            username=args.sftp_username,
            password=args.sftp_password,
            remote_path=args.sftp_remote_path,
            local_path=args.sftp_local_path,
        )

        downloader_elapsed_time = time.time() - downloader_start_time
        print(
            f"------\nSTEP 1 COMPLETED: DOWNLOADED LTSA FILES. Elapsed Time: {downloader_elapsed_time:.2f} seconds"
        )

        # Check if folder has already been run
        # folder = "folder_name"    # Uncomment to test locally
        run_status = get_status_from_etl_log_table(engine, folder)

        if run_status in [None, "Failure", "In Progress", "Cancelled"]:
            # Update with latest folder name
            update_status_in_etl_log_table(engine, job_id, "In Progress", folder=folder)

            # # Step 2: Process the downloaded SFTP files and write to the output folder

            # parser_start_time = time.time()
            # print("------\nSTEP 2: PARSING LTSA FILES\n------")

            # ltsa_parser.run(
            #     input_directory=args.sftp_local_path,
            #     output_directory=args.processed_data_path,
            #     data_rules_url=args.data_rules_url,
            # )

            # parser_elapsed_time = time.time() - parser_start_time
            # print(
            #     f"------\nSTEP 2 COMPLETED: PARSED LTSA FILES. Elapsed Time: {parser_elapsed_time:.2f} seconds"
            # )

            # # Step 3: Write the above processed data to the PostgreSQL database

            # writer_start_time = time.time()
            # print("------\nSTEP 3: WRITING PARSED FILES TO DATABASE\n------")

            # postgres_writer.run(
            #     input_directory=args.processed_data_path,
            #     etl_job_id=job_id,
            #     database_name=args.db_name,
            #     batch_size=args.db_write_batch_size,
            #     host=args.db_host,
            #     port=args.db_port,
            #     user=args.db_username,
            #     password=args.db_password,
            # )

            # writer_elapsed_time = time.time() - writer_start_time
            # print(
            #     f"------\nSTEP 3 COMPLETED: WROTE PARSED FILES TO DATABASE. Elapsed Time: {writer_elapsed_time:.2f} seconds"
            # )

            # # Step 4: Expire PINs of cancelled titles

            # expier_start_time = time.time()
            # print("------\nSTEP 4: EXPIRING PINS\n------")

            # pin_expirer.run(
            #     input_directory=args.sftp_local_path,
            #     expire_api_url=args.expire_api_url,
            #     vhers_api_key=args.vhers_api_key,
            #     database_name=args.db_name,
            #     host=args.db_host,
            #     port=args.db_port,
            #     user=args.db_username,
            #     password=args.db_password,
            # )

            # expier_elapsed_time = time.time() - expier_start_time
            # print(
            #     f"------\nSTEP 4 COMPLETED: EXPIRED PINS. Elapsed Time: {expier_elapsed_time:.2f} seconds"
            # )

            # update_status_in_etl_log_table(engine, job_id, "Success")

            logger.info("------\nETL JOB COMPLETED SUCCESSFULLY\n------")

            personalisation = {
                "status": "Success",
                "message": "ETL job completed successfully",
            }

        else:
            # Update with Cancelled status and latest folder name
            update_status_in_etl_log_table(engine, job_id, "Cancelled", folder=folder)

            personalisation = {
                "status": "Cancelled",
                "message": "ETL job cancelled because of a previous successful run.",
            }

            print(f"------\nETL JOB NOT RUN: FILE ALREADY RAN\n------")

    except Exception as e:
        personalisation = {
            "status": "Failure",
            "message": str(e),
        }

        logger.info(f"An error occurred: {str(e)}")

        try:
            if "job_id" in locals():
                raw_tables_and_log_id_column = {
                    "title_raw": "etl_log_id",
                    "parcel_raw": "etl_log_id",
                    "titleparcel_raw": "etl_log_id",
                    "titleowner_raw": "etl_log_id",
                }
                update_status_in_etl_log_table(engine, job_id, "Failure")
                delete_rows_with_job_id(engine, raw_tables_and_log_id_column, job_id)

            else:
                insert_status_into_etl_log_table(engine, "Failure")

        except Exception as e:
            logger.info(f"An error occurred: {str(e)}")

    finally:
        # Send an email with the log file attachment regardless of success or error
        personalisation["start_time"] = start_time

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
