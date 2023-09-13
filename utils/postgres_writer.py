import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
import datetime
import sys


def setup_logging(log_file_prefix="postgres_writer"):
    # Create a timestamped log file
    log_file = (
        f"{log_file_prefix}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    )
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Define a console handler to display log messages in the console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # Set the desired console log level
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)

    # Attach the console handler to the root logger
    logging.getLogger().addHandler(console_handler)


def write_dataframe_to_postgres(dataframe, table_name, engine, batch_size=1000):
    try:
        # Split the dataframe into batches
        batch_list = [
            dataframe[i : i + batch_size] for i in range(0, len(dataframe), batch_size)
        ]

        for i, batch in enumerate(batch_list):
            batch.to_sql(table_name, engine, if_exists="append", index=False)
            logging.info(
                f"Batch {i + 1} of {len(batch_list)} written to the '{table_name}' table."
            )

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


def run(
    input_directory,
    database_name,
    batch_size=1000,
    host="localhost",
    port=5432,
    user="your_username",
    password="your_password",
):
    try:
        # Create a connection to the PostgreSQL database
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        engine = create_engine(conn_str)

        # List all files in the input directory
        file_list = [
            f
            for f in os.listdir(input_directory)
            if os.path.isfile(os.path.join(input_directory, f))
        ]

        for file_name in file_list:
            file_path = os.path.join(input_directory, file_name)
            df = pd.read_csv(file_path)  # Adjust for different file formats
            table_name = os.path.splitext(file_name)[
                0
            ]  # Use file name without extension as table name
            write_dataframe_to_postgres(df, table_name, engine, batch_size=batch_size)

        logging.info("All files processed.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


# Example usage:
if __name__ == "__main__":
    setup_logging()  # Initialize logging
    input_directory = "your_input_directory"  # Replace with your input directory path
    database_name = "your_database_name"  # Replace with your database name
    batch_size = 1000  # Set your desired batch size
    run(input_directory, database_name, batch_size=batch_size)
