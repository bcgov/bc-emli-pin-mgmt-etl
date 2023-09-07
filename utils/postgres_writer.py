import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
import datetime
from tqdm import tqdm
import sys


def setup_logging():
    # Create a timestamped log file
    log_file = (
        f"postgres_writer_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    )
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def write_dataframe_to_postgres(
    dataframe_file,
    table_name,
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

        # Read the DataFrame in chunks
        chunk_size = batch_size
        total_rows = pd.read_csv(dataframe_file).shape[
            0
        ]  # Total number of rows in the DataFrame
        progress_bar = tqdm(
            total=total_rows, unit=" rows", unit_scale=True, dynamic_ncols=True
        )

        for chunk in pd.read_csv(
            dataframe_file, chunksize=chunk_size
        ):  # Adjust for different file formats
            # Write the current chunk to the PostgreSQL database
            chunk.to_sql(table_name, engine, if_exists="append", index=False)
            progress_bar.update(len(chunk))
            sys.stdout.flush()  # Flush the output to update the progress bar in real-time

        progress_bar.close()
        logging.info(
            f"Data successfully written to the '{table_name}' table in the '{database_name}' database in batches of {batch_size} records each."
        )

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


# Example usage:
if __name__ == "__main__":
    setup_logging()  # Initialize logging
    dataframe_file = "your_dataframe.csv"  # Replace with your actual file path
    table_name = "your_table_name"  # Replace with your desired table name
    database_name = "your_database_name"  # Replace with your database name
    batch_size = 1000  # Set your desired batch size
    write_dataframe_to_postgres(dataframe_file, table_name, database_name, batch_size)
