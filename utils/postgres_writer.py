import argparse
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
import datetime


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
        for chunk in pd.read_csv(
            dataframe_file, chunksize=chunk_size
        ):  # Adjust for different file formats
            # Write the current chunk to the PostgreSQL database
            chunk.to_sql(table_name, engine, if_exists="append", index=False)

        logging.info(
            f"Data successfully written to the '{table_name}' table in the '{database_name}' database in batches of {batch_size} records each."
        )

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


def main():
    parser = argparse.ArgumentParser(
        description="Write data from a DataFrame to a PostgreSQL database table."
    )
    parser.add_argument("dataframe_file", help="Path to the DataFrame file")
    parser.add_argument("table_name", help="Name of the PostgreSQL table")
    parser.add_argument("database_name", help="Name of the PostgreSQL database")
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1000,
        help="Batch size for writing data (default: 1000)",
    )
    parser.add_argument(
        "--host", default="localhost", help="PostgreSQL host (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=5432, help="PostgreSQL port (default: 5432)"
    )
    parser.add_argument(
        "--user",
        default="your_username",
        help="PostgreSQL username (default: your_username)",
    )
    parser.add_argument(
        "--password",
        default="your_password",
        help="PostgreSQL password (default: your_password)",
    )

    args = parser.parse_args()
    setup_logging()
    write_dataframe_to_postgres(
        args.dataframe_file,
        args.table_name,
        args.database_name,
        args.batch_size,
        args.host,
        args.port,
        args.user,
        args.password,
    )


if __name__ == "__main__":
    main()
