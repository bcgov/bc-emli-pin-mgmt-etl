import numpy as np
import pandas as pd
from sqlalchemy import text, create_engine, func, select
import time
import psycopg2, os


def insert_postgres_table_if_rows_not_exist(
    dataframe, table_name, engine, unique_key_columns
):
    """
    Inserts non-duplicate rows into PostreSQL table in batches.

    Parameters:
    - dataframe (pd.DataFrame): The DataFrame to be written.
    - table_name (str): The name of the PostgreSQL table.
    - engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.
    - unique_key_columns (list): Columns that will prevent data insert on conflict.

    Returns:
    - None (or Error)
    """
    try:
        # Create a list of column names as a comma-separated string
        column_names = ", ".join(dataframe.columns)

        # Convert the DataFrame to a list of tuples for insertion
        data_to_insert = [tuple(row) for row in dataframe.values]
        data_to_insert = (
            str(data_to_insert)[1:-1]
            .replace("'", "''")
            .replace(", ''", ", '")
            .replace("'',", "',")
            .replace("(''", "('")
            .replace("'')", "')")
            .replace("', \"", "', '")
            .replace("\", '", "', '")
        )

        # Create a SQL INSERT statement with ON CONFLICT DO NOTHING clause
        insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES {data_to_insert} ON CONFLICT ({', '.join(unique_key_columns)}) DO NOTHING;"

        # Execute the SQL statement with parameter binding
        with engine.begin() as conn:
            conn.execute(text(insert_sql))

    except Exception as e:
        return f"Error: {str(e)}"


def get_row_count(table_name, engine):
    """
    Counts number of rows in database table.

    Parameters:
    - table_name (str): The name of the PostgreSQL table.
    - engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.

    Returns:
    - int:  The total number of rows in the table.
    """
    query = select([func.count()]).select_from(text(table_name))
    conn = engine.connect()
    totalCount = conn.execute(query).fetchone()[0]
    return totalCount


def write_dataframe_to_postgres(
    dataframe, table_name, engine, etl_job_id, batch_size=1000
):
    """
    Write a DataFrame to a PostgreSQL table in batches.

    Parameters:
    - dataframe (pd.DataFrame): The DataFrame to be written.
    - table_name (str): The name of the PostgreSQL table.
    - engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.
    - batch_size (int, optional): Number of rows to write in each batch. Default is 1000.

    Returns:
    - int: The total number of rows inserted into the table.
    """
    total_rows_inserted = 0  # Initialize the total count of rows inserted
    try:
        print(f"Updating table '{table_name}'...")  # Print the table being updated

        rows_before_insert = get_row_count(table_name, engine)
        dataframe = dataframe.replace(np.nan, "")
        unique_key_columns = dataframe.columns.tolist()

        if table_name != "active_pin":
            dataframe["etl_log_id"] = str(etl_job_id)

        # Split the dataframe into batches
        batches = [
            dataframe[i : i + batch_size] for i in range(0, len(dataframe), batch_size)
        ]

        # Define the columns that make up the unique key --all columns

        for batch in batches:
            update_response = insert_postgres_table_if_rows_not_exist(
                batch, table_name, engine, unique_key_columns
            )
            if update_response:
                print(update_response)

        print("Table updated")

        rows_after_insert = get_row_count(table_name, engine)
        total_rows_inserted = rows_after_insert - rows_before_insert

        return total_rows_inserted  # Return the total count of rows inserted

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return 0


def run(
    input_directory,
    etl_job_id,
    database_name,
    batch_size=1000,
    host="localhost",
    port=5432,
    user="your_username",
    password="your_password",
):
    """
    Process files in a directory and write them to a PostgreSQL database.

    Parameters:
    - input_directory (str): The path to the directory containing input files.
    - database_name (str): The name of the PostgreSQL database.
    - batch_size (int, optional): Number of rows to write in each batch. Default is 1000.
    - host (str, optional): The database host. Default is "localhost".
    - port (int, optional): The database port. Default is 5432.
    - user (str, optional): The database username. Default is "your_username".
    - password (str, optional): The database password. Default is "your_password".

    Returns:
    - None
    """
    try:
        # Create a connection to the PostgreSQL database
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        engine = create_engine(conn_str)

        # List all files in the input directory
        file_list = [
            f
            for f in os.listdir(input_directory)
            if (os.path.isfile(os.path.join(input_directory, f)) and f.endswith("csv"))
        ]

        table_statistics = []  # List to store table-wise statistics

        start_time = time.time()  # Start measuring time

        for file_name in file_list:
            file_path = os.path.join(input_directory, file_name)
            # Adjust for different file formats (e.g., pd.read_csv for CSV files)
            df = pd.read_csv(file_path, encoding="unicode_escape", low_memory=False)
            # Use file name without extension as table name
            table_name = os.path.splitext(file_name)[0]
            rows_inserted = write_dataframe_to_postgres(
                df, table_name, engine, etl_job_id, batch_size=batch_size
            )
            elapsed_time = time.time() - start_time
            table_statistics.append(
                {
                    "Table Name": table_name,
                    "Rows Inserted": rows_inserted,
                    "Elapsed Time (s)": elapsed_time,
                }
            )

        end_time = time.time()  # Stop measuring time

        total_rows_inserted = sum(stat["Rows Inserted"] for stat in table_statistics)

        print("Table-wise Statistics:")
        for stat in table_statistics:
            print(
                f"Table: {stat['Table Name']}, Rows Inserted: {stat['Rows Inserted']}, Elapsed Time: {stat['Elapsed Time (s)']:.2f} seconds"
            )

        print(
            f"All files processed. Total rows inserted: {total_rows_inserted}, Total Time elapsed: {end_time - start_time:.2f} seconds"
        )

    except Exception as e:
        print(f"An error occurred: {str(e)}")


# Entry point of the script
if __name__ == "__main__":
    # Replace these placeholders with your actual values
    input_directory = "your_input_directory"  # Replace with your input directory path
    database_name = "your_database_name"  # Replace with your database name
    batch_size = 1000  # Set your desired batch size
    host = "localhost"  # Replace with your host
    port = 5432  # Replace with your port
    user = "your_username"  # Replace with your username
    password = "your_password"  # Replace with your password

    # Call the run function with the specified parameters
    run(
        input_directory,
        database_name,
        batch_size=batch_size,
        host=host,
        port=port,
        user=user,
        password=password,
    )
