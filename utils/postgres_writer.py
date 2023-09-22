import pandas as pd
from sqlalchemy import create_engine
import time
import psycopg2, os


def write_dataframe_to_postgres(dataframe, table_name, engine, batch_size=1000):
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

        # Split the dataframe into batches
        batch_list = [
            dataframe[i : i + batch_size] for i in range(0, len(dataframe), batch_size)
        ]

        for i, batch in enumerate(batch_list):
            rows_inserted = len(batch)
            batch.to_sql(table_name, engine, if_exists="append", index=False)
            total_rows_inserted += rows_inserted  # Update the total count

        return total_rows_inserted  # Return the total count of rows inserted

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return 0


def run(
    input_directory,
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
                df, table_name, engine, batch_size=batch_size
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
