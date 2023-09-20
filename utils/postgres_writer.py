import os
import pandas as pd
from sqlalchemy import create_engine


def write_dataframe_to_postgres(dataframe, table_name, engine, batch_size=1000):
    """
    Write a DataFrame to a PostgreSQL table in batches.

    Parameters:
    - dataframe (pd.DataFrame): The DataFrame to be written.
    - table_name (str): The name of the PostgreSQL table.
    - engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.
    - batch_size (int, optional): Number of rows to write in each batch. Default is 1000.

    Returns:
    - None
    """
    try:
        # Split the dataframe into batches
        batch_list = [
            dataframe[i : i + batch_size] for i in range(0, len(dataframe), batch_size)
        ]

        for i, batch in enumerate(batch_list):
            batch.to_sql(table_name, engine, if_exists="append", index=False)
            print(
                f"Batch {i + 1} of {len(batch_list)} written to the '{table_name}' table."
            )

    except Exception as e:
        print(f"An error occurred: {str(e)}")


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
            if os.path.isfile(os.path.join(input_directory, f))
        ]

        for file_name in file_list:
            file_path = os.path.join(input_directory, file_name)
            # Adjust for different file formats (e.g., pd.read_csv for CSV files)
            df = pd.read_csv(file_path)
            # Use file name without extension as table name
            table_name = os.path.splitext(file_name)[0]
            write_dataframe_to_postgres(df, table_name, engine, batch_size=batch_size)

        print("All files processed.")

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
