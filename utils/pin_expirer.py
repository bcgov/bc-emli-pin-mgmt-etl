import pandas as pd
from sqlalchemy import create_engine
import requests
import time
from datetime import datetime


def create_expiration_df(input_directory):
    """
    Finds cancelled titles from LTSA 1_title.csv file, and writes titles to dataframe.

    Parameters:
    - input_directory (str): Directory to read LTSA 1_title.csv file from.

    Returns:
    - cancelled_titles_df (pd.Dataframe): Dataframe containing cancelled titles with columns title_number and title_status.
    """
    try:
        # Read 1_title.csv
        title_df = pd.read_csv(
            input_directory + "1_title.csv",
            usecols=[
                "TITLE_NMBR",
                "TTL_STTS_CD",
            ],
            dtype={
                "TITLE_NMBR": str,
                "TTL_STTS_CD": str,
            },
        ).applymap(lambda x: x.strip() if isinstance(x, str) else x)
        print("Read file: 1_title.csv")

        title_df.rename(
            columns={
                "TITLE_NMBR": "title_number",
                "TTL_STTS_CD": "title_status",
            },
            inplace=True,
        )

        # Drop active titles
        cancelled_titles_df = title_df.drop(
            title_df[title_df["title_status"] != "C"].index
        )

        return cancelled_titles_df

    except Exception as e:
        raise e


def expire_pins(expired_titles_df, engine, expire_api_url, vhers_api_key):
    """
    Reads expired_titles_df. For each cancelled title, the database is queried for the corresponding live_pin_id(s). Then the Expire PIN API is called.

    Parameters:
    - expired_titles_df (pd.Dataframe): Dataframe containing cancelled titles with columns title_number and title_status.
    - engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.
    - expire_api_url (str): Path for Expire PIN API endpoint.
    - vhers_api_key (str): API Key for Expire PIN API endpoint.

    Returns:
    - None
    """
    try:
        expired_title_list = list(expired_titles_df["title_number"])

        # Check if there are any cancelled titles
        if len(expired_title_list) > 0:
            title_number_string = (
                str(expired_title_list).replace("[", "(").replace("]", ")")
            )
            # Find live_pin_id for each cancelled title
            query = f"SELECT live_pin_id, title_number FROM active_pin WHERE title_number IN {title_number_string}"
            expired_rows_df = pd.read_sql(query, engine)

            # Call expire pin api for each title in expired_titles.csv
            for live_pin_id in expired_rows_df["live_pin_id"]:
                try:
                    live_pin_id = str(live_pin_id)
                    data = {
                        "livePinId": live_pin_id,
                        "expirationReason": "CO",
                    }
                    url = expire_api_url
                    headers = {"x-api-key": vhers_api_key}

                    response = requests.post(url=url, json=data, headers=headers)
                    response.raise_for_status()

                except requests.exceptions.HTTPError as e:
                    # console.log()
                    print(f"An error occurred calling Expire PIN API: {str(e)}")
                    raise e

            total_pins_expired = len(expired_rows_df["live_pin_id"])

            print(
                f"Expired PINs of cancelled titles, {total_pins_expired} pins expired, timestamp: {datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

        else:
            print(
                f"No PINs to expire, timestamp: {datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

    except Exception as e:
        raise e


def run(
    input_directory,
    expire_api_url,
    vhers_api_key,
    database_name,
    host="localhost",
    port=5432,
    user="your_username",
    password="your_password",
):
    """
    Finds cancelled titles from LTSA 1_title.csv file, and writes titles to dataframe. Reads expired_titles_df.
    For each cancelled title, the database is queried for the corresponding live_pin_id(s). Then the Expire PIN API is called.

    Parameters:
    - input_directory (str): Directory to read LTSA 1_title.csv file from.
    - expire_api_url (str): Path for Expire PIN API endpoint.
    - vhers_api_key (str): API Key for Expire PIN API endpoint.
    - database_name (str): The name of the PostgreSQL database.
    - host (str, optional): The database host. Default is "localhost".
    - port (int, optional): The database port. Default is 5432.
    - user (str, optional): The database username. Default is "your_username".
    - password (str, optional): The database password. Default is "your_password".

    Returns:
    - None
    """
    try:
        df = create_expiration_df(input_directory)

        # Create a connection to the PostgreSQL database
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        engine = create_engine(conn_str)

        start_time = time.time()  # Start measuring time

        # Call expire pin API for each title in dataframe
        expire_pins(df, engine, expire_api_url, vhers_api_key)

        elapsed_time = time.time() - start_time

        print(f"Elapsed Time: {elapsed_time:.2f} seconds")

    except Exception as e:
        print(f"An error occurred expiring PINs: {str(e)}")
        raise e


if __name__ == "__main__":  # pragma: no cover
    # Replace these placeholders with your actual values
    input_directory = "your_input_directory"  # Replace with your input directory path
    expire_api_url = "your_expire_api_url"  # Replace with your local expire api url
    vhers_api_key = "vhers_api_key"
    database_name = "postgres"  # Replace with your database name
    host = "localhost"  # Replace with your host
    port = 53173  # Replace with your port
    user = "your_username"  # Replace with your username
    password = "your_password"  # Replace with your password

    # Call the run function with the specified parameters
    run(
        input_directory,
        expire_api_url,
        vhers_api_key,
        database_name,
        host=host,
        port=port,
        user=user,
        password=password,
    )
