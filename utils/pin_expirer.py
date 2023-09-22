import pandas as pd
from sqlalchemy import create_engine
import requests
import time
from datetime import datetime


def create_expiration_file(input_directory):
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
    ).map(lambda x: x.strip() if isinstance(x, str) else x)
    print("READ FILE----------------1_title.csv")

    title_df.rename(
        columns={
            "TITLE_NMBR": "title_number",
            "TTL_STTS_CD": "title_status",
        },
        inplace=True,
    )

    # Drop active titles
    expired_titles_df = title_df.drop(title_df[title_df["title_status"] != "C"].index)

    return expired_titles_df


def expire_pins(expired_titles_df, engine, expire_api_url):
    # Find live_pin_id of title
    title_number_string = (
        str(list(expired_titles_df["title_number"])).replace("[", "(").replace("]", ")")
    )
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
            requests.post(url=expire_api_url, json=data)

        except Exception as e:
            print(f"An error occurred calling Expire PIN API: {str(e)}")

    total_pins_expired = len(expired_rows_df["live_pin_id"])

    print(
        f"Expired PINs of cancelled titles, {total_pins_expired} pins expired, timestamp: {datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )


def run(
    input_directory,
    expire_api_url,
    database_name,
    host,
    port,
    user,
    password,
):
    try:
        df = create_expiration_file(input_directory)

        # Create a connection to the PostgreSQL database
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        engine = create_engine(conn_str)

        start_time = time.time()  # Start measuring time

        # Call expire pin API for each title in dataframe
        expire_pins(df, engine, expire_api_url)

        elapsed_time = time.time() - start_time

        print(f"Elapsed Time: {elapsed_time:.2f} seconds")

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    # Replace these placeholders with your actual values
    input_directory = "your_input_directory"  # Replace with your input directory path
    expire_api_url = "your_expire_api_url"  # Replace with your local expire api url
    database_name = "postgres"  # Replace with your database name
    host = "localhost"  # Replace with your host
    port = 53173  # Replace with your port
    user = "your_username"  # Replace with your username
    password = "your_password"  # Replace with your password

    # Call the run function with the specified parameters
    run(
        input_directory,
        expire_api_url,
        database_name,
        host=host,
        port=port,
        user=user,
        password=password,
    )
