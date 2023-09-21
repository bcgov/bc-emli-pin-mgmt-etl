import pandas as pd
import os
from sqlalchemy import create_engine
import requests


def create_expiration_file(input_directory, output_directory):
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

    # Write inactive titles to csv
    expired_titles_df.to_csv(output_directory + "expired_titles.csv", index=False)
    print(
        f"WROTE CANCELLED TITLES TO FILE:----------------{output_directory+'expired_titles.csv'}"
    )


# create separate folder within output script
# add time function


def expire_pins(expired_titles_df, engine, expire_api_url):
    # Find live_pin_id of title
    title_number_string = (
        str(list(expired_titles_df["title_number"])).replace("[", "(").replace("]", ")")
    )
    query = f"SELECT live_pin_id, title_number FROM active_pin WHERE title_number IN {title_number_string}"
    expired_rows_df = pd.read_sql(query, engine)

    # Call expire pin api for each title in expired_titles.csv
    for live_pin_id in expired_rows_df["live_pin_id"]:
        print(live_pin_id)
        live_pin_id = str(live_pin_id)
        data = {
            "livePinId": live_pin_id,
            "expirationReason": "CO",
        }
        response = requests.post(url=expire_api_url, json=data)
        print(response.text)

    print(f"EXPIRED PINS OF CANCELLED TITLES")


def run(
    input_directory,
    output_directory,
    expire_api_url,
    database_name="postgres",
    host="localhost",
    port=53173,
    user="postgres",
    password="bcpassword",
):
    try:
        create_expiration_file(input_directory, output_directory)
        expiration_file_directory = output_directory

        # Create a connection to the PostgreSQL database
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        engine = create_engine(conn_str)

        file_name = "expired_titles.csv"
        file_path = os.path.join(expiration_file_directory, file_name)
        df = pd.read_csv(file_path)

        # Call expire pin API for each title in dataframe
        expire_pins(df, engine, expire_api_url)

    except Exception as e:
        print(f"An error occurred: {str(e)}")


# Add if name = main

run(
    "/Users/emendelson/Downloads/export/EMLI_UPDATE_20230824/EMLI_UPDATE_20230824/",
    "/Users/emendelson/Downloads/export/EMLI_UPDATE_20230824/EMLI_UPDATE_20230824/",
    "http://localhost:3000/pins/expire",
)
