import pandas as pd


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


# def expire_pins():
# Call expire pin api for each title in expired_titles.csv
