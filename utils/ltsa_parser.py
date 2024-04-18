import json
import pandas as pd
import numpy as np
import requests
import time
import os


def pid_parser(pids):
    """
    Parses list of pids into string form and adds leading zeros such that each pid is 9 digits.

    Parameters:
    - pids (list): List of parcel ids.

    Returns:
    - pids (str): String of pids in cleaned form.
    """
    formatted_pids = []

    # Add leading zeros until each pid is 9 digits long
    for pid in pids:
        pid = pid.zfill(9)
        formatted_pids.append(pid)

    # Combine and format PIDs as a string
    return "|".join(sorted(set(map(str, formatted_pids))))


def load_data_cleaning_rules(data_rules_url):
    """
    Loads content from data_rules.json file hosted on github.

    Parameters:
    - data_rules_url (str): URL to data_rules.json file hosted on github.

    Returns:
    - data_cleaning (dict): Dictionary of rules read from data_rules.json.
    """
    # Load data cleaning rules from a JSON file hosted on GitHub
    response = requests.get(data_rules_url)
    if response.status_code == 200:
        data_cleaning = json.loads(response.text)
        return data_cleaning
    else:
        raise Exception(f"Failed to fetch data cleaning rules from {data_rules_url}")


def clean_active_pin_df(active_pin_df, output_directory, data_rules_url):
    """
    Applies cleaning rules from data_rules_url to active_pin_df.

    Parameters:
    - active_pin_df (pd.Dataframe): The dataframe to be cleaned.
    - output_directory (str): Directory to write active_pin.csv to.
    - data_rules_url (str): URL to data_rules.json file hosted on github.

    Returns:
    - None
    """
    try:
        # Load data cleaning rules from the specified GitHub URL
        data_cleaning_start_time = time.time()

        data_cleaning = load_data_cleaning_rules(data_rules_url)

        # Apply cleaning rules to each column
        for column, rule in data_cleaning["column_rules"].items():
            # Replace Exact Values - Looks for exact string match in column and replaces it with value
            if "replace_exact_values" in rule.keys():
                for replacement in rule["replace_exact_values"]:
                    active_pin_df[column] = active_pin_df[column].replace(
                        rule["replace_exact_values"][replacement], replacement
                    )

            # Trim after comma
            if "trim_after_comma" in rule.keys():
                active_pin_df[column] = active_pin_df[column].apply(
                    lambda x: x.split(",")[0] if isinstance(x, str) else x
                )

            # Remove Characters - Looks for strings containing character in column and removes character
            if "remove_characters" in rule.keys():
                for replacement in rule["remove_characters"]:
                    active_pin_df[column] = (
                        active_pin_df[column]
                        .str.replace(replacement, "")
                        .replace("  ", " ")
                    )

            # To uppercase
            if "to_uppercase" in rule.keys():
                active_pin_df[column] = active_pin_df[column].apply(
                    lambda x: x.upper() if isinstance(x, str) else x
                )

            # Switch value from one column, from_column, to another, to_column
            if "switch_column_value" in rule.keys():
                from_column = rule["switch_column_value"]["from_column"]
                to_column = rule["switch_column_value"]["to_column"]

                if "datatype" in rule["switch_column_value"]:
                    datatype = rule["switch_column_value"]["datatype"]
                    for value in active_pin_df[from_column]:
                        if datatype == "int" and value and value.isdigit():
                            active_pin_df[to_column] = np.where(
                                (active_pin_df[from_column] == value),
                                active_pin_df[from_column],
                                active_pin_df[to_column],
                            )

                if "region_map" in rule["switch_column_value"]:
                    region_map = rule["switch_column_value"]["region_map"]
                    for replacement in region_map:
                        active_pin_df[column] = active_pin_df[column].replace(
                            region_map[replacement], replacement
                        )

                    for value in region_map.keys():
                        active_pin_df[to_column] = np.where(
                            (active_pin_df[from_column] == value),
                            active_pin_df[from_column],
                            active_pin_df[to_column],
                        )

        print(f"Cleaning rules applied to file: active_pin.csv")

        active_pin_df = active_pin_df.drop(columns=["occupation", "parcel_status"])

        active_pin_df.to_csv(output_directory + "active_pin.csv", index=False)

        data_cleaning_elapsed_time = time.time() - data_cleaning_start_time
        print(
            f"Wrote cleaned ltsa data to file: {output_directory+'active_pin.csv'}. Elapsed Time: {data_cleaning_elapsed_time:.2f} seconds"
        )

    except Exception as e:
        raise e(f"Failed to clean active_pin dataframe")


def parse_ltsa_files(input_directory, output_directory, data_rules_url, engine):
    """
    Reads raw LTSA files to CSVs and writes them to output_directory. Writes processed and cleaned data to active_pin.csv.

    Parameters:
    - input_directory (str): Directory to read LTSA CSV files from.
    - output_directory (str): Directory to write CSV files to.
    - data_rules_url (str): URL to data_rules.json file hosted on github.
    - engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.

    Returns:
    - None
    """
    try:
        # Read, process, and write CSV files
        read_files_start_time = time.time()

        # Read valid_pid table from database and create dataframe
        valid_pid_df = pd.read_sql_table("valid_pid", engine, columns=["pid"])

        # Remove leading zeros from pid to match LTSA data
        valid_pid_df["pid"] = valid_pid_df["pid"].astype(str)
        print("Read table: valid_pid")

        # Creating an index of valid PIDs
        valid_pid_df_index = valid_pid_df.set_index(["pid"]).index

        # 2_parcel.csv
        parcel_df = (
            pd.read_csv(
                input_directory + "2_parcel.csv",
                usecols=["PRMNNT_PRCL_ID", "PRCL_STTS_CD"],
                dtype={"PRMNNT_PRCL_ID": str, "PRCL_STTS_CD": str},
            )
            .applymap(lambda x: x.strip() if isinstance(x, str) else x)
            .replace("", None)
            .replace(np.nan, None)
            .dropna(subset=["PRMNNT_PRCL_ID", "PRCL_STTS_CD"])
        )

        print("Read file: 2_parcel.csv")

        parcel_df = parcel_df.rename(
            columns={"PRMNNT_PRCL_ID": "pid", "PRCL_STTS_CD": "parcel_status"}
        )

        # Filter parcel dataframe by valid_pid dataframe:
        # Creating an index of PIDs inside parcel_df
        parcel_df_index = parcel_df.set_index(["pid"]).index

        # Updating parcel_df to only include rows with PIDs included in valid_pid_df
        parcel_df = parcel_df[parcel_df_index.isin(valid_pid_df_index)]

        print(f"Filtered data from 2_parcel.csv")

        parcel_df.to_csv(output_directory + "parcel_raw.csv", index=False)
        print(f"Wrote raw LTSA data to file: {output_directory+'parcel_raw.csv'}")

        # 3_titleparcel.csv
        title_parcel_df = (
            pd.read_csv(
                input_directory + "3_titleparcel.csv",
                usecols=["TITLE_NMBR", "LTB_DISTRICT_CD", "PRMNNT_PRCL_ID"],
                dtype={
                    "TITLE_NMBR": str,
                    "LTB_DISTRICT_CD": str,
                    "PRMNNT_PRCL_ID": str,
                },
            )
            .applymap(lambda x: x.strip() if isinstance(x, str) else x)
            .replace("", None)
            .replace(np.nan, None)
            .dropna(subset=["TITLE_NMBR", "LTB_DISTRICT_CD", "PRMNNT_PRCL_ID"])
        )
        print("Read file: 3_titleparcel.csv")

        title_parcel_df = title_parcel_df.rename(
            columns={
                "TITLE_NMBR": "title_number",
                "LTB_DISTRICT_CD": "land_title_district",
                "PRMNNT_PRCL_ID": "pid",
            }
        )

        # Filter titleparcel dataframe by valid_pid dataframe:
        # Creating list of columns in title_parcel_df
        title_parcel_df_keys = list(title_parcel_df.columns.values)

        # Creating index of PIDs inside of title_parcel_df
        title_parcel_df_index = title_parcel_df.set_index(["pid"]).index

        # Updating title_parcel_df to only include rows with PIDs included in valid_pid_df
        title_parcel_df = title_parcel_df[
            title_parcel_df_index.isin(valid_pid_df_index)
        ]

        print(f"Filtered data from 3_titleparcel.csv")

        title_parcel_df.to_csv(output_directory + "titleparcel_raw.csv", index=False)
        print(f"Wrote raw LTSA data to file: {output_directory+'titleparcel_raw.csv'}")

        #  1_title.csv
        title_df = (
            pd.read_csv(
                input_directory + " 1_title.csv",
                usecols=[
                    "TITLE_NMBR",
                    "LTB_DISTRICT_CD",
                    "TTL_STTS_CD",
                    "FRM_TTL_NMBR",
                    "FRM_LT_DISTRICT_CD",
                ],
                dtype={
                    "TITLE_NMBR": str,
                    "LTB_DISTRICT_CD": str,
                    "TTL_STTS_CD": str,
                    "FRM_TTL_NMBR": str,
                    "FRM_LT_DISTRICT_CD": str,
                },
            )
            .applymap(lambda x: x.strip() if isinstance(x, str) else x)
            .replace("", None)
            .replace(np.nan, None)
            .dropna(subset=["TITLE_NMBR", "LTB_DISTRICT_CD", "TTL_STTS_CD"])
        )
        print("Read file:  1_title.csv")

        title_df.rename(
            columns={
                "TITLE_NMBR": "title_number",
                "LTB_DISTRICT_CD": "land_title_district",
                "TTL_STTS_CD": "title_status",
                "FRM_TTL_NMBR": "from_title_number",
                "FRM_LT_DISTRICT_CD": "from_land_title_district",
            },
            inplace=True,
        )

        # Filter title dataframe by title_parcel dataframe:

        title_parcel_df_keys.remove("pid")

        # Creating multiIndex of title numbers and land title districts inside of title_df
        title_df_index = title_df.set_index(title_parcel_df_keys).index

        # Creating title_parcel_df without "pid" column
        title_parcel_without_pid_df = title_parcel_df.drop(["pid"], axis=1)

        # Creating multiIndex of valid title numbers and valid land title districts
        title_parcel_df_without_pid_index = title_parcel_without_pid_df.set_index(
            title_parcel_df_keys
        ).index

        # Updating title_df to only include rows with valid title numbers and valid land title districts included in title_parcel_df
        title_df = title_df[title_df_index.isin(title_parcel_df_without_pid_index)]

        print(f"Filtered data from  1_title.csv")

        title_df.to_csv(output_directory + "title_raw.csv", index=False)
        print(f"Wrote raw ltsa data to file: {output_directory+'title_raw.csv'}")

        # 4_titleowner.csv
        title_owner_df = (
            pd.read_csv(
                input_directory + "4_titleowner.csv",
                usecols=[
                    "TITLE_NMBR",
                    "LTB_DISTRICT_CD",
                    "CLIENT_GVN_NM",
                    "CLIENT_LST_NM_1",
                    "CLIENT_LST_NM_2",
                    "OCCPTN_DESC",
                    "INCRPRTN_NMBR",
                    "ADDRS_DESC_1",
                    "ADDRS_DESC_2",
                    "ADDRS_CITY",
                    "ADDRS_PROV_CD",
                    "ADDRS_PROV_ST",
                    "ADDRS_CNTRY",
                    "ADDRS_PSTL_CD",
                ],
                dtype={
                    "TITLE_NMBR": str,
                    "LTB_DISTRICT_CD": str,
                    "CLIENT_GVN_NM": str,
                    "CLIENT_LST_NM_1": str,
                    "CLIENT_LST_NM_2": str,
                    "OCCPTN_DESC": str,
                    "INCRPRTN_NMBR": str,
                    "ADDRS_DESC_1": str,
                    "ADDRS_DESC_2": str,
                    "ADDRS_CITY": str,
                    "ADDRS_PROV_CD": str,
                    "ADDRS_PROV_ST": str,
                    "ADDRS_CNTRY": str,
                    "ADDRS_PSTL_CD": str,
                },
            )
            .applymap(lambda x: x.strip() if isinstance(x, str) else x)
            .replace("", None)
            .replace(np.nan, None)
            .dropna(subset=["TITLE_NMBR", "LTB_DISTRICT_CD"])
        )
        print("Read file: 4_titleowner.csv")

        print(f"Filtered data from 4_titleowner.csv")

        title_owner_df = title_owner_df.rename(
            columns={
                "TITLE_NMBR": "title_number",
                "LTB_DISTRICT_CD": "land_title_district",
                "CLIENT_GVN_NM": "given_name",
                "CLIENT_LST_NM_1": "last_name_1",
                "CLIENT_LST_NM_2": "last_name_2",
                "OCCPTN_DESC": "occupation",
                "INCRPRTN_NMBR": "incorporation_number",
                "ADDRS_DESC_1": "address_line_1",
                "ADDRS_DESC_2": "address_line_2",
                "ADDRS_CITY": "city",
                "ADDRS_PROV_CD": "province_abbreviation",
                "ADDRS_PROV_ST": "province_long",
                "ADDRS_CNTRY": "country",
                "ADDRS_PSTL_CD": "postal_code",
            }
        )

        # Filter title_owner dataframe by title_parcel dataframe:
        # Creating multiIndex of title numbers and land title districts inside of title_owner_df
        title_owner_df_index = title_owner_df.set_index(title_parcel_df_keys).index

        # Updating title_owner_df to only include rows with valid title numbers and valid land title districts included in title_parcel_df
        title_owner_df = title_owner_df[
            title_owner_df_index.isin(title_parcel_df_without_pid_index)
        ]

        title_owner_df.to_csv(output_directory + "titleowner_raw.csv", index=False)
        read_files_elapsed_time = time.time() - read_files_start_time
        print(
            f"Wrote raw LTSA data to file: {output_directory+'titleowner_raw.csv'}. Elapsed Time: {read_files_elapsed_time:.2f} seconds"
        )

        # Join dataframes
        parse_files_start_time = time.time()

        title_titleowner_df = pd.merge(
            title_owner_df, title_df, on=["title_number", "land_title_district"]
        )
        print("Dataframes merged: title_owner_df, title_df")
        print(f"Number of rows in title_titleowner_df: {len(title_titleowner_df)}")

        titleparcel_parcel_df = pd.merge(title_parcel_df, parcel_df, on="pid")
        print("Dataframes merged: title_parcel_df, parcel_df")
        print(f"Number of rows in titleparcel_parcel_df: {len(titleparcel_parcel_df)}")

        active_pin_df = pd.merge(
            title_titleowner_df,
            titleparcel_parcel_df,
            on=["title_number", "land_title_district"],
        )
        print("Dataframes merged: title_titleowner_df, titleparcel_parcel_df")
        print(f"Number of rows in active_pin_df: {len(active_pin_df)}")

        # Group by title number to get a list of active pids associated with each title
        titlenumber_pids_df = (
            active_pin_df.groupby(["title_number", "land_title_district"])["pid"]
            .apply(list)
            .reset_index(name="pids")
        )
        print("Grouped dataframe created: titlenumber_pids_df")

        # Format PIDs as strings
        titlenumber_pids_df["pids"] = titlenumber_pids_df["pids"].apply(pid_parser)

        # Merge dataframes to add in PIDs column and drop duplicate rows
        active_pin_df = pd.merge(
            active_pin_df,
            titlenumber_pids_df,
            on=["title_number", "land_title_district"],
        ).drop(columns=["pid"])
        print("Dataframes merged: active_pin_df, titlenumber_pids_df")

        # Remove duplicate rows
        active_pin_df = active_pin_df.loc[
            active_pin_df.astype(str).drop_duplicates().index
        ]
        print("Duplicate rows dropped: active_pin_df")

        parse_files_elapsed_time = time.time() - parse_files_start_time
        print(
            f"Data parsing complete. Elapsed Time: {parse_files_elapsed_time:.2f} seconds"
        )

        clean_active_pin_df(active_pin_df, output_directory, data_rules_url)

    except Exception as e:
        raise e


def run(input_directory, output_directory, data_rules_url, engine):
    """
    Reads raw LTSA files to CSVs and writes them to output_directory. Writes processed and cleaned data to active_pin.csv.

    Parameters:
    - input_directory (str): Directory to read LTSA CSV files from.
    - output_directory (str): Directory to write CSV files to.
    - data_rules_url (str): URL to data_rules.json file hosted on github.
    - engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.

    Returns:
    - None
    """
    try:
        start_time = time.time()

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Parse the files
        parse_ltsa_files(input_directory, output_directory, data_rules_url, engine)

        end_time = time.time()
        total_time = end_time - start_time

        print(
            f"All files parsed and cleaned. Total time elapsed: {total_time:.2f} seconds"
        )

    except Exception as e:
        print(f"Error parsing LTSA data: {str(e)}")
        raise e


if __name__ == "__main__":  # pragma: no cover
    # Set your input directory path (where CSV files are located)
    input_directory = "input_directory_path/"

    # Set your output directory path (where cleaned and processed data will be saved)
    output_directory = "output_directory_path/"

    # Specify the URL of the data_rules.json file in your GitHub repository
    data_rules_url = (
        "https://raw.githubusercontent.com/your-username/your-repo/main/data_rules.json"
    )

    # Run the ETL process
    run(input_directory, output_directory, data_rules_url)
