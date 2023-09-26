import json
import pandas as pd
import numpy as np
import requests
import time
import os


def pid_parser(pids):
    # Combine and format PIDs as a string
    return "|".join(sorted(set(map(str, pids))))


def load_data_cleaning_rules(data_rules_url):
    # Load data cleaning rules from a JSON file hosted on GitHub
    response = requests.get(data_rules_url)
    if response.status_code == 200:
        data_cleaning = json.loads(response.text)
        return data_cleaning
    else:
        raise Exception(f"Failed to fetch data cleaning rules from {data_rules_url}")


def clean_active_pin_df(active_pin_df, output_directory, data_rules_url):
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

    print(f"CLEANING RULES APPLIED TO FILE:----------------active_pin.csv")

    active_pin_df = active_pin_df.drop(columns=["occupation", "parcel_status"])

    active_pin_df.to_csv(output_directory + "active_pin.csv", index=False)

    data_cleaning_elapsed_time = time.time() - data_cleaning_start_time
    print(
        f"WROTE CLEANED LTSA DATA TO FILE:----------------{output_directory+'active_pin.csv'}. Elapsed Time: {data_cleaning_elapsed_time:.2f} seconds"
    )


def parse_ltsa_files(input_directory, output_directory, data_rules_url):
    # Read, process, and write CSV files
    read_files_start_time = time.time()

    # If not null fields are null, drop them
    # Add number of dropped rows to logs

    # 1_title.csv
    title_df = (
        pd.read_csv(
            input_directory + "1_title.csv",
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
        .map(lambda x: x.strip() if isinstance(x, str) else x)
        .replace("", None)
        .replace(np.nan, None)
    )
    print("READ FILE----------------1_title.csv")

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

    title_df.to_csv(output_directory + "title_raw.csv", index=False)
    print(
        f"WROTE RAW LTSA DATA TO FILE:----------------{output_directory+'title_raw.csv'}"
    )

    # 2_parcel.csv
    parcel_df = (
        pd.read_csv(
            input_directory + "2_parcel.csv",
            usecols=["PRMNNT_PRCL_ID", "PRCL_STTS_CD"],
            dtype={"PRMNNT_PRCL_ID": int, "PRCL_STTS_CD": str},
        )
        .map(lambda x: x.strip() if isinstance(x, str) else x)
        .replace("", None)
        .replace(np.nan, None)
    )
    print("READ FILE----------------2_parcel.csv")

    parcel_df = parcel_df.rename(
        columns={"PRMNNT_PRCL_ID": "pid", "PRCL_STTS_CD": "parcel_status"}
    )

    parcel_df.to_csv(output_directory + "parcel_raw.csv", index=False)
    print(
        f"WROTE RAW LTSA DATA TO FILE:----------------{output_directory+'parcel_raw.csv'}"
    )

    # 3_titleparcel.csv
    title_parcel_df = (
        pd.read_csv(
            input_directory + "3_titleparcel.csv",
            usecols=["TITLE_NMBR", "LTB_DISTRICT_CD", "PRMNNT_PRCL_ID"],
            dtype={"TITLE_NMBR": str, "LTB_DISTRICT_CD": str, "PRMNNT_PRCL_ID": int},
        )
        .map(lambda x: x.strip() if isinstance(x, str) else x)
        .replace("", None)
        .replace(np.nan, None)
    )
    print("READ FILE----------------3_titleparcel.csv")

    title_parcel_df = title_parcel_df.rename(
        columns={
            "TITLE_NMBR": "title_number",
            "LTB_DISTRICT_CD": "land_title_district",
            "PRMNNT_PRCL_ID": "pid",
        }
    )

    title_parcel_df.to_csv(output_directory + "titleparcel_raw.csv", index=False)
    print(
        f"WROTE RAW LTSA DATA TO FILE:----------------{output_directory+'titleparcel_raw.csv'}"
    )

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
        .map(lambda x: x.strip() if isinstance(x, str) else x)
        .replace("", None)
        .replace(np.nan, None)
    )
    print("READ FILE----------------4_titleowner.csv")

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

    title_owner_df.to_csv(output_directory + "titleowner_raw.csv", index=False)

    read_files_elapsed_time = time.time() - read_files_start_time

    print(
        f"WROTE RAW LTSA DATA TO FILE:----------------{output_directory+'titleowner_raw.csv'}. Elapsed Time: {read_files_elapsed_time:.2f} seconds"
    )

    # Join dataframes
    parse_files_start_time = time.time()

    title_titleowner_df = pd.merge(
        title_owner_df, title_df, on=["title_number", "land_title_district"]
    )
    print("DATAFRAMES MERGED----------------title_owner_df, title_df")
    print(f"NUMBER OF ROWS IN title_titleowner_df: {len(title_titleowner_df)}")

    titleparcel_parcel_df = pd.merge(title_parcel_df, parcel_df, on="pid")
    print("DATAFRAMES MERGED----------------title_parcel_df AND parcel_df")
    print(f"NUMBER OF ROWS IN titleparcel_parcel_df: {len(titleparcel_parcel_df)}")

    active_pin_df = pd.merge(
        title_titleowner_df,
        titleparcel_parcel_df,
        on=["title_number", "land_title_district"],
    )
    print("DATAFRAMES MERGED----------------title_titleowner_df, titleparcel_parcel_df")
    print(f"NUMBER OF ROWS IN active_pin_df: {len(active_pin_df)}")

    # Group by title number to get a list of active pids associated with each title
    titlenumber_pids_df = (
        active_pin_df.groupby(["title_number", "land_title_district"])["pid"]
        .apply(list)
        .reset_index(name="pids")
    )
    print("GROUPED DATAFRAME CREATED----------------titlenumber_pids_df")

    # Format PIDs as strings
    titlenumber_pids_df["pids"] = titlenumber_pids_df["pids"].apply(pid_parser)

    # Merge dataframes to add in PIDs column and drop duplicate rows
    active_pin_df = pd.merge(
        active_pin_df, titlenumber_pids_df, on=["title_number", "land_title_district"]
    ).drop(columns=["pid"])
    print("DATAFRAMES MERGED----------------active_pin_df, titlenumber_pids_df")

    # Remove duplicate rows
    active_pin_df = active_pin_df.loc[active_pin_df.astype(str).drop_duplicates().index]
    print("DUPLICATE ROWS DROPPED----------------active_pin_df")

    parse_files_elapsed_time = time.time() - parse_files_start_time
    print(
        f"DATA PARSING COMPLETE. Elapsed Time: {parse_files_elapsed_time:.2f} seconds"
    )

    clean_active_pin_df(active_pin_df, output_directory, data_rules_url)


def run(input_directory, output_directory, data_rules_url):
    start_time = time.time()

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Parse the files
    parse_ltsa_files(input_directory, output_directory, data_rules_url)

    end_time = time.time()
    total_time = end_time - start_time

    print(f"All files parsed and cleaned. Total time elapsed: {total_time:.2f} seconds")


if __name__ == "__main__":
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
