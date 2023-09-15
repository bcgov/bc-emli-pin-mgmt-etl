import json
import pandas as pd
import datetime
import numpy as np
import requests


def pid_parser(pids):
    pids = (
        str(sorted(list(set(pids))))
        .replace("[", "")
        .replace("]", "")
        .replace(",", " |")
    )
    return pids


# Parses csv files from input_directory and outputs a single formatted csv file into the output_directory
def parse_ltsa_files(input_directory, output_directory, data_rules_url):
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
    print(f"READ FILE----------------1_title.csv")

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
    print(f"READ FILE----------------2_parcel.csv")

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
    print(f"READ FILE----------------3_titleparcel.csv")

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
    print(f"READ FILE----------------4_titleowner.csv")

    # Join dataframes
    print(f"NUMBER OF ROWS IN title_owner_df: {len(title_owner_df)}")
    print(f"NUMBER OF ROWS IN title_df: {len(title_df)}")
    title_titleowner_df = pd.merge(
        title_owner_df, title_df, on=["TITLE_NMBR", "LTB_DISTRICT_CD"]
    )
    print(f"DATAFRAMES MERGED----------------title_owner_df, title_df")
    print(
        f"NUMBER OF ROWS IN MERGED DATAFRAME title_titleowner_df: {len(title_titleowner_df)}"
    )

    print(f"NUMBER OF ROWS IN title_parcel_df: {len(title_parcel_df)}")
    print(f"NUMBER OF ROWS IN parcel_df: {len(parcel_df)}")
    titleparcel_parcel_df = pd.merge(title_parcel_df, parcel_df, on="PRMNNT_PRCL_ID")
    print(f"DATAFRAMES MERGED----------------title_parcel_df AND parcel_df")
    print(
        f"NUMBER OF ROWS IN MERGED DATAFRAME titleparcel_parcel_df: {len(titleparcel_parcel_df)}"
    )

    active_pin_df = pd.merge(
        title_titleowner_df, titleparcel_parcel_df, on=["TITLE_NMBR", "LTB_DISTRICT_CD"]
    )
    print(
        f"DATAFRAMES MERGED----------------title_titleowner_df, titleparcel_parcel_df"
    )
    print(f"NUMBER OF ROWS IN MERGED DATAFRAME active_pin_df: {len(active_pin_df)}")

    # Group by title number to get a list of active pids associated with each title
    active_parcel_df = active_pin_df.loc[active_pin_df["PRCL_STTS_CD"] != "I"]

    titlenumber_pids_df = (
        active_parcel_df.groupby("TITLE_NMBR")["PRMNNT_PRCL_ID"]
        .apply(list)
        .reset_index(name="pids")
    )
    print(f"GROUPED DATAFRAME CREATED----------------titlenumber_pids_df")

    # For each title number in grouped_df, put list of pids in proper sorted string format
    titlenumber_pids_df["pids"] = titlenumber_pids_df["pids"].apply(pid_parser)

    # Merge dataframes to add in PIDs column and drop duplicate rows
    print(f"NUMBER OF ROWS IN active_pin_df: {len(active_pin_df)}")
    print(f"NUMBER OF ROWS IN titlenumber_pids_df: {len(titlenumber_pids_df)}")
    active_pin_df = pd.merge(active_pin_df, titlenumber_pids_df, on="TITLE_NMBR").drop(
        columns=["PRMNNT_PRCL_ID"]
    )
    print(f"DATAFRAMES MERGED----------------active_pin_df, titlenumber_pids_df")
    print(f"NUMBER OF ROWS IN MERGED DATAFRAME active_pin_df: {len(active_pin_df)}")

    active_pin_df = active_pin_df.loc[active_pin_df.astype(str).drop_duplicates().index]
    print(f"DUPLICATE ROWS DROPPED----------------active_pin_df")
    print(f"NUMBER OF ROWS IN MERGED DATAFRAME active_pin_df: {len(active_pin_df)}")

    active_pin_df = active_pin_df.rename(
        columns={
            "TITLE_NMBR": "title_number",
            "LTB_DISTRICT_CD": "land_title_district",
            "TTL_STTS_CD": "title_status",
            "FRM_TTL_NMBR": "from_title_number",
            "FRM_LT_DISTRICT_CD": "from_land_title_district",
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
    print(f"DATAFRAME COLUMNS RENAMED----------------active_pin_df")

    clean_active_pin_df(active_pin_df, output_directory, data_rules_url)

    active_pin_df = active_pin_df.drop(
        columns=["occupation", "province_long", "PRCL_STTS_CD"]
    )

    # Write to output file
    # current_date_time = str(datetime.datetime.now())
    # active_pin_df.to_csv(
    #     output_directory + "processed_data_" + current_date_time + ".csv", index=False
    # )
    active_pin_df.to_csv(output_directory + "active_pin_uncleaned.csv", index=False)

    print(
        f"WROTE PROCESSED LTSA DATA TO FILE:----------------{output_directory+'active_pin_uncleaned.csv'}"
    )


def clean_active_pin_df(active_pin_df, output_directory, data_rules_url):
    print(
        f"GETTING DATA CLEANING RULES FROM CONFIG FILE:----------------{data_rules_url}"
    )

    json_url = data_rules_url

    try:
        response = requests.get(json_url)
        response.raise_for_status()  # Check for any errors in the request

        # Parse the JSON content into a Python dictionary
        data_rules = json.loads(response.text)

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

    # Apply cleaning rules to each column
    for column, rule in data_rules["column_rules"].items():
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
                active_pin_df[column] = active_pin_df[column].str.replace(
                    replacement, ""
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
            datatype = rule["switch_column_value"]["datatype"]

            for value in active_pin_df[from_column]:
                if datatype == "int" and value and value.isdigit():
                    active_pin_df[to_column] = np.where(
                        (active_pin_df[from_column] == value),
                        active_pin_df[from_column],
                        active_pin_df[to_column],
                    )

    print(f"CLEANING RULES APPLIED TO FILE:----------------active_pin.csv")

    active_pin_df = active_pin_df.drop(
        columns=["occupation", "province_long", "PRCL_STTS_CD"]
    )

    active_pin_df.to_csv(output_directory + "active_pin.csv", index=False)

    print(
        f"WROTE CLEANED LTSA DATA TO FILE:----------------{output_directory+'active_pin.csv'}"
    )


def run(input_directory, output_directory, data_rules_url):
    # Parse the files
    parse_ltsa_files(input_directory, output_directory, data_rules_url)
