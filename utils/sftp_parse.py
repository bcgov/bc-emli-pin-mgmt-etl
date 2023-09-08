import pandas as pd
import datetime
import numpy as np


def pid_parser(pids):
    pids = (
        str(sorted(list(set(pids))))
        .replace("[", "")
        .replace("]", "")
        .replace(",", " |")
    )
    return pids


# Parses csv files from input_directory and outputs a single formatted csv file into the output_directory
# Add in more printing statements
def parse_sftp_files(input_directory, output_directory):
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
    print(f"READ TITLE CSV")

    parcel_df = (
        pd.read_csv(
            input_directory + "2_parcel.csv",
            usecols=["PRMNNT_PRCL_ID"],
            dtype={"PRMNNT_PRCL_ID": int},
        )
        .map(lambda x: x.strip() if isinstance(x, str) else x)
        .replace("", None)
        .replace(np.nan, None)
    )
    print(f"READ PARCEL CSV")

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
    print(f"READ TITLEPARCEL CSV")

    title_owner_df = (
        pd.read_csv(
            input_directory + "4_titleowner.csv",
            usecols=[
                "TITLE_NMBR",
                "LTB_DISTRICT_CD",
                "CLIENT_GVN_NM",
                "CLIENT_LST_NM_1",
                "CLIENT_LST_NM_2",
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
    print(f"READ TITLEOWNER CSV")

    # LOG NUMBER OF ROWS BEFORE AND AFTER JOINS
    # Join dataframes
    title_titleowner_df = pd.merge(
        title_owner_df, title_df, on=["TITLE_NMBR", "LTB_DISTRICT_CD"]
    )
    titleparcel_parcel_df = pd.merge(title_parcel_df, parcel_df, on="PRMNNT_PRCL_ID")
    active_pin_df = pd.merge(
        title_titleowner_df, titleparcel_parcel_df, on=["TITLE_NMBR", "LTB_DISTRICT_CD"]
    )

    # Group by title number to get a list of pids associated with each title
    titlenumber_pids_df = (
        active_pin_df.groupby("TITLE_NMBR")["PRMNNT_PRCL_ID"]
        .apply(list)
        .reset_index(name="pids")
    )

    # For each title number in grouped_df, put list of pids in proper sorted string format
    titlenumber_pids_df["pids"] = titlenumber_pids_df["pids"].apply(pid_parser)

    # Merge dataframes to add in PIDs column and drop duplicate rows
    active_pin_df = pd.merge(active_pin_df, titlenumber_pids_df, on="TITLE_NMBR").drop(
        columns=["PRMNNT_PRCL_ID"]
    )
    active_pin_df = active_pin_df.loc[active_pin_df.astype(str).drop_duplicates().index]

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

    # Write to output file
    # current_date_time = str(datetime.datetime.now())
    # active_pin_df.to_csv(
    #     output_directory + "processed_data_" + current_date_time + ".csv"
    # )
    active_pin_df.to_csv(output_directory + "processed_data.csv")

    print(
        f"WROTE PROCESSED LTSA DATA TO FILE:----------------{output_directory+'processed_data.csv'}"
    )
