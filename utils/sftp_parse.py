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
    title_owner_df = (
        pd.read_csv(
            input_directory + "4_titleowner.csv",
            usecols=[
                "TITLE_NMBR",
                "LTB_DISTRICT_CD",
                "CLIENT_GVN_NM",
                "CLIENT_LST_NM_1",
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

    # Join dataframes
    title_titleowner_df = pd.merge(
        title_owner_df, title_df, on=["TITLE_NMBR", "LTB_DISTRICT_CD"]
    )
    titleparcel_parcel_df = pd.merge(title_parcel_df, parcel_df, on="PRMNNT_PRCL_ID")
    active_pin_df = pd.merge(
        title_titleowner_df, titleparcel_parcel_df, on=["TITLE_NMBR", "LTB_DISTRICT_CD"]
    )

    # Make column names lowercase
    active_pin_df.columns = active_pin_df.columns.str.lower()

    # Group by title number to get a list of pids associated with each title
    titlenumber_pids_df = (
        active_pin_df.groupby("title_nmbr")["prmnnt_prcl_id"]
        .apply(list)
        .reset_index(name="pids")
    )

    # For each title number in grouped_df, put list of pids in proper sorted string format
    titlenumber_pids_df["pids"] = titlenumber_pids_df["pids"].apply(pid_parser)

    # Merge dataframes to add in PIDs column and drop duplicate rows
    active_pin_df = pd.merge(active_pin_df, titlenumber_pids_df, on="title_nmbr").drop(
        columns=["prmnnt_prcl_id"]
    )
    active_pin_df = active_pin_df.loc[active_pin_df.astype(str).drop_duplicates().index]

    # Write to output file
    current_date_time = str(datetime.datetime.now())
    active_pin_df.to_csv(
        output_directory + "processed_data_" + current_date_time + ".csv"
    )


parse_sftp_files(
    "/Users/emendelson/Downloads/export/EMLI_UPDATE_20230824/EMLI_UPDATE_20230824/",
    "/Users/emendelson/Downloads/export/EMLI_UPDATE_20230824/EMLI_UPDATE_20230824/",
)
