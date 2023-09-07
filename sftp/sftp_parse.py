import pandas as pd
import os


# Parses csv files from input_directory and outputs a single formatted csv file into the output_directory
def parse_sftp_files(input_directory, output_directory):
    # Set directory to folder containing csv file
    os.chdir(input_directory)

    # Make an array of csv files
    csv_files = [f for f in os.listdir() if f.endswith(".csv")]
    csv_files = sorted(csv_files)

    # Set each csv file to a dataframe
    dfs = []
    for csv in csv_files:
        df = pd.read_csv(csv)
        dfs.append(df)

    # Name dfs appropriately
    title_table = dfs[0]
    parcel_table = dfs[1]
    title_parcel_table = dfs[2]
    title_owner_table = dfs[3]

    # Join dataframes on title number
    merged_df_1 = pd.merge(title_table, parcel_table, on="TITLE_NMBR")
    merged_df_2 = pd.merge(title_parcel_table, title_owner_table, on="TITLE_NMBR")
    merged_df = pd.merge(merged_df_1, merged_df_2, on="TITLE_NMBR")

    # Remove unnecessary columns and spaces, make column names lowercase
    # Check datatypes of columns
    # Check for dropped records (Counts) - There is 78714 rows in merged (75681 in title owner table)
    merged_df = merged_df.drop(
        columns=[
            "NATURE_OF_XFER1",
            "NATURE_OF_XFER2",
            "TTL_ENTRD_DT",
            "TTL_CNCL_DT",
            "DCMNT_ACPTNC_DT",
            "MRKT_VALUE_AMNT",
            "REGISTRATION_DATE",
            "CANCELLATION_DATE",
            "REGDES",
            "TX_ATHRTY_NM",
            "LGDS",
            "AIRSPACE_IND",
            "STRATA_PLAN_NMBR",
            "NMRTR_NMBR",
            "DNMNTR_NMBR",
            "TNNCY_TYP_IND",
            "OCCPTN_DESC",
            "TTL_RMRK_TEXT",
        ]
    )
    merged_df.columns = merged_df.columns.str.lower()
    merged_df = merged_df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Group by title number to get a list of pids associated with each title
    grouped_df = (
        merged_df.groupby("title_nmbr")["prmnnt_prcl_id_x"]
        .apply(list)
        .reset_index(name="pids")
    )

    # For each title number in grouped_df, put list of pids in proper sorted string format
    grouped_df["pids"] = grouped_df["pids"].tolist()
    grouped_df["pids"] = grouped_df["pids"].apply(lambda x: str(sorted(list(set(x)))))
    grouped_df["pids"] = grouped_df["pids"].apply(
        lambda x: x.replace("[", "").replace("]", "").replace(",", " |")
    )

    # Merge dataframes to add in PIDs column and drop duplicate rows
    final_df = pd.merge(merged_df, grouped_df, on="title_nmbr")
    final_df = final_df.drop(
        columns=[
            "prmnnt_prcl_id_x",
            "prmnnt_prcl_id_y",
            "ltb_district_cd_x",
            "ltb_district_cd_y",
            "ttl_ownrshp_nmbr",
            "prcl_stts_cd",
        ]
    )
    final_df = final_df.loc[final_df.astype(str).drop_duplicates().index]

    # Write to output file
    final_df.to_csv(output_directory)
