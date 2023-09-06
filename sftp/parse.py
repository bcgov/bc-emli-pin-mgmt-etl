import pandas as pd
# import os

# Takes array of csv_files, and parses them into a single formatted dataframe
def parse(csv_files):
    # # Set directory to folder containing csv file
    # os.chdir('../../Downloads/export/EMLI_UPDATE_20230824/EMLI_UPDATE_20230824')

    # Make an array of csv files
    # csv_files = [f for f in os.listdir() if f.endswith('.csv')]

    # Set each csv file to a dataframe
    dfs = []
    for csv in csv_files:
        df = pd.read_csv(csv)
        dfs.append(df)

    # Join dataframes on title number
    merged_df_1 = pd.merge(dfs[0], dfs[1], on='TITLE_NMBR')
    merged_df_2 = pd.merge(dfs[2], dfs[3], on='TITLE_NMBR')
    merged_df = pd.merge(merged_df_1, merged_df_2, on='TITLE_NMBR')

    # Remove unnecessary columns and spaces
    merged_df = merged_df.drop(columns=['NATURE_OF_XFER1', 'NATURE_OF_XFER2', 'TTL_ENTRD_DT', 'TTL_CNCL_DT', 'DCMNT_ACPTNC_DT', 'MRKT_VALUE_AMNT', 'REGISTRATION_DATE', 'CANCELLATION_DATE', 'REGDES', 'TX_ATHRTY_NM', 'LGDS', 'AIRSPACE_IND', 'STRATA_PLAN_NMBR', 'NMRTR_NMBR', 'DNMNTR_NMBR', 'TNNCY_TYP_IND', 'OCCPTN_DESC', 'TTL_RMRK_TEXT'])
    merged_df = merged_df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Group by title number to get a list of pids associated with each title
    grouped_df = merged_df.groupby('TITLE_NMBR')['PRMNNT_PRCL_ID_x'].apply(list).reset_index(name='PIDs')

    # For each title number, put list of pids in proper string format
    grouped_df['PIDs'] = grouped_df['PIDs'].tolist()

    for pids in grouped_df['PIDs']:
        index = (grouped_df['PIDs'].tolist()).index(pids)
        pids = str(sorted(list(set(pids))))
        pids = pids.replace("[", "").replace("]", "").replace(",", "|")
        grouped_df['PIDs'][index] = pids

    # Merge dataframes to add in PIDs column and drop duplicate rows
    final_df = pd.merge(merged_df, grouped_df, on='TITLE_NMBR')
    final_df = final_df.drop(columns=['PRMNNT_PRCL_ID_x', 'PRMNNT_PRCL_ID_y', 'LTB_DISTRICT_CD_x', 'LTB_DISTRICT_CD_y','TTL_OWNRSHP_NMBR', 'PRCL_STTS_CD'])
    final_df = final_df.loc[final_df.astype(str).drop_duplicates().index]

    print(final_df.query('`TITLE_NMBR` == "A13613" or  `TITLE_NMBR` == "AC206761" or `TITLE_NMBR` == "AD134401"'))
    