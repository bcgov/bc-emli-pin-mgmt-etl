import csv
import os
from utils.ltsa_parser import pid_parser
from utils.ltsa_parser import parse_ltsa_files
from utils.ltsa_parser import run

pidListMultiplePids = [123, 234, 345]
pidListOnePid = [123]
pidListNoPid = []

inputDirectory = ""
outputDirectory = ""
dataRulesUrl = "https://raw.githubusercontent.com/bcgov/bc-emli-pin-mgmt-etl/main/data_rules.json"

rawTitleFileName = "title_raw.csv"
rawParcelFileName = "parcel_raw.csv"
rawTitleparcelFileName = "titleparcel_raw.csv"
rawTitleownerFileName = "titleowner_raw.csv"
activePinFileName = "active_pin.csv"

title_test_file = "1_title.csv"
title_rows = [
    ["TITLE_NMBR", "LTB_DISTRICT_CD", "TTL_STTS_CD", "FRM_TTL_NMBR", "FRM_LT_DISTRICT_CD", "NATURE_OF_XFER1", "NATURE_OF_XFER2", "TTL_ENTRD_DT", "TTL_CNCL_DT", "DCMNT_ACPTNC_DT", "MRKT_VALUE_AMNT"],
    ["AA100060E  ", "NW", "C", "RD68051E   ", "NW", "FEE", " ", "1987-06-03", "", "1987-06-02", "$200,000.00 "],
]

parcel_test_file = "2_parcel.csv"
parcel_rows = [
    ["PRMNNT_PRCL_ID", "PRCL_STTS_CD", "REGISTRATION_DATE", "CANCELLATION_DATE,REGDES", "TX_ATHRTY_NM,LGDS", "AIRSPACE_IND", "STRATA_PLAN_NMBR", "TITLE_NMBR"],
    ["48445", "A", "", "", "Central Coast Regional District, Cariboo Assessment Area, DISTRICT LOT 1617, RANGE 2, COAST DISTRICT", "N", "", "AA100060E  "],
]

titleparcel_test_file = "3_titleparcel.csv"
titleparcel_rows = [
    ["TITLE_NMBR","LTB_DISTRICT_CD","PRMNNT_PRCL_ID"],
    ["AA100060E  ", "NW", "48445"],
]

titleowner_test_file = "4_titleowner.csv"
titleowner_rows = [
    ["TITLE_NMBR", "LTB_DISTRICT_CD", "TTL_OWNRSHP_NMBR", "NMRTR_NMBR", "DNMNTR_NMBR", "TNNCY_TYP_IND", "CLIENT_GVN_NM", "CLIENT_LST_NM_1", "CLIENT_LST_NM_2", "OCCPTN_DESC", "INCRPRTN_NMBR", "ADDRS_DESC_1", "ADDRS_DESC_2", "ADDRS_CITY", "ADDRS_PROV_CD", "ADDRS_PROV_ST", "ADDRS_CNTRY", "ADDRS_PSTL_CD", "TTL_RMRK_TEXT"],
    ["AA100060E  ", "NW", 1, 1, 1, "J", "ANN MARY", "LIVESLEY", "", "HOMEMAKER  ", " ", "10560 BLUNDELL ROAD", "", "RICHMOND", "BC", " ", "CANADA", "V6Y 1L1", ""],
]

def test_pid_parser():
    assert pid_parser(pidListMultiplePids) == '123|234|345'
    assert pid_parser(pidListOnePid) == '123'
    assert pid_parser(pidListNoPid) == ''

def create_csvs():
    with open(inputDirectory + title_test_file, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file, dialect='excel')
            writer.writerows(title_rows)

    with open(inputDirectory + parcel_test_file, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, dialect='excel')
        writer.writerows(parcel_rows)

    with open(inputDirectory + titleparcel_test_file, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, dialect='excel')
        writer.writerows(titleparcel_rows)

    with open(inputDirectory + titleowner_test_file, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, dialect='excel')
        writer.writerows(titleowner_rows)

def test_parse_ltsa_files(mocker):
    create_csvs()
    # mocker.patch('utils.ltsa_parser.clean_active_pin_df')
    parse_ltsa_files(inputDirectory, outputDirectory, dataRulesUrl)
    os.remove(title_test_file)
    os.remove(parcel_test_file)
    os.remove(titleparcel_test_file)
    os.remove(titleowner_test_file)
    os.remove(rawTitleFileName)
    os.remove(rawParcelFileName)
    os.remove(rawTitleparcelFileName)
    os.remove(rawTitleownerFileName)
    os.remove(activePinFileName)
    
def test_run(mocker):
    mocker.patch('utils.ltsa_parser.parse_ltsa_files')
    mocker.patch('os.makedirs')
    run(inputDirectory, outputDirectory, dataRulesUrl)
     