import csv
import os
from unittest.mock import patch
import pytest
from utils.ltsa_parser import (
    pid_parser,
    parse_ltsa_files,
    run,
    load_data_cleaning_rules,
)

pidListMultiplePids = [123, 234, 345]
pidListMultiplePidsParsed = "123|234|345"
pidListOnePid = [123]
pidListOnePidParsed = "123"
pidListNoPid = []
pidListNoPidParsed = ""

inputDirectory = ""
outputDirectory = ""
dataRulesUrl = (
    "https://raw.githubusercontent.com/bcgov/bc-emli-pin-mgmt-etl/main/data_rules.json"
)

rawTitleFileName = "title_raw.csv"
rawParcelFileName = "parcel_raw.csv"
rawTitleparcelFileName = "titleparcel_raw.csv"
rawTitleownerFileName = "titleowner_raw.csv"
activePinFileName = "active_pin.csv"

title_test_file = "EMLI_1_WKLY_TITLE.csv"
title_rows = [
    [
        "TITLE_NMBR",
        "LTB_DISTRICT_CD",
        "TTL_STTS_CD",
        "FRM_TTL_NMBR",
        "FRM_LT_DISTRICT_CD",
        "NATURE_OF_XFER1",
        "NATURE_OF_XFER2",
        "TTL_ENTRD_DT",
        "TTL_CNCL_DT",
        "DCMNT_ACPTNC_DT",
        "MRKT_VALUE_AMNT",
    ],
    [
        "AA12345E",
        "AB",
        "R",
        "AB12345E",
        "AB",
        "FEE",
        "",
        "1987-06-03",
        "",
        "1987-06-02",
        "$200,000.00",
    ],
]

parcel_test_file = "EMLI_2_WKLY_PARCEL.csv"
parcel_rows = [
    [
        "PRMNNT_PRCL_ID",
        "PRCL_STTS_CD",
        "REGISTRATION_DATE",
        "CANCELLATION_DATE,REGDES",
        "TX_ATHRTY_NM,LGDS",
        "AIRSPACE_IND",
        "STRATA_PLAN_NMBR",
        "TITLE_NMBR",
    ],
    [
        "48445",
        "A",
        "",
        "",
        "Example Tax Authority Name",
        "N",
        "",
        "AA12345E",
    ],
]

titleparcel_test_file = "EMILY_3_WKLY_TITLEPARCEL.csv"
titleparcel_rows = [
    ["TITLE_NMBR", "LTB_DISTRICT_CD", "PRMNNT_PRCL_ID"],
    ["AA12345E", "AB", "48445"],
]

titleowner_test_file = "EMLI_4_WKLY_TITLEOWNER.csv"
titleowner_rows = [
    [
        "TITLE_NMBR",
        "LTB_DISTRICT_CD",
        "TTL_OWNRSHP_NMBR",
        "NMRTR_NMBR",
        "DNMNTR_NMBR",
        "TNNCY_TYP_IND",
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
        "TTL_RMRK_TEXT",
    ],
    [
        "AA12345E",
        "AB",
        "1",
        "1",
        "1",
        "J",
        "Jane",
        "Green",
        "",
        "Chef",
        "",
        "123 main street",
        "",
        "RICHMOND",
        "BC",
        "",
        "CANADA",
        "ABC 123",
        "",
    ],
]


def create_csvs():
    with open(inputDirectory + title_test_file, "w", newline="") as csv_file:
        writer = csv.writer(csv_file, dialect="excel")
        writer.writerows(title_rows)

    with open(inputDirectory + parcel_test_file, "w", newline="") as csv_file:
        writer = csv.writer(csv_file, dialect="excel")
        writer.writerows(parcel_rows)

    with open(inputDirectory + titleparcel_test_file, "w", newline="") as csv_file:
        writer = csv.writer(csv_file, dialect="excel")
        writer.writerows(titleparcel_rows)

    with open(inputDirectory + titleowner_test_file, "w", newline="") as csv_file:
        writer = csv.writer(csv_file, dialect="excel")
        writer.writerows(titleowner_rows)


def remove_csvs(listOfFiles):
    for file in listOfFiles:
        os.remove(file)


def test_pid_parser():
    assert pid_parser(pidListMultiplePids) == pidListMultiplePidsParsed
    assert pid_parser(pidListOnePid) == pidListOnePidParsed
    assert pid_parser(pidListNoPid) == pidListNoPidParsed


def test_parse_ltsa_files():
    create_csvs()
    parse_ltsa_files(inputDirectory, outputDirectory, dataRulesUrl)
    remove_csvs(
        [
            title_test_file,
            parcel_test_file,
            titleparcel_test_file,
            titleowner_test_file,
            rawTitleFileName,
            rawParcelFileName,
            rawTitleparcelFileName,
            rawTitleownerFileName,
            activePinFileName,
        ]
    )


@patch("utils.ltsa_parser.clean_active_pin_df", side_effect=ValueError)
def test_parse_ltsa_files_error(clean_mock):
    create_csvs()
    with pytest.raises(ValueError):
        parse_ltsa_files(inputDirectory, outputDirectory, dataRulesUrl)
    assert clean_mock.calledOnce()
    remove_csvs(
        [
            title_test_file,
            parcel_test_file,
            titleparcel_test_file,
            titleowner_test_file,
            rawTitleFileName,
            rawParcelFileName,
            rawTitleparcelFileName,
            rawTitleownerFileName,
        ]
    )


def test_load_data_cleaning_rules():
    dataCleaningRules = load_data_cleaning_rules(dataRulesUrl)
    assert type(dataCleaningRules) == dict


@patch("utils.ltsa_parser.parse_ltsa_files")
@patch("os.makedirs")
def test_run(parser_mock, makedirs_mock):
    run(inputDirectory, outputDirectory, dataRulesUrl)
    assert parser_mock.calledOnce()
    assert makedirs_mock.calledOnce()
