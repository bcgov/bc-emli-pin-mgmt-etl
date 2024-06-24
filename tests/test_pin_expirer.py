import csv
import os
from unittest.mock import patch
import pandas as pd
import requests
from utils.pin_expirer import run, create_expiration_df, expire_pins
from sqlalchemy import create_engine
import pytest

inputDirectory = ""

title_test_file = "20240417-Title.csv"
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
        "C",
        "AB12345E",
        "AB",
        "FEE",
        "",
        "1987-06-03",
        "",
        "1987-06-02",
        "$200,000.00",
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

title_rows_missing_fields = [
    [
        "TITLE_NMBR",
        "LTB_DISTRICT_CD",
    ],
    [
        "AA12345E",
        "AB",
    ],
]

activePinDf = pd.DataFrame(
    data={
        "live_pin_id": ["123456"],
        "title_number": ["AA123456E"],
        "land_title_district": ["AB"],
        "title_status": ["R"],
        "from_title_number": [""],
        "from_land_title_district": [""],
        "given_name": ["Jane"],
        "last_name_1": ["Green"],
        "last_name_2": [""],
        "incorporation_number": [""],
        "address_line_1": ["123 Main St"],
        "address_line_2": [""],
        "city": ["Vancouver"],
        "province_abbreviation": ["BC"],
        "province_long": [""],
        "country": ["Canada"],
        "postal_code": ["ABC 123"],
        "created_at": ["2023-11-29"],
        "updated_at": [""],
    }
)

emptyExpiredTitlesDf = pd.DataFrame(
    data={
        "title_number": [],
    }
)

db = create_engine("sqlite:///:memory:")

expireApiUrl = "expireAPIKeyURL"
vhersApiKey = "apikey"


def create_title_csv():
    with open(inputDirectory + title_test_file, "w", newline="") as csv_file:
        writer = csv.writer(csv_file, dialect="excel")
        writer.writerows(title_rows)


def remove_csvs(listOfFiles):
    for file in listOfFiles:
        os.remove(file)


def test_create_expiration_df():
    create_title_csv()
    cancelledTitlesDf = create_expiration_df(inputDirectory)
    assert type(cancelledTitlesDf) == pd.core.frame.DataFrame
    assert len(cancelledTitlesDf.index) == 1
    remove_csvs(["20240417-Title.csv"])


@patch("pandas.read_csv", return_value=ValueError)
def test_create_expiration_df_error(readcsv_mock):
    create_title_csv()
    with pytest.raises(AttributeError):
        create_expiration_df(inputDirectory)
    remove_csvs(["20240417-Title.csv"])
    assert readcsv_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
@patch("pandas.read_sql", return_value=activePinDf)
@patch("requests.post")
def test_expire_pins(connect_mock, read_mock, requests_mock):
    create_title_csv()
    cancelledTitlesDf = create_expiration_df(inputDirectory)
    expire_pins(cancelledTitlesDf, db, expireApiUrl, vhersApiKey)
    assert connect_mock.called_once()
    assert read_mock.called_once()
    assert requests_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
@patch("pandas.read_sql", return_value=ValueError)
def test_expire_pins_error(connect_mock, read_mock):
    create_title_csv()
    cancelledTitlesDf = create_expiration_df(inputDirectory)
    with pytest.raises(TypeError):
        expire_pins(cancelledTitlesDf, db, expireApiUrl, vhersApiKey)
    remove_csvs(["20240417-Title.csv"])
    assert connect_mock.called_once()
    assert read_mock.called_once()


# @patch("sqlalchemy.engine.Engine.connect")
# @patch("pandas.read_sql", return_value=activePinDf)
# def test_expire_pins_api_error(connect_mock, readsql_mock):
#     create_title_csv()
#     cancelledTitlesDf = create_expiration_df(inputDirectory)
#     with pytest.raises(requests.exceptions.MissingSchema):
#         expire_pins(cancelledTitlesDf, db, expireApiUrl, vhersApiKey)
#     remove_csvs(["EMLI_1_WKLY_TITLE.csv"])
#     assert connect_mock.called_once()
#     assert readsql_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
@patch("pandas.read_sql")
def test_expire_pins_empty_df(connect_mock, readsql_mock):
    expire_pins(emptyExpiredTitlesDf, db, expireApiUrl, vhersApiKey)
    assert not connect_mock.called
    assert not readsql_mock.called


@patch("sqlalchemy.engine.Engine.connect")
@patch("utils.pin_expirer.expire_pins")
def test_run(connect_mock, expire_mock):
    create_title_csv()
    run(inputDirectory, expireApiUrl, vhersApiKey, "databaseName")
    remove_csvs(["20240417-Title.csv"])
    assert connect_mock.called_once()
    assert expire_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
@patch("utils.pin_expirer.run", return_value=ValueError)
def test_run_error(run_mock, connect_mock):
    create_title_csv()
    with pytest.raises(KeyError):
        run(inputDirectory, expireApiUrl, vhersApiKey, "databaseName")
    remove_csvs(["20240417-Title.csv"])
    assert run_mock.called_once()
    assert connect_mock.called_once()
