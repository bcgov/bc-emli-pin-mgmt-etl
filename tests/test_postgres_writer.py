from unittest.mock import patch
import pandas as pd
import sqlalchemy
from utils.postgres_writer import (
    write_dataframe_to_postgres,
    run,
    insert_postgres_table_if_rows_not_exist,
    get_row_count,
)
from sqlalchemy import create_engine
import pytest

dataframe = pd.DataFrame(
    data={
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

file_list = [
    "title_raw.csv",
    "parcel_raw.csv",
    "titleparcel_raw.csv",
    "titleowner_raw.csv",
    "activepin.csv",
]

db = create_engine("sqlite:///:memory:")


@patch("utils.postgres_writer.insert_postgres_table_if_rows_not_exist")
@patch("sqlalchemy.engine.Engine.connect")
@patch("utils.postgres_writer.get_row_count", return_value=5)
def test_write_dataframe_to_postgres(insert_mock, count_mock, connect_mock):
    numberOfRowsInserted = write_dataframe_to_postgres(
        dataframe, "tablename", db, "etlJobId", ["tablename"], 5
    )
    assert insert_mock.called_once()
    assert count_mock.called_once()
    assert connect_mock.called_once()
    assert (
        numberOfRowsInserted == 0
    )  # Number of rows before and after insert is the same


@patch(
    "utils.postgres_writer.write_dataframe_to_postgres",
    side_effect=sqlalchemy.exc.OperationalError,
)
def test_write_dataframe_to_postgres_error(writer_mock):
    with pytest.raises(sqlalchemy.exc.OperationalError):
        write_dataframe_to_postgres(
            dataframe, "tablename", db, "etlJobId", ["tablename"], 5
        )
    assert writer_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
def test_insert_postgres_table_if_rows_not_exist(connect_mock):
    insert_postgres_table_if_rows_not_exist(dataframe, "tablename", db, "uniqueColumns")
    assert connect_mock.called_once()


@patch(
    "utils.postgres_writer.insert_postgres_table_if_rows_not_exist",
    side_effect=sqlalchemy.exc.OperationalError,
)
def test_insert_postgres_table_if_rows_not_exist_error(insert_mock):
    with pytest.raises(sqlalchemy.exc.OperationalError):
        insert_postgres_table_if_rows_not_exist(
            dataframe, "tablename", db, "uniqueKeyColumns"
        )
    assert insert_mock.called_once()


@patch("os.listdir", return_value=file_list)
@patch("os.path.join")
@patch("pandas.read_csv")
@patch(
    "utils.postgres_writer.write_dataframe_to_postgres"
)  # write_dataframe_to_postgres already tested
def test_run(listdir_mock, pathjoin_mock, readcsv_mock, write_mock):
    run("", "etlJobId", "databaseName")
    assert listdir_mock.called_once()
    assert pathjoin_mock.called_once()
    assert readcsv_mock.called_once()
    assert write_mock.called_once()


@patch("utils.postgres_writer.run", side_effect=FileNotFoundError)
def test_run_error(write_mock):
    with pytest.raises(FileNotFoundError):
        run("", "etlJobId", "databaseName")
    assert write_mock.called_once()


@patch("sqlalchemy.engine.Engine.connect")
def test_get_row_count(connect_mock):
    get_row_count("tablename", db)
    assert connect_mock.called_once()


@patch(
    "utils.postgres_writer.get_row_count", side_effect=sqlalchemy.exc.OperationalError
)
def test_get_row_count_error(count_mock):
    with pytest.raises(sqlalchemy.exc.OperationalError):
        get_row_count("tablename", db)
    assert count_mock.called_once()
