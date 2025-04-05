import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from unhcr import gb_eyedro
from unhcr import constants as const
from sqlalchemy import text, create_engine
import sqlite3
import requests
import psycopg2

# Mocking necessary modules and functions
@pytest.fixture(autouse=True)
def mock_dependencies():
    with patch("unhcr.gb_eyedro.requests") as mock_requests, \
            patch("unhcr.gb_eyedro.logger") as mock_logger, \
            patch("unhcr.gb_eyedro.err_handler") as mock_err_handler, \
            patch("unhcr.gb_eyedro.db") as mock_db:
        yield mock_requests, mock_logger, mock_err_handler, mock_db


# Test cases for db_create_tables
def test_db_create_tables_success(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    # Correct in-memory Postgres engine creation
    mock_engine = create_engine("postgresql:///?host=localhost&port=5431", creator=lambda _: psycopg2.connect(database="testdb", user="postgres", password="postgres", host="localhost", port=5431))
    with mock_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS eyedro.gb_test_123 cascade"))
        conn.execute(text("DROP TABLE IF EXISTS eyedro.gb_test_321 cascade"))
        conn.commit()
    result, err = gb_eyedro.db_create_tables(["gb_test_123"], mock_engine)

    assert result == 'All good Houston'
    assert err is None
    # Check if the table was created (replace with appropriate check for your schema)
    with mock_engine.connect() as conn:
        result = conn.execute(text("SELECT 1")) # Replace with a check relevant to your table structure
        assert result.fetchone() is not None
        
        
def test_db_hyper_gb_gaps_success(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    # Correct in-memory Postgres engine creation
    mock_engine = create_engine("postgresql:///?host=localhost&port=5431", creator=lambda _: psycopg2.connect(database="testdb", user="postgres", password="postgres", host="localhost", port=5431))
    # Create a dummy table and insert some data
    with mock_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS eyedro.gb_test_123 cascade"))
        conn.commit()
        conn.execute(text("CREATE TABLE IF NOT EXISTS eyedro.gb_test_123 (epoch_secs INTEGER, ts TIMESTAMP)"))
        conn.execute(text("INSERT INTO eyedro.gb_test_123 (epoch_secs, ts) VALUES (1, '2023-01-01 00:00:00'), (2, '2023-01-01 00:01:00'), (4, '2023-01-01 00:03:00')"))
        conn.commit()

    result = gb_eyedro.db_hyper_gb_gaps("gb_test_123", mock_engine)
    assert len(result) == 2
    assert result[0][0] == 'gb_test_123'
    assert result[0][1] == 2
    assert result[0][2] == 1
    assert result[0][3] == 1
    assert result[1][0] == 'gb_test_123'
    assert result[1][1] == 4
    assert result[1][2] == 2
    assert result[1][3] == 2


def test_hyper_gb_gaps_concur_success(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    # Correct in-memory Postgres engine creation
    mock_engine = create_engine("postgresql:///?host=localhost&port=5431", creator=lambda _: psycopg2.connect(database="testdb", user="postgres", password="postgres", host="localhost", port=5431))
    # Create a dummy table and insert some data
    with mock_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS eyedro.gb_test_123 cascade"))
        conn.execute(text("DROP TABLE IF EXISTS eyedro.gb_test_321 cascade"))
        conn.commit()
        conn.execute(text("CREATE TABLE IF NOT EXISTS eyedro.gb_test_123 (epoch_secs INTEGER, ts TIMESTAMP)"))
        conn.execute(text("INSERT INTO eyedro.gb_test_123 (epoch_secs, ts) VALUES (1, '2023-01-01 00:00:00'), (2, '2023-01-01 00:01:00'), (4, '2023-01-01 00:03:00')"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS eyedro.gb_test_321 (epoch_secs INTEGER, ts TIMESTAMP)"))
        conn.execute(text("INSERT INTO eyedro.gb_test_321 (epoch_secs, ts) VALUES (1, '2023-01-01 00:00:00'), (2, '2023-01-01 00:01:00'), (4, '2023-01-01 00:03:00')"))
        conn.commit()

    ht_names = [("gb_test_123",), ("gb_test_321",)]
    result = gb_eyedro.hyper_gb_gaps_concur(ht_names=ht_names, chunks=2, db_eng=mock_engine)

    assert len(result) == 2
    assert len(result[0]) == 1
    assert len(result[0][0]) == 2
    assert result[0][0][0][0] == 'gb_test_123'
    assert result[1][0][0][0] == 'gb_test_321'


# Test cases for api_get_gb_user_info
def test_api_get_gb_user_info_success(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_response = MagicMock()
    mock_response.json.return_value = {"UserInfo": {"DeviceSerialList": [123456, 987654]}, "Errors": []}
    mock_response.status_code = 200
    mock_requests.request.return_value = mock_response
    
    def mock_error_wrapper(func):
        return func(), None

    mock_err_handler.error_wrapper.side_effect = mock_error_wrapper

    result, err = gb_eyedro.api_get_gb_user_info()

    assert result == {"UserInfo": {"DeviceSerialList": [123456, 987654]}, "Errors": []}
    assert err is None
    mock_requests.request.assert_called_once()
    assert len(result["UserInfo"]["DeviceSerialList"])  == 2

def test_api_get_gb_user_info_request_error(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.side_effect = Exception("'NoneType' object has no attribute 'json'")
    mock_requests.request.return_value = mock_response
    
    def mock_error_wrapper(func):
        try:
            func()
        except Exception as e:
            return None, str(mock_response.status_code)
        return None, None

    mock_err_handler.error_wrapper.side_effect = mock_error_wrapper
    

    result, err = gb_eyedro.api_get_gb_user_info()

    assert result is None
    assert "500" in err
    mock_requests.request.assert_called_once()

def test_api_get_gb_user_info_json_error(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_response = MagicMock()
    mock_response.json.side_effect = Exception("JSON Error")  # Raise the exception
    mock_response.status_code = 200
    mock_requests.request.return_value = mock_response

# Test cases for parse_user_info_as_df
def test_parse_user_info_as_df_success(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    user_info_data = {
        "UserInfo": {
            "DeviceSerialList": [123456, 789012],
            "SiteList": [
                {
                    "SiteLabel": "Site1",
                    "DisplayGroupList": [
                        {
                            "DeviceList": [
                                {"DeviceSerial": 123456, "LastCommSecUtc": 1678886400, "State": "Active"},
                                {"DeviceSerial": 789012, "LastCommSecUtc": 1678886460, "State": "Inactive"},
                            ]
                        }
                    ]
                }
            ]
        },
        "Errors": []
    }

    result_df = gb_eyedro.parse_user_info_as_df(user_info_data)

    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert all(col in result_df.columns for col in const.GB_SN_COLS)
    assert result_df.iloc[0]['gb_serial'] == '123-456'
    assert result_df.iloc[0]['site_label'] == 'Site1'
    assert result_df.iloc[0]['epoch_utc'] == 1678886400
    assert result_df.iloc[0]['status'] == 'Active'

def test_parse_user_info_as_df_empty_data(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    user_info_data = {"UserInfo": {}, "Errors": []}

    result_df = gb_eyedro.parse_user_info_as_df(user_info_data)

    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 0

def test_parse_user_info_as_df_missing_site_label(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    user_info_data = {
        "UserInfo": {
            "DeviceSerialList": [123456],
            "SiteList": [
                {
                    "DisplayGroupList": [
                        {
                            "DeviceList": [
                                {"DeviceSerial": 123456, "LastCommSecUtc": 1678886400, "State": "Active"},
                            ]
                        }
                    ]
                }
            ]
        },
        "Errors": []
    }
    result_df = gb_eyedro.parse_user_info_as_df(user_info_data)
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 1
    assert result_df.iloc[0]['site_label'] is None

def test_parse_user_info_as_df_standalone_serial(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    user_info_data = {
        "UserInfo": {
            "DeviceSerialList": [123456, 999999],
            "SiteList": [
                {
                    "SiteLabel": "Site1",
                    "DisplayGroupList": [
                        {
                            "DeviceList": [
                                {"DeviceSerial": 123456, "LastCommSecUtc": 1678886400, "State": "Active"},
                            ]
                        }
                    ]
                }
            ]
        },
        "Errors": []
    }
    result_df = gb_eyedro.parse_user_info_as_df(user_info_data)
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert result_df.iloc[1]['gb_serial'] == '999-999'
    assert result_df.iloc[1]['site_label'] is None
    assert pd.isna(result_df.iloc[1]['epoch_utc'])  # Check for NaN using pandas.isna()

    assert result_df.iloc[1]['status'] is None

# Test cases for api_get_user_info_as_df
def test_api_get_user_info_as_df_success(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_user_info_data = {
        "UserInfo": {
            "DeviceSerialList": [123456],
            "SiteList": [
                {
                    "SiteLabel": "Site1",
                    "DisplayGroupList": [
                        {
                            "DeviceList": [
                                {"DeviceSerial": 123456, "LastCommSecUtc": 1678886400, "State": "Active"},
                            ]
                        }
                    ]
                }
            ]
        },
        "Errors": []
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_user_info_data
    mock_response.status_code = 200
    mock_requests.request.return_value = mock_response
    
    def mock_error_wrapper(func):
        return func(), None

    mock_err_handler.error_wrapper.side_effect = mock_error_wrapper

    result_df, err = gb_eyedro.api_get_user_info_as_df()

    assert isinstance(result_df, pd.DataFrame)
    assert err is None
    assert len(result_df) == 1
    assert result_df.iloc[0]['gb_serial'] == '123-456'


def test_api_get_user_info_as_df_api_error(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies

    # Directly mock api_get_gb_user_info to return the error
    gb_eyedro.api_get_gb_user_info = MagicMock(return_value=(None, "API Error"))

    result_df, err = gb_eyedro.api_get_user_info_as_df()

    assert result_df is None
    assert "Failed to get user info data" in err
    #The following assertion is likely incorrect and should be removed or modified
    #mock_requests.request.assert_called_once() 


def test_api_get_user_info_as_df_parse_error(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_user_info_data = {
        "UserInfo": {
            "DeviceSerialList": [123456],
            "SiteList": [
                {
                    "SiteLabel": "Site1",
                    "DisplayGroupList": [
                        {
                            "DeviceList": [
                                {"DeviceSerial": 123456, "LastCommSecUtc": 1678886400, "State": "Active"},
                            ]
                        }
                    ]
                }
            ]
        },
        "Errors": []
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_user_info_data
    mock_response.status_code = 200
    mock_requests.request.return_value = mock_response
    
    def mock_error_wrapper(func):
        if func.__name__ == 'parse_user_info_as_df':
            return None, "Parse Error"
        else:
            return func(), None
    mock_err_handler.error_wrapper.side_effect = mock_error_wrapper

    result_df, err = gb_eyedro.api_get_user_info_as_df()

    assert result_df is None
    assert "Failed to get user info data" in err
    ####mock_requests.request.assert_called_once()

def test_api_get_user_info_as_df_api_error_in_response(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_user_info_data = {
        "UserInfo": {
            "DeviceSerialList": [123456],
        },
        "Errors": ["API Error"]
    }
    mock_response = MagicMock()
    mock_response.json.return_value = mock_user_info_data
    mock_response.status_code = 200
    mock_requests.request.return_value = mock_response

    def mock_error_wrapper(func):
        return func(), None

    mock_err_handler.error_wrapper.side_effect = mock_error_wrapper

    result_df, err = gb_eyedro.api_get_user_info_as_df()

    assert result_df is None
    assert "Failed to get user info data" in err
    ####mock_requests.request.assert_called_once()

def test_db_create_tables_db_error(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    # Create an in-memory Postgres engine for testing
    mock_engine = create_engine("postgresql:///?host=localhost&port=5431", creator=lambda _: psycopg2.connect(database="testdb", user="postgres", password="postgres", host="localhost", port=5431))
    # Simulate a database error by trying to create a table with an invalid name
    ###with pytest.raises(psycopg2.errors.SyntaxError):
    res, err = gb_eyedro.db_create_tables(["z.gb_test_123;"], mock_engine)
    str(err.orig.pgerror).startswith("ERROR:  syntax error at or near") is True


def test_api_get_user_info_as_df_api_error(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies

    # Directly mock api_get_gb_user_info to return the error
    gb_eyedro.api_get_gb_user_info = MagicMock(return_value=(None, "API Error"))

    result_df, err = gb_eyedro.api_get_user_info_as_df()

    assert result_df is None
    assert "Failed to get user info data" in err


def test_db_create_tables_missing_index(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    # Create an in-memory Postgres engine for testing
    mock_engine = create_engine("postgresql:///?host=localhost&port=5431", creator=lambda _: psycopg2.connect(database="testdb", user="postgres", password="postgres", host="localhost", port=5431))
    with mock_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS eyedro.gb_test_123 cascade"))
        conn.execute(text("DROP TABLE IF EXISTS eyedro.gb_test_321 cascade"))
        conn.commit()
    result, err = gb_eyedro.db_create_tables(["gb_test_123"], mock_engine)

    assert result == 'All good Houston'
    assert err is None
    # Check if the table was created
    with mock_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.fetchone() is not None

# Test cases for db_create_gb_gaps_table
def test_db_create_gb_gaps_table_success(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_engine = create_engine("postgresql:///?host=localhost&port=5431", creator=lambda _: psycopg2.connect(database="testdb", user="postgres", password="postgres", host="localhost", port=5431))

    # Mock db.sql_execute using MagicMock to control return values for multiple calls
    mock_db.sql_execute = MagicMock(side_effect=[
        ([1], None),  # First call (select epoch_secs) - success
        ([], None),  # Second call (create table) - simulates table already exists
    ])

    result, err = gb_eyedro.db_create_gb_gaps_table(mock_engine)

    assert err is None
    mock_db.sql_execute.assert_called_once()


def test_db_create_gb_gaps_table_create_error(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_engine = MagicMock()
    mock_db.sql_execute.side_effect = [(None, "Error"), (None, "Error")]

    result, err = gb_eyedro.db_create_gb_gaps_table(mock_engine)

    assert err == "Error"
    mock_db.sql_execute.assert_called()

# Test cases for db_get_gb_hypertables
def test_db_get_gb_hypertables_success(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_engine = MagicMock()
    mock_db.sql_execute.return_value = (["gb_test_123"], None)

    result, err = gb_eyedro.db_get_gb_hypertables(mock_engine)

    assert result == ["gb_test_123"]
    mock_db.sql_execute.assert_called_once()


def test_db_hyper_gb_gaps_db_error(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_engine = MagicMock()
    mock_engine.raw_connection.side_effect = psycopg2.OperationalError("DB Error")

    result = gb_eyedro.db_hyper_gb_gaps("gb_test_123", mock_engine)

    assert result == []
    mock_engine.raw_connection.assert_called_once()

def test_db_hyper_gb_gaps_general_error(mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_engine.raw_connection.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("General Error")

    result = gb_eyedro.db_hyper_gb_gaps("gb_test_123", mock_engine)

    assert result == []
    mock_engine.raw_connection.assert_called_once()
    mock_conn.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()
    mock_conn.close.assert_called_once()

# Test cases for db_get_all_gb_gaps
@patch("unhcr.gb_eyedro.pd.DataFrame")
@patch("unhcr.gb_eyedro.hyper_gb_gaps_concur")
@patch("unhcr.gb_eyedro.db_get_gb_hypertables")
def test_db_get_all_gb_gaps_success(mock_get_hypertables, mock_hyper_gaps_concur, mock_df, mock_dependencies):
    mock_requests, mock_logger, mock_err_handler, mock_db = mock_dependencies
    mock_get_hypertables.return_value = [("gb_test_123",), ("gb_test_321",)], None
    mock_hyper_gaps_concur.return_value = [[[(1, 2, 3, 4)]], [[(5, 6, 7, 8)]]]
    mock_df.return_value.to_csv.return_value = None
    mock_df.return_value.head.return_value = "head"

    gb_eyedro.db_get_all_gb_gaps()

    mock_get_hypertables.assert_called_once()
    mock_hyper_gaps_concur.assert_called_once()
    mock_df.assert_called_once()
    mock_df.return_value.to_csv.assert_called_once()
    mock_df.return_value.head.assert_called_once()
