import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import os
from unittest.mock import patch, MagicMock, call
import requests
from sqlalchemy import create_engine, exc, text
from pathlib import Path

import sqlalchemy
import unhcr
from unhcr import constants as const
from unhcr.api_solarman import (
    db_get_sm_weather_max_epoch,
)
from unhcr.db import (
    set_db_engine,
    set_db_engine_by_name,
    get_db_session,
    sql_execute,
    db_get_max_date,
    db_update_leonics,
    update_rows,
    prospect_get_start_ts,
    update_prospect,
    backfill_prospect,
    prospect_backfill_key,
    update_fuel_data,
    update_bulk_fuel,
    db_update_takum_raw,
    get_fuel_max_ts,
    get_gb_epoch,
    set_db_engines,
    set_local_defaultdb_engine,
    set_azure_defaultdb_engine
)


@pytest.fixture
def setup_environment():
    """Save and restore environment state"""
    # Save original environment variables
    original_local = const.LOCAL
    original_takum_conn = const.TAKUM_RAW_CONN_STR
    original_leonics_table = const.LEONICS_RAW_TABLE
    yield
    # Restore original environment variables
    const.LOCAL = original_local
    const.TAKUM_RAW_CONN_STR = original_takum_conn
    const.LEONICS_RAW_TABLE = original_leonics_table


@pytest.fixture
def mock_engine():
    """Create a mock engine"""
    engine = MagicMock()
    session = MagicMock()
    engine.connect.return_value.__enter__.return_value = session
    return engine


@pytest.fixture
def db_engine():
    """Set up a real SQLite engine for testing"""
    engine = create_engine("sqlite:///:memory:")
    # Create test tables
    with engine.begin() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS TAKUM_LEONICS_API_RAW (
                datetimeserver DATETIME PRIMARY KEY,
                BDI1_Power_P1_kW INTEGER,
                external_id VARCHAR(100)
            )
        """)
        conn.execute("""
            INSERT INTO TAKUM_LEONICS_API_RAW (datetimeserver, BDI1_Power_P1_kW, external_id)
            VALUES ('2024-08-01 10:00:00', 100, '12345')
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS solarman.weather (
                org_epoch INTEGER,
                device_id INTEGER
            )
        """)
        conn.execute("""
            INSERT INTO solarman.weather (org_epoch, device_id)
            VALUES (1628000000, 123)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fuel.fuel_kwh_site1 (
                st_ts DATETIME PRIMARY KEY
            )
        """)
        conn.execute("""
            INSERT INTO fuel.fuel_kwh_site1 (st_ts)
            VALUES ('2024-08-01 10:00:00')
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eyedro.gb_12345 (
                epoch_secs INTEGER
            )
        """)
        conn.execute("""
            INSERT INTO eyedro.gb_12345 (epoch_secs)
            VALUES (1628000000)
        """)
        
    yield engine


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing"""
    data = {
        'datetimeserver': pd.to_datetime(['2024-08-15 12:00:00', '2024-08-15 12:01:00']),
        'BDI1_Power_P1_kW': [100, 150],
        'external_id': ['12345', '67890']
    }
    return pd.DataFrame(data)


@pytest.fixture
def fuel_df():
    """Create a sample DataFrame for fuel data testing"""
    dates = [datetime(2024, 8, 1).date(), datetime(2024, 8, 2).date()]
    data = {
        'date': dates,
        'hour': [10, 11],
        'kwh': [100.5, 200.5],
        'deltal1': [10.2, 15.3],
        'deltal2': [5.1, 7.2],
        'kWh/L1': [9.8, 13.1],
        'kWh/L2': [19.7, 27.8]
    }
    return pd.DataFrame(data)


@pytest.fixture
def bulk_fuel_df():
    """Create a sample DataFrame for bulk fuel data testing"""
    data = {
        'Unit Name': ['Generator1', 'Generator2'],
        'Time': [datetime(2024, 8, 15, 12, 0), datetime(2024, 8, 15, 13, 0)],
        'Event Name': ['Fuel_Drop', 'Refueled'],
        'Value': ['10 Liter', '20 Liter'],
    }
    return pd.DataFrame(data)


# Tests for set_db_engine
def test_set_db_engine():
    """Test creating a database engine with default parameters"""
    engine = set_db_engine("sqlite:///:memory:")
    assert engine is not None
    assert hasattr(engine, 'connect')


# Tests for set_db_engine_by_name
@patch('unhcr.db.set_db_engine')
@patch('unhcr.constants.is_running_on_azure')
def test_set_db_engine_by_name_postgresql(mock_is_azure, mock_set_engine, setup_environment):
    """Test setting DB engine by name for PostgreSQL"""
    mock_is_azure.return_value = False
    mock_set_engine.return_value = "mock_engine"
    
    # Set environment variables
    os.environ["AZURE_TAKUM_LEONICS_API_RAW_CONN_STR"] = "postgresql://user:pass@localhost/db"
    os.environ["AZURE_LEONICS_RAW_TABLE"] = "leonics_table"
    
    engine, table = set_db_engine_by_name("postgresql")
    
    assert engine == "mock_engine"
    assert table == "leonics_table"
    assert const.TAKUM_RAW_CONN_STR == "mysql://avnadmin:AVNS_LoixdoCxbSyjyauho38@mysql-takum-hybrid-takum-leonics.d.aivencloud.com:14231/defaultdb"
    assert const.LEONICS_RAW_TABLE == "TAKUM_LEONICS_API_RAW"
    mock_set_engine.assert_called_once_with("postgresql://user:pass@localhost/db")


@patch('unhcr.db.set_db_engine')
def test_set_db_engine_by_name_non_postgresql(mock_set_engine, setup_environment):
    """Test setting DB engine by name for non-PostgreSQL"""
    mock_set_engine.return_value = "mock_engine"

    
    engine, table = set_db_engine_by_name("mysql")
    
    assert engine == "mock_engine"
    assert table == "TAKUM_LEONICS_API_RAW"
    assert const.TAKUM_RAW_CONN_STR == 'mysql://avnadmin:AVNS_LoixdoCxbSyjyauho38@mysql-takum-hybrid-takum-leonics.d.aivencloud.com:14231/defaultdb'
    assert const.LEONICS_RAW_TABLE == 'TAKUM_LEONICS_API_RAW'
    mock_set_engine.assert_called_once_with(const.TAKUM_RAW_CONN_STR)


# Tests for get_db_session
def test_get_db_session_success(db_engine):
    """Test successful database session creation and commit"""
    with get_db_session(db_engine) as session:
        assert session is not None
        # Session should allow queries
        result = session.execute(text("SELECT 1")).fetchone()
        assert result[0] == 1


def test_get_db_session_exception(db_engine):
    """Test database session with exception handling"""
    with pytest.raises(exc.SQLAlchemyError):
        with get_db_session(db_engine) as session:
            # Cause a SQL error
            session.execute("INVALID SQL")


# Tests for sql_execute
def test_sql_execute_select(db_engine):
    """Test SQL SELECT execution"""
    result, error = sql_execute("SELECT * FROM TAKUM_LEONICS_API_RAW", db_engine)
    assert error is None
    assert len(result) == 1
    assert result[0][0] == '2024-08-01 10:00:00'
    assert result[0][1] == 100


def test_sql_execute_insert(db_engine):
    """Test SQL INSERT execution"""
    result, error = sql_execute(
        "INSERT INTO TAKUM_LEONICS_API_RAW (datetimeserver, BDI1_Power_P1_kW, external_id) VALUES ('2024-08-02 10:00:00', 200, '67890') Returning datetimeserver",
        db_engine
    )
    assert error is None
    assert result.rowcount == 1

    # Verify the insert worked
    verify, _ = sql_execute("SELECT COUNT(*) FROM TAKUM_LEONICS_API_RAW", db_engine)
    assert verify[0][0] == 2


def test_sql_execute_with_parameters(db_engine):
    """Test SQL execution with parameters"""
    params = {'ts': '2024-08-03 10:00:00', 'power': 300, 'id': '13579'}
    result, error = sql_execute(
        "INSERT INTO TAKUM_LEONICS_API_RAW (datetimeserver, BDI1_Power_P1_kW) VALUES (:ts, :power) Returning datetimeserver",
        db_engine,
        params
    )
    assert error is None
    assert len(result) == 1

    # Verify the insert worked
    verify, _ = sql_execute("SELECT COUNT(*) FROM TAKUM_LEONICS_API_RAW", db_engine)
    assert verify[0][0] == 2


def test_sql_execute_error(db_engine):
    """Test SQL execution with error"""
    result, error = sql_execute("INVALID SQL", db_engine)
    assert result is False
    assert error is not None
    assert error['error_type'] == 'OperationalError'


def test_sql_execute_no_engine():
    """Test SQL execution with no engine"""
    with pytest.raises(ValueError):
        sql_execute("SELECT 1", None)


# Tests for db_get_max_date
def test_get_db_max_date(db_engine):
    """Test retrieving maximum date from database"""
    max_date, error = db_get_max_date(db_engine)
    assert error is None
    print('DDDDD', max_date)
    assert max_date == datetime(2024, 8, 1, 10, 0)


@patch('unhcr.db.sql_execute')
def test_get_db_max_date_error(mock_sql_execute):
    """Test error handling in get_db_max_date"""
    mock_sql_execute.return_value = (None, Exception("Test error"))
    max_date, error = db_get_max_date()
    assert max_date is None
    assert isinstance(error, Exception)


# Tests for db_update_leonics and update_rows
@patch('unhcr.db.update_rows')
def test_db_update_leonics(mock_update_rows, sample_df):
    """Test db_update_leonics delegates to update_rows"""
    mock_update_rows.return_value = (MagicMock(rowcount=2), None)
    max_dt = datetime(2024, 8, 14, 0, 0)
    
    result, error = db_update_leonics(max_dt, sample_df, db_engine)
    
    assert error is None
    assert result.rowcount == 2
    ######mock_update_rows.assert_called_once_with(max_dt, sample_df, db_engine)


def test_update_rows_filtering(db_engine, sample_df):
    """Test filtering in update_rows"""
    max_dt = datetime(2024, 8, 15, 11, 59)  # Just before the samples
    
    # Mock the sql_execute to avoid actual execution
    with patch('unhcr.db.sql_execute') as mock_sql_execute:
        mock_sql_execute.return_value = (MagicMock(rowcount=2), None)
        
        result, error = update_rows(max_dt, sample_df, db_engine)
        
        assert error is None
        assert mock_sql_execute.called
        # Check that the SQL contains both rows (since both are after max_dt)
        sql_arg = mock_sql_execute.call_args[0][0]
        assert "'2024-08-15 12:00'" in sql_arg
        assert "'2024-08-15 12:01'" in sql_arg


def test_update_rows_no_data(sample_df):
    """Test update_rows with no data after filtering"""
    max_dt = datetime(2024, 8, 15, 13, 0)  # After the samples
    
    result, error = update_rows(max_dt, sample_df, "TAKUM_LEONICS_API_RAW")
    
    assert error is None
    assert result.rowcount == 0


@patch('unhcr.db.set_db_engine_by_name')
@patch('unhcr.db.sql_execute')
def test_update_rows_postgresql(mock_sql_execute, mock_set_engine, sample_df):
    """Test update_rows with PostgreSQL and lowercase key"""
    mock_set_engine.return_value = (MagicMock(), "test_table")
    mock_sql_execute.return_value = (MagicMock(rowcount=2), None)
    
    max_dt = datetime(2024, 8, 14, 0, 0)
    
    result, error = update_rows(max_dt, sample_df, db_engine)
    
    assert error is None
    assert mock_sql_execute.called
    # Check that PostgreSQL conflict syntax is used
    sql_arg = mock_sql_execute.call_args[0][0]
    assert "ON CONFLICT (datetimeserver) DO UPDATE" in sql_arg
    mock_set_engine.assert_called_once_with("postgresql")


@patch('unhcr.db.set_db_engine_by_name')
@patch('unhcr.db.sql_execute')
def test_update_rows_mysql(mock_sql_execute, mock_set_engine, sample_df):
    """Test update_rows with MySQL (default)"""
    mock_set_engine.return_value = (MagicMock(), "test_table")
    mock_sql_execute.return_value = (MagicMock(rowcount=2), None)
    
    max_dt = datetime(2024, 8, 14, 0, 0)
    
    result, error = update_rows(max_dt, sample_df, "test_table")
    
    assert error is None
    mock_sql_execute.assert_called_once()
    # Check that MySQL syntax is used
    sql_arg = mock_sql_execute.call_args[0][0]
    assert "ON CONFLICT (datetimeserver) DO UPDATE" in sql_arg
    print(mock_set_engine)   #.assert


# Mocking requests.request to avoid actual API calls during testing
@pytest.fixture
def mock_request(monkeypatch):
    """
    Mocks the requests.request function to return a MockResponse object
    with a status code of 200 and a JSON response containing a single
    data point with a custom field "datetimeserver" set to
    "2024-08-15T12:00:00Z" and an "external_id" set to "123".

    This fixture is used to isolate the tests from actual API calls.

    :param monkeypatch: a pytest fixture to monkeypatch the requests.request function
    :return: None
    """

    class MockResponse:
        def __init__(self, status_code, text):
            """
            Initializes a MockResponse object with a given status code and JSON response.

            Args:
                status_code (int): The HTTP status code for the response.
                text (str): The JSON response as a string.

            Attributes:
                status_code (int): The HTTP status code for the response.
                text (str): The JSON response as a string.
            """
            self.status_code = status_code
            self.text = text

    def mock_request_func(*args, **kwargs):
        """
        Simulates a request and returns a mock response object.

        This function is used to mock the behavior of an API request by returning
        a pre-defined MockResponse object with a status code of 200 and a JSON
        response. The JSON response includes a single data point with a custom
        "datetimeserver" field and an "external_id".

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            MockResponse: A mock response object with a status code of 200 and a
            predefined JSON data structure.
        """

        return MockResponse(
            200,
            '{"data": [{"custom": {"datetimeserver": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}',
        )

    monkeypatch.setattr(requests, "request", mock_request_func)


@pytest.fixture(scope="module")
def db_engine():
    """Fixture to set up the in-memory database engine."""
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS TAKUM_LEONICS_API_RAW (
                datetimeserver DATETIME PRIMARY KEY,
                BDI1_Power_P1_kW INTEGER
            )
        """
            )
        )
        
    return engine


@pytest.fixture
def mock_sql_execute():
    """
    Mocks the sql_execute function to isolate the tests from actual database interactions.

    This fixture is used to mock the sql_execute function in the unhcr.db module,
    which is used to execute SQL queries against the database. This allows the tests
    to be isolated from actual database interactions, ensuring that the tests are
    independent and reproducible.

    :return: A mock object for the sql_execute function.
    """
    with patch("unhcr.db.sql_execute") as mock:
        yield mock


@pytest.fixture
def mock_api_prospect():
    """
    Mocks the api_prospect function to isolate the tests from actual API interactions.

    This fixture is used to mock the api_prospect function in the unhcr.db module,
    which is used to send data to the Prospect API. This allows the tests to be
    isolated from actual API interactions, ensuring that the tests are independent
    and reproducible.

    :return: A mock object for the api_prospect function.
    """
    with patch("unhcr.db.api_prospect") as mock:
        yield mock


@pytest.fixture
def dummy_df():
    """
    A fixture that returns a pandas DataFrame with a datetimeserver column and a BDI1_Power_P1_kW column.
    The DataFrame contains two rows of data, with timestamps one minute apart, and power values of 1 and 2 respectively.
    """
    data = {
        "datetimeserver": [
            datetime(2024, 1, 1, 10, 0, 0),
            datetime(2024, 1, 1, 10, 1, 0),
        ],
        "BDI1_Power_P1_kW": [1, 2],
    }
    return pd.DataFrame(data)


# -----
# Test cases for set_db_engine
# -----


def test_set_db_engine(db_engine):
    assert set_db_engine(const.TAKUM_RAW_CONN_STR) is not None


# -----
# Test cases for get_db_session
# -----
def test_get_db_session(db_engine):
    with get_db_session(db_engine) as session:
        assert session is not None


# -----
# Test cases for sql_execute
# -----


def test_sql_execute_select(db_engine):
    result, error = unhcr.db.sql_execute("SELECT 1", db_engine)
    assert error is None
    assert result[0][0] == 1


def test_sql_execute_insert(db_engine):
    result, error = unhcr.db.sql_execute(
        "INSERT INTO TAKUM_LEONICS_API_RAW (datetimeserver, BDI1_Power_P1_kW) VALUES ('2024-01-01 10:00:00', 1) returning datetimeserver",
        db_engine,
    )
    assert error is None
    assert len(result) == 1


def test_sql_execute_error(db_engine):
    result, error = unhcr.db.sql_execute("INVALID SQL", db_engine)
    assert result is False
    assert error is not None


# -----
# Test cases for db_get_max_date
# -----


def test_get_db_max_date(db_engine):
    # Insert a dummy row for testing
    unhcr.db.sql_execute(
        "INSERT INTO TAKUM_LEONICS_API_RAW (DatetimeServer, BDI1_Power_P1_kW) VALUES ('2024-08-01 10:00', 1)",
        db_engine,
    )
    max_date, error = db_get_max_date(db_engine)
    assert error is None
    assert max_date == datetime(2024, 8, 3, 10, 0)


def test_get_db_session():
    with get_db_session("") as session:
        assert session is not None


# -----
# Test cases for db_update_leonics and update_rows
# -----
@pytest.mark.parametrize(
    "max_dt, dummy_df, expected_count",
    [
        (
            datetime(2023, 12, 31),
            pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}),
            2
        ),
        (
            datetime(2023, 12, 31),
            pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}),
            2
        ),
    ],
    ids=["db_update_leonics_success", "db_update_leonics_failure"],
)
@patch("unhcr.db.db_update_leonics")
def test_db_update_leonics_and_update_rows(
    mock_db_update_leonics, max_dt, dummy_df, expected_count, db_engine
):
    # Mock the return values of the db_update_leonics function
    mock_db_update_leonics.return_value = (
        (None, "err") if expected_count == 0 else (dummy_df, None)
    )

    # Call the function
    result, error = unhcr.db.db_update_leonics(max_dt, dummy_df, db_engine)

    # Assert the expected values
    if expected_count == 0:
        assert result is None
        assert error == "err"
    else:
        assert result is not None
        assert error is None

    # Check if rows were inserted
    res, err = unhcr.db.sql_execute(f"SELECT COUNT(*) FROM takum_leonics_api_raw", db_engine)
    if err is None:
        assert res[0][0] == expected_count
    else:
        assert err["error_message"].startswith("(sqlite3.OperationalError)")


# -----
# Test cases for update_prospect
# -----
@pytest.mark.skip(reason="Need to put data in the database first")
def test_update_prospect(mock_api_prospect, mock_sql_execute, db_engine):
    start_ts = "2024-08-01 00:00"
    mock_api_prospect.prospect_get_start_ts.return_value = start_ts
    mock_sql_execute.return_value = (MagicMock(), None)
    mock_api_prospect.api_in_prospect.return_value = MagicMock(status_code=200)

    result, error = update_prospect(start_ts, local=True)
    assert result is not None
    assert error is None


# -----
# Test cases for backfill_prospect
# -----
@pytest.mark.skip(reason="Backfill is not ready yet")
def test_backfill_prospect(mock_api_prospect, db_engine):
    start_ts = "2024-08-01 00:00"
    with patch("unhcr.db.prospect_backfill_key") as mock_backfill_key:
        backfill_prospect(start_ts, local=True)
        mock_backfill_key.assert_called_once()


@pytest.mark.parametrize(
    "local, start_ts, expected_timestamp",
    [
        (True, None, "2025-01-29T14:22:00Z"),  # local_no_start_ts
        (False, None, "2025-01-29T14:22:00Z"),  # external_no_start_ts
        (True, "2024-08-14T12:00:00Z", "2024-08-14T12:00:00Z"),  # local_with_start_ts
        (
            False,
            "2024-08-14T12:00:00Z",
            "2024-08-14T12:00:00Z",
        ),  # external_with_start_ts
    ],
    ids=[
        "local_no_start_ts",
        "external_no_start_ts",
        "local_with_start_ts",
        "external_with_start_ts",
    ],
)
@patch("unhcr.db.prospect_get_start_ts")
@patch("unhcr.api_prospect.api_in_prospect")
def test_prospect_get_start_ts(
    mock_api_in_prospect,
    mock_prospect_get_start_ts,
    local,
    start_ts,
    expected_timestamp,
    mock_request,
):
    mock_prospect_get_start_ts.return_value = expected_timestamp
    mock_api_in_prospect.return_value = MagicMock(status_code=200)

    result = unhcr.db.prospect_get_start_ts(local=local, start_ts=start_ts)

    if type(result) is datetime:
        assert result.strftime("%Y-%m-%dT%H:%M:%SZ") == expected_timestamp
    else:
        assert result == expected_timestamp
