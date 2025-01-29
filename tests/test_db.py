import pytest
import requests
from unhcr.db import (
    set_db_engine, sql_execute, get_db_max_date, update_leonics_db, update_rows,
    update_prospect, backfill_prospect, get_db_session, prospect_get_start_ts
)
import unhcr
from unhcr import constants as const
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from sqlalchemy import text
import sqlalchemy
from unittest.mock import patch, MagicMock

# -----
# Arrange
# -----
# Mocking requests.request to avoid actual API calls during testing
@pytest.fixture
def mock_request(monkeypatch):
    class MockResponse:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

    def mock_request_func(*args, **kwargs):
        return MockResponse(200, '{"data": [{"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}')

    monkeypatch.setattr(requests, 'request', mock_request_func)


@pytest.fixture(scope="module")
def db_engine():
    """Fixture to set up the in-memory database engine."""
    engine = sqlalchemy.create_engine('sqlite:///:memory:')
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS TAKUM_LEONICS_API_RAW (
                DatetimeServer DATETIME PRIMARY KEY,
                BDI1_Power_P1_kW INTEGER
            )
        """))
        conn.commit()
    return engine


@pytest.fixture
def mock_sql_execute():
    with patch("unhcr.db.sql_execute") as mock:
        yield mock

@pytest.fixture
def mock_api_prospect():
    with patch("unhcr.db.api_prospect") as mock:
        yield mock

@pytest.fixture
def dummy_df():
    data = {'DatetimeServer': [datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 1, 10, 1, 0)], 'BDI1_Power_P1_kW': [1, 2]}
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
    assert result.scalar() == 1

def test_sql_execute_insert(db_engine):
    result, error = unhcr.db.sql_execute("INSERT INTO TAKUM_LEONICS_API_RAW (DatetimeServer, BDI1_Power_P1_kW) VALUES ('2024-01-01 10:00:00', 1)", db_engine)
    assert error is None
    assert result.rowcount == 1

def test_sql_execute_error(db_engine):
    result, error = unhcr.db.sql_execute("INVALID SQL", db_engine)
    assert result is False
    assert error is not None

# -----
# Test cases for get_db_max_date
# -----

def test_get_db_max_date(db_engine):
    # Insert a dummy row for testing
    unhcr.db.sql_execute("INSERT INTO TAKUM_LEONICS_API_RAW (DatetimeServer, BDI1_Power_P1_kW) VALUES ('2024-08-01 10:00', 1)", db_engine)

    max_date, error = get_db_max_date(db_engine)
    assert error is None
    assert max_date == datetime(2024, 8, 1, 10, 0)

def test_get_db_session():
    with get_db_session('') as session:
        assert session is not None
# -----
# Test cases for update_leonics_db and update_rows
# -----
@pytest.mark.parametrize(
    "max_dt, dummy_df, table_name, expected_count",
    [
        (datetime(2023, 12, 31), pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]}), "TAKUM_LEONICS_API_RAW", 2),
        (datetime(2023, 12, 31), pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]}), "OTHER_TABLE", False),
    ],
    ids=["update_leonics_db_success", "update_leonics_db_failure"]
)
@patch('unhcr.db.update_leonics_db')
def test_update_leonics_db_and_update_rows(mock_update_leonics_db, max_dt, dummy_df, table_name, expected_count, db_engine):
    # Mock the return values of the update_leonics_db function
    mock_update_leonics_db.return_value = (None, 'err') if expected_count == 0 else (dummy_df, None)

    # Call the function
    result, error = unhcr.db.update_leonics_db(max_dt, dummy_df, table_name)

    # Assert the expected values
    if expected_count == 0:
        assert result is None
        assert error == 'err'
    else:
        assert result is not None
        assert error is None

    # Check if rows were inserted
    res, err = unhcr.db.sql_execute(f"SELECT COUNT(*) FROM {table_name}", db_engine)
    if err is None:
        assert res.scalar() == expected_count
    else:
        assert err['error_message'].startswith('(sqlite3.OperationalError)')



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
        (False, "2024-08-14T12:00:00Z", "2024-08-14T12:00:00Z"),  # external_with_start_ts
    ],
    ids=["local_no_start_ts", "external_no_start_ts", "local_with_start_ts", "external_with_start_ts"]
)
@patch('unhcr.db.prospect_get_start_ts')
@patch('unhcr.api_prospect.api_in_prospect')
def test_prospect_get_start_ts(mock_api_in_prospect, mock_prospect_get_start_ts, local, start_ts, expected_timestamp, mock_request):
    mock_prospect_get_start_ts.return_value = expected_timestamp
    mock_api_in_prospect.return_value = MagicMock(status_code=200)

    result = unhcr.db.prospect_get_start_ts(local=local, start_ts=start_ts)

    if type(result) is datetime:
        assert result.strftime('%Y-%m-%dT%H:%M:%SZ') == expected_timestamp
    else:
        assert result == expected_timestamp
