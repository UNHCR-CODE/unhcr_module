import pytest
from unhcr.db import (
    set_db_engine, sql_execute, get_db_max_date, update_leonics_db, update_rows,
    update_prospect, backfill_prospect, get_db_session
)
from unhcr import constants as const
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from sqlalchemy import text
import sqlalchemy

# -----
# Arrange
# -----

@pytest.fixture(scope="module")
def db_engine():
    """Fixture to set up the in-memory database engine."""
    engine = sqlalchemy.create_engine('sqlite:///:memory:')
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS TAKUM_LEONICS_API_RAW (
                DateTimeServer DATETIME PRIMARY KEY,
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
    data = {'DateTimeServer': [datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 1, 10, 1, 0)], 'BDI1_Power_P1_kW': [1, 2]}
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
    result, error = sql_execute("SELECT 1", db_engine)
    assert error is None
    assert result.scalar() == 1

def test_sql_execute_insert(db_engine):
    result, error = sql_execute("INSERT INTO TAKUM_LEONICS_API_RAW (DateTimeServer, BDI1_Power_P1_kW) VALUES ('2024-01-01 10:00:00', 1)", db_engine)
    assert error is None
    assert result.rowcount == 1

def test_sql_execute_error(db_engine):
    result, error = sql_execute("INVALID SQL", db_engine)
    assert result is False
    assert error is not None

# -----
# Test cases for get_db_max_date
# -----

def test_get_db_max_date(db_engine):
    # Insert a dummy row for testing
    sql_execute("INSERT INTO TAKUM_LEONICS_API_RAW (DateTimeServer, BDI1_Power_P1_kW) VALUES ('2024-08-01 10:00', 1)", db_engine)

    max_date, error = get_db_max_date(db_engine)
    assert error is None
    assert max_date == datetime(2024, 8, 1, 10, 0)

def test_get_db_session():
    with get_db_session('') as session:
        assert session is not None
# -----
# Test cases for update_leonics_db and update_rows
# -----

def test_update_leonics_db_and_update_rows(db_engine, dummy_df):
    max_dt = datetime(2023, 12, 31)
    result, error = update_leonics_db(max_dt, dummy_df, "TAKUM_LEONICS_API_RAW")
    assert result is not None
    assert error is None

    # Check if rows were inserted
    res, _ = sql_execute("SELECT COUNT(*) FROM TAKUM_LEONICS_API_RAW", db_engine)
    assert res.scalar() == 2



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

