from typing import Literal, LiteralString
import pytest
import pandas as pd
from datetime import datetime
import requests
from sqlalchemy import Engine, create_engine, text, exc
from unhcr import db, constants as const
from unittest.mock import patch

# Arrange
TEST_DB_URL = 'sqlite:///:memory:'  # Use an in-memory SQLite database for testing

@pytest.fixture(scope="module")
def test_engine():
    engine = create_engine(TEST_DB_URL)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS TAKUM_LEONICS_API_RAW (
                DatetimeServer DATETIME PRIMARY KEY,
                BDI1_Power_P1_kW FLOAT,
                external_id INTEGER
            )
        """))
        conn.execute(
            text(
                "INSERT INTO TAKUM_LEONICS_API_RAW (DatetimeServer, BDI1_Power_P1_kW, external_id) VALUES ('2024-07-01 00:00', 10.5, 1)"
            )
        )
        conn.commit()
    return engine


@pytest.fixture(scope="function")
def reset_table(test_engine: Engine):
    with test_engine.connect() as conn:
        conn.execute(text("DELETE FROM TAKUM_LEONICS_API_RAW"))
        conn.execute(
            text(
                "INSERT INTO TAKUM_LEONICS_API_RAW (DatetimeServer, BDI1_Power_P1_kW, external_id) VALUES ('2024-07-01 00:00', 10.5, 1)"
            )
        )
        conn.commit()
    yield


@pytest.mark.parametrize(
    "sql, data, expected_result",
    [
        ("SELECT 1", None, [(1,)]),  # simple_select
        ('SELECT max(DatetimeServer) FROM TAKUM_LEONICS_API_RAW', None, [('2024-07-01 00:00',)]),  # select_max_date
        ("SELECT * FROM TAKUM_LEONICS_API_RAW WHERE external_id = :id", {"id": 1}, [('2024-07-01 00:00', 10.5, 1)]),  # select_with_parameter
    ],
    ids=["simple_select", "select_max_date", "select_with_parameter"]
)
def test_mysql_execute_select(test_engine, sql, data, expected_result, reset_table: None):

    # Act
    res, err = db.mysql_execute(sql, engine=test_engine, data=data)
    # Assert
    assert err is None
    assert res is not None

    # Assert
    assert res.fetchall() == expected_result


def test_mysql_execute_insert(test_engine: Engine, reset_table: None):

    # Act
    res, err = db.mysql_execute(
        "INSERT INTO TAKUM_LEONICS_API_RAW (DatetimeServer, BDI1_Power_P1_kW, external_id) VALUES ('2024-07-02 00:00', 12.5, 2)",
        engine=test_engine,
    )

    # Assert
    assert err is None
    assert res is not None
    assert res.rowcount == 1

    # Assert
    with test_engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM TAKUM_LEONICS_API_RAW"))
        assert result.fetchall() == [('2024-07-01 00:00', 10.5, 1), ('2024-07-02 00:00', 12.5, 2)]


def test_mysql_execute_no_engine():
    with pytest.raises(ValueError) as excinfo:

        # Act
        db.mysql_execute("SELECT 1")

    # Assert
    assert str(excinfo.value) == "Database engine must be provided"


def test_get_mysql_max_date(test_engine: Engine, reset_table: None):

    # Act
    max_date, err = db.get_mysql_max_date(engine=test_engine, table_name = 'TAKUM_LEONICS_API_RAW')

    # Assert
    assert err is None
    assert max_date == datetime(2024, 7, 1, 0, 0)


def test_update_mysql_happy_path(test_engine: Engine, reset_table: None):
    # Arrange
    max_dt = datetime(2024, 7, 1, 0, 0)
    df = pd.DataFrame({'DateTimeServer': [datetime(2024, 7, 2, 0, 0)], 'BDI1_Power_P1_kW': [12.5], 'external_id': [2]})

    # Act
    res, err = db.update_mysql(max_dt, df, "TAKUM_LEONICS_API_RAW")

    # Assert
    assert err is None
    assert res is not None
    assert res.rowcount == 1

    with test_engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM TAKUM_LEONICS_API_RAW"))
        assert result.fetchall() == [('2024-07-01 00:00', 10.5, 1)]##########, ('2024-07-02 00:00', 12.5, 2)]


@patch('unhcr.api_prospect.prospect_get_start_ts')
@patch('unhcr.api_prospect.api_in_prospect')
def test_update_prospect(mock_api_in_prospect, mock_prospect_get_start_ts, test_engine: Engine, reset_table: None):
    # Arrange
    mock_prospect_get_start_ts.return_value = "2024-07-01 00:00"
    mock_api_in_prospect.return_value = requests.Response()
    mock_api_in_prospect.return_value.status_code = 200
    mock_api_in_prospect.return_value._content = str.encode('{"success": true}')

    # Act
    result, error = db.update_prospect(local=True)

    # Assert
    assert result.status_code == 200
    assert error is None


@patch('unhcr.api_prospect.prospect_get_start_ts')
@patch('unhcr.api_prospect.api_in_prospect')
def test_update_prospect_api_fail(mock_api_in_prospect, mock_prospect_get_start_ts, test_engine: Engine, reset_table: None):
    # Arrange
    mock_prospect_get_start_ts.return_value = "2024-07-01 00:00"
    mock_api_in_prospect.return_value = None  # Simulate API failure

    # Act
    result, error = db.update_prospect(local=True)

    # Assert
    assert result is None
    assert error == '"Prospect API failed"'

