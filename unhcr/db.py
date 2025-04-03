"""
Overview
    This script db.py manages database interactions for energy monitoring data. It connects to a DB database using
    SQLAlchemy and interacts with a Prospect API. The primary functions handle updating both the database and the API
    with new data, managing duplicates, and logging errors. Connection pooling is used for database efficiency.
    A critical vulnerability exists: the update_rows function is susceptible to SQL injection.

Key Components
sql_execute(sql, engine=default_engine, data=None):
    Executes SQL queries against the DB database. Handles session management and utilizes connection pooling.
    Important: Vulnerable to SQL injection in update_rows due to string formatting.

update_leonics_db(max_dt, df, table_name, key='DatetimeServer'):
    Orchestrates the database update process. Retrieves the latest timestamp from the database, filters new data from the
    input DataFrame, and inserts the new data into the specified table. Includes error handling.

update_rows(max_dt, df, table_name, key="DateTimeServer"):
    Inserts new data into the DB database. Filters the DataFrame, formats data, and performs a bulk INSERT with an
    ON DUPLICATE KEY UPDATE clause. The ON DUPLICATE KEY UPDATE clause is excessively long and should be refactored.

update_prospect(start_ts=None, local=True, table_name=const.LEONICS_RAW_TABLE):
    Manages updates to the Prospect API. Retrieves the latest timestamp from Prospect, queries the database for newer records,
    and sends them to the API. This function could benefit from being broken down into smaller, more manageable functions.

set_db_engine(connection_string):
    Creates and returns a SQLAlchemy engine with connection pooling for efficient database access. Pool parameters are
    configurable via environment variables.

set_db_engine_by_name(ename):
    Sets the database engine by name and configures connection strings accordingly.

update_fuel_data(conn_str, merged_hourly_sums, table, site):
    Updates the fuel data in the database.
    This function takes in the merged hourly sums DataFrame, connection string, table name, and site name.
    It constructs the SQL query string by replacing 'TABLE' with the table name and adds the VALUES clause.
    The SQL query is then executed using the connection string and the updated row count is printed.
    Finally, the merged hourly sums DataFrame is saved to a CSV file named 'mhs_<site>.csv' and the function returns None, None, None.

update_bulk_fuel(conn_str, df, df1):
    Updates the bulk fuel data in the database.
    This function takes in the connection string, the first DataFrame, and the second DataFrame.
    It constructs the SQL query string by replacing 'TABLE' with the table name and adds the VALUES clause.
    The SQL query is then executed using the connection string and the updated row count is printed.
    Finally, the first DataFrame is saved to a CSV file named 'mhs_<site>.csv' and the function returns None, None, None.

update_takum_raw_db(token, start_ts):
    Updates the Takum raw data in the database.
    This function takes in the API token and the start timestamp.
    It queries the Takum API for the raw data, filters it, and updates the database.
    The function returns None, None, None.

WIP backfill_prospect(start_ts=None, local=True) & prospect_backfill_key(func, start_ts, local, table_name):
    These functions appear to be related to backfilling data into the Prospect API but are marked as "WIP"
    (work in progress) and are not fully functional.
"""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import logging
import math
import os
from types import SimpleNamespace
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, exc, orm, text

from unhcr import constants as const
from unhcr import utils
from unhcr import api_prospect
from unhcr import api_leonics

mods = const.import_local_libs(
    mods=[
        ["constants", "const"],
        ["utils", "utils"],
        ["api_leonics", "api_leonics"],
        ["api_prospect", "api_prospect"],
    ]
)
logger, *rest = mods
if const.LOCAL:  # testing with local python files
    logger, const, utils, api_leonics, api_prospect = mods


default_engine = None
prospect_engine = None


# Create a connection pool
def set_db_engine(connection_string):
    global default_engine
    global prospect_engine
    """
    Create a SQLAlchemy engine with configurable connection pooling.

    Pool parameters can be configured via environment variables:

    - SQLALCHEMY_POOL_SIZE: Maximum number of connections in the pool (default: 5)
    - SQLALCHEMY_POOL_TIMEOUT: Seconds to wait before giving up on getting a connection (default: 30)
    - SQLALCHEMY_POOL_RECYCLE: Connection recycle time in seconds (default: 3600)
    - SQLALCHEMY_MAX_OVERFLOW: Number of connections that can be created beyond pool_size (default: 10)

    :param connection_string: A DB connection string, e.g. DB://user:pass@host/db
    :return: A SQLAlchemy engine
    """
    # if iso:
    #     isolation_level="AUTOCOMMIT"
    # else:
    #     isolation_level="SERIALIZABLE"
    return create_engine(
        connection_string,
        pool_size=const.SQLALCHEMY_POOL_SIZE,
        pool_timeout=const.SQLALCHEMY_POOL_TIMEOUT,
        pool_recycle=const.SQLALCHEMY_POOL_RECYCLE,
        max_overflow=const.SQLALCHEMY_MAX_OVERFLOW,
        # isolation_level=isolation_level
    )


def set_db_engine_by_name(ename, local=False):
    """
    Sets the database engine by name and configures connection strings accordingly.

    This function checks if the current default engine's name matches the given engine
    name (`ename`). If not, it updates the connection strings for the `TAKUM_RAW_CONN_STR`
    and `LEONICS_RAW_TABLE` constants based on the specified engine name.

    If `ename` is 'postgresql', it sets the connection strings to the Azure environment
    variables. Otherwise, it uses the Aiven connection strings.

    :param ename: The name of the database engine (e.g., 'postgresql').
    :return: The newly created SQLAlchemy engine.
    """

    ##if default_engine.engine.name != ename:
    if ename == "postgresql":
        const.TAKUM_RAW_CONN_STR = os.getenv(
            "AZURE_TAKUM_LEONICS_API_RAW_CONN_STR", "xxxxxx"
        )
        const.LEONICS_RAW_TABLE = os.getenv("AZURE_LEONICS_RAW_TABLE", "pppppp")
        if const.is_running_on_azure() or local:
            const.TAKUM_RAW_CONN_STR = const.TAKUM_RAW_CONN_STR.replace(
                const.AZURE_URL, "localhost"
            )
    else:
        const.TAKUM_RAW_CONN_STR = os.getenv(
            "AIVEN_TAKUM_LEONICS_API_RAW_CONN_STR", "zzzzz"
        )
        const.LEONICS_RAW_TABLE = os.getenv("LEONICS_RAW_TABLE", "qqqqq")
    return set_db_engine(const.TAKUM_RAW_CONN_STR), const.LEONICS_RAW_TABLE


# @contextmanager
# def get_db_session(engine):
#     """Provide a transactional scope around a series of operations."""
#     Session = orm.sessionmaker(bind=engine)
#     session = Session()
#     try:
#         yield session
#         session.commit()
#     except exc.SQLAlchemyError:
#         session.rollback()
#         raise
#     finally:
#         session.close()

from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import exc
from contextlib import contextmanager
import logging


@contextmanager
def get_db_session(eng):
    # Create a scoped session for thread-local session management
    Session = scoped_session(sessionmaker(bind=eng))
    session = Session()
    try:
        yield session
        session.commit()
    except exc.SQLAlchemyError as db_error:
        session.rollback()
        error_msg = f"Database update failed: {str(db_error)}"
        logger.error(error_msg)
        raise
    except ValueError as val_error:
        session.rollback()
        error_msg = f"Data validation error: {str(val_error)}"
        logger.error(error_msg)
        raise
    except Exception as e:
        session.rollback()
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        raise
    finally:
        session.close()
        Session.remove()  # Remove the session from the scoped session registry


def sql_execute(sql, engine=default_engine, data=None):
    # If no engine is provided, raise an error or use a default
    """
    Execute a SQL query against the DB database using a provided engine.

    :param str sql: A SQL query string
    :param engine: A SQLAlchemy engine
    :param data: Optional data to be passed as parameters to the query
    :return: A tuple containing the result of the query and None on success, otherwise a tuple containing False and an error dictionary
    """
    if engine is None:
        raise ValueError("Database engine must be provided")

    # Assuming get_db_session is defined correctly elsewhere
    #####with get_db_session(engine) as session:
    Session = sessionmaker(bind=engine)
    session = Session()
    ######session.execute(text("SET search_path TO solarman;"))
    try:
        # Use SQLAlchemy's execute method
        result = session.execute(text(sql), params=data)

        # If it's a SELECT query, fetch results
        # if sql.strip().upper().startswith("SELECT"):
        #     res = result.fetchall()
        #     return res, None

        # # For INSERT, UPDATE, DELETE return the result
        res = result.fetchall()
        session.commit()
        return res, None

    except exc.SQLAlchemyError as db_error:
        session.rollback()
        error_msg = f"Database update failed: {str(db_error)}"
        logger.error(error_msg)
        return False, {
            "error_type": type(db_error).__name__,
            "error_message": str(db_error),
            "sql": sql,
        }

    except ValueError as val_error:
        session.rollback()
        error_msg = f"Data validation error: {str(val_error)}"
        logger.error(error_msg)
        return False, {
            "error_type": "ValidationError",
            "error_message": str(val_error),
            "sql": sql,
        }

    except Exception as e:
        session.rollback()
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return False, {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "sql": sql,
        }
    finally:
        session.close()


default_engine = set_db_engine(const.TAKUM_RAW_CONN_STR)


def get_db_max_date(engine=default_engine):
    """
    Retrieves the latest timestamp from the database. If the database is empty or an error occurs,
    returns None and the error.
    """

    try:
        dt, err = sql_execute(
            f"select max(DatetimeServer) FROM TAKUM_LEONICS_API_RAW", engine
        )
        assert err is None
        val = dt[0][0]
        if len(val) > 18:
            val = dt[0][0][:-3]
        return datetime.strptime(val, "%Y-%m-%d %H:%M"), None
    except Exception as e:
        logger.error(f"Can not get DB max timestanp   {e}")
        return None, e


def update_leonics_db(max_dt, df, eng):
    """
    Updates the specified DB table with new data from a DataFrame.

    This function processes and inserts new data into a DB table by filtering
    the input DataFrame for records with 'DatetimeServer' greater than the specified
    max_dt. The filtered data is formatted and used to generate a SQL bulk INSERT
    statement, which is then executed.

    Parameters
    ----------
    max_dt : datetime
        The maximum timestamp to filter against.
    df : pandas.DataFrame
        The DataFrame containing the new data to be inserted.
    table_name : str
        The name of the table to be updated.
    key : str
        The column name of the timestamp to filter against.

    Returns
    -------
    tuple
        A tuple containing the result of the update attempt (bool) and
        an error (dict) if the update failed. The error dict contains
        information about the error: error_type, error_message, and sql.
    """
    return update_rows(max_dt, df, eng)


def update_rows(max_dt, df, eng):
    """
    Inserts new data into the specified DB table by filtering and formatting a DataFrame.

    This function filters the input DataFrame for records with the specified key greater than
    the given max_dt threshold, removes duplicates, and constructs a SQL INSERT statement.
    The SQL statement includes an ON DUPLICATE KEY UPDATE clause to handle duplicate entries.

    Parameters
    ----------
    max_dt : datetime
        The maximum timestamp to filter against.
    df : pandas.DataFrame
        The DataFrame containing the data to be inserted.
    table_name : str
        The name of the table to be updated.
    key : str, optional
        The column name of the timestamp to filter against, by default "DateTimeServer".

    Returns
    -------
    tuple
        A tuple containing the result of the update attempt (SimpleNamespace) and
        an error (None) if the update succeeds. If the update fails, returns None
        and an error string.

    Notes
    -----
    - The DataFrame is filtered to exclude duplicates based on the specified key.
    - The SQL query is generated with an ON DUPLICATE KEY UPDATE clause to update
      existing records.
    - SQL injection vulnerability exists due to direct string formatting in the query.
    """

    # Define the threshold datetime
    threshold = pd.to_datetime(max_dt.isoformat())
    # Filter rows where datetime_column is greater than or equal to the threshold
    df_filtered = df[df["datetimeserver"] > threshold]
    l = len(df_filtered)
    df_filtered = df_filtered.drop_duplicates(subset="datetimeserver", keep="first")
    # print(l - len(df_filtered))
    if l == 0:
        return SimpleNamespace(rowcount=0), None

    # TODO not substituting params correctly
    # # Prepare columns and placeholders for a single insert statement
    # columns = ', '.join(df_filtered.columns)  # Get the column names as comma-separated values
    # placeholders = f"({', '.join([':' + col for col in df_filtered.columns])})"

    # # Create the SQL query with placeholders for each value
    # sql_query = f"INSERT INTO {table_name} ({columns}) VALUES {placeholders}"

    # # Prepare parameter list
    # param_list = []

    # # Replace "err" with None in the DataFrame
    # for idx, row in df_filtered.iterrows():
    #     param = row.to_dict()  # Convert row to dictionary
    #     # Replace "err" or NaN with None
    #     for key, value in param.items():
    #         if value == "err" or pd.isna(value):  # Handle NaN and "err" values
    #             param[key] = None
    #     param_list.append(param)

    # # Ensure param_list is not empty before executing
    # if param_list:
    #     try:
    #         # Execute the query with multiple bind parameters
    #         res = sql_execute(text(sql_query), param_list)
    #         if isinstance(res, str):
    #             logger.error(f'ERROR update_leonics_db: {res}')
    #             return None, 'Error: query result is not a string'
    #         logger.debug(f'ROWS UPDATED: {table_name}  {res.rowcount}')
    #         return res, None
    #     except Exception as e:
    #         print(f"Error occurred: {e}")
    # else:
    #     print("No data to insert.")

    # Construct the SQL query
    columns = ", ".join(
        df_filtered.columns
    )  # Get the column names as comma-separated values

    # values = ", ".join(
    #     f"({', '.join(f'\'{val.strftime('%Y-%m-%d %H:%M')}\'' if isinstance(val, pd.Timestamp) else str(val) for val in df_filtered.loc[idx])})"
    #     for idx in df_filtered.index
    # )

    def format_value(val):
        if isinstance(val, pd.Timestamp):
            return f"'{val.strftime('%Y-%m-%d %H:%M')}'"
        return str(val)

    values = ", ".join(
        f"({', '.join(format_value(val) for val in df_filtered.loc[idx])})"
        for idx in df_filtered.index
    )
    values = values.replace("err", "NULL")
    values.replace("'err'", "NULL")
    # Full DB INSERT statement
    sql_query = f"INSERT INTO takum_leonics_api_raw ({columns}) VALUES {values}"
    sql_pred = " ON DUPLICATE KEY UPDATE "

    default_engine, _ = set_db_engine_by_name("postgresql")
    sql_pred = " ON CONFLICT (datetimeserver) DO UPDATE SET "
    for col in df_filtered.columns:
        sql_pred += f"{col} = EXCLUDED.{col}, "
    sql_pred = sql_pred[:-2] + " RETURNING datetimeserver;"

    sql_query += sql_pred
    res, err = sql_execute(sql_query, eng)
    assert err is None
    logger.debug(f"ROWS UPDATED: {len(res)}")
    return res, None


def prospect_get_start_ts(local=None, start_ts=None):
    """
    Retrieves data from the Prospect API and updates the MySQL database.

    This function constructs a URL and fetches data from the Prospect API using the provided
    function to get the necessary URL and API key. It then retrieves the latest timestamp
    from the API response, queries the MySQL database for newer records, and sends this data
    back to the Prospect API. If the API call fails, it logs an error and exits the program.

    Args:
        local (bool): A flag indicating whether to retrieve data from the local or external
                      Prospect API. When True, retrieves from the local API.
        start_ts (str, optional): The timestamp to start retrieval from. If not provided (default),
                                  retrieves the latest timestamp from the Prospect API.

    Raises:
        SystemExit: Exits the program if the Prospect API call fails.

    Logs:
        Various debug and informational logs, including headers, keys, URLs, and response
        statuses. Also logs errors if API calls or database operations fail.
    """

    if start_ts is not None:
        return start_ts
    else:
        server = "datetimeserver"
        postfix = "sys_%"
        conn_str = const.PROS_CONN_AZURE_STR
        if local:
            conn_str = const.PROS_CONN_LOCAL_STR
        prospect_engine = set_db_engine(conn_str)
        sql = f"select custom->>'{server}', external_id from data_custom where external_id like '{postfix}' order by custom->>'{server}' desc limit 1"
        dt, err = sql_execute(sql, prospect_engine)
        assert err is None
        val = dt[0][0]
        return datetime.strptime(val, "%Y-%m-%d %H:%M")


def update_prospect(eng, start_ts=None, local=None):
    """
    Updates the Prospect API with new data entries from the database.

    This function retrieves the latest data entries from the specified database table
    since the given timestamp and sends them to the Prospect API. The data entries
    are fetched in batches and transformed into a Pandas DataFrame before being sent
    to the API. The function logs the update process and handles any exceptions that occur.

    Args:
        start_ts (str, optional): The starting timestamp for the data retrieval process. Defaults to None.
        local (bool, optional): A flag indicating whether to use the local or external Prospect API. Defaults to None.
        table_name (str, optional): The name of the database table to query. Defaults to const.LEONICS_RAW_TABLE.

    Returns:
        tuple: A tuple containing the API response and an error message (if any). Returns None and an error message if the API call fails.

    Logs:
        Various informational and error logs, including the status of API calls and any exceptions that occur.
    """

    logger.info(f"Starting update_prospect ts: {start_ts}  local = {local}")
    try:
        start_ts = prospect_get_start_ts(local, start_ts)
        rows, err = sql_execute(
            f"select * FROM takum_leonics_api_raw where DatetimeServer >= '{start_ts}' order by DatetimeServer  limit 50000;",
            eng,
        )
        assert err is None

        # Convert the result to a Pandas DataFrame
        columns = columns = list(rows[0]._fields) if rows else []
        # Get column names
        df = pd.DataFrame(rows, columns=columns)
        postfix = "sys_"
        # if local:
        #     postfix='raw_'
        df["external_id"] = df["external_id"].astype(str).apply(lambda x: postfix + x)

        res = api_prospect.api_in_prospect(df, local)
        if res is None:
            logger.error("Prospect API failed")
            return None, '"Prospect API failed"'
        logger.info(f"{res.status_code}:  {res.text}")

        # Save the DataFrame to a CSV file
        #####logger = logging.getLogger()
        if logger.getEffectiveLevel() < logging.INFO:
            sts = start_ts.replace(" ", "_").replace(":", "HM")
            df.to_csv(f"sys_pros_{sts}.csv", index=False)

        logger.info("Data has been saved to 'sys_pros'")
        return res, None

    except Exception as e:
        logger.error(f"PROSPECT Error occurred: {e}")
        return None, e


# WIP
def backfill_prospect(start_ts=None, local=True):
    """
    Updates the Prospect API with new data entries.

    This function initiates the update process for the Prospect API by retrieving the
    necessary keys and URL based on the local flag. It logs the starting timestamp
    and handles any exceptions that occur during the process.

    Args:
        start_ts (str, optional): The starting timestamp for the update process. Defaults to None.
        local (bool, optional): A flag indicating whether the update is local or not. Defaults to True.

    Raises:
        Exception: Logs any errors that occur during the prospect key retrieval process.
    """

    logger.info(f"Starting update_prospect ts: {start_ts}  local = {local}")
    try:
        prospect_backfill_key(api_prospect.get_prospect_url_key, start_ts, local)
    except Exception as e:
        logger.error(f"PROSPECT Error occurred: {e}")


# WIP
def prospect_backfill_key(
    func, start_ts, local=None, table_name="defaultdb.TAKUM_LEONICS_API_RAW"
):
    """
    Retrieves data from the Prospect API and updates the DB database.

    This function constructs a URL and fetches data from the Prospect API using the provided
    function to get the necessary URL and API key. It then retrieves the latest timestamp
    from the API response, queries the DB database for newer records, and sends this data
    back to the Prospect API. If the API call fails, it logs an error and exits the program.

    Args:
        func (callable): A function that returns the API URL and key based on the 'local' flag.
        local (bool): A flag indicating whether to retrieve data from the local or external
                      Prospect API. When True, retrieves from the local API.

    Raises:
        SystemExit: Exits the program if the Prospect API call fails.

    Logs:
        Various debug and informational logs, including headers, keys, URLs, and response
        statuses. Also logs errors if API calls or database operations fail.
    """

    url, key = func(local, out=True)
    sid = 3 if local else 421
    url += f"/v1/out/custom/?size=50&page=1&q[source_id_eq]={sid}&q[s]=created_at+desc"
    payload = {}
    headers = {
        "Authorization": f"Bearer {key}",
    }

    logger.info(f"\n\n{key}\n{url}\n{start_ts}")

    rows, err = sql_execute(
        f"select * FROM {table_name} where DatetimeServer > '{start_ts}' order by DatetimeServer limit 1450",
        default_engine,
        # {'ts':start_ts}
    )
    assert err is None

    # Convert the result to a Pandas DataFrame
    columns = res.keys()  # Get column names
    df = pd.DataFrame(rows, columns=columns)
    postfix = "sys_"
    # if local:
    #     postfix='raw_'
    df["external_id"] = df["external_id"].astype(str).apply(lambda x: postfix + x)

    res = api_prospect.api_in_prospect(df, local)
    if res is None:
        logger.error("Prospect API failed, exiting")
        exit()
    logger.info(f"{res.status_code}:  {res.text}")

    sts = start_ts.replace(" ", "_").replace(":", "HM")
    sts += str(local)
    df.to_csv(f"sys_pros_{sts}.csv", index=False)

    # Save the DataFrame to a CSV file
    #####logger = logging.getLogger()
    if logger.getEffectiveLevel() < logging.INFO:
        sts = start_ts.replace(" ", "_").replace(":", "HM")
        sts += str(local)
        df.to_csv(f"sys_pros_{sts}.csv", index=False)
        # df.to_json(f'sys_pros_{sts}.json', index=False)

    logger.info(f"Data has been saved to 'sys_pros'   LOCAL: {local}")


def update_fuel_data(engine, merged_hourly_sums, table, site):
    """
    Updates the fuel data in the database.

    This function takes in the merged hourly sums DataFrame, connection string, table name, and site name.
    It constructs the SQL query string by replacing 'TABLE' with the table name and adds the VALUES
    clause based on the merged_hourly_sums DataFrame. The SQL query is then executed using the connection
    string and the updated row count is printed. Finally, the merged_hourly_sums DataFrame is saved to a CSV
    file named 'mhs_<site>.csv' and the function returns None, None, None.

    Args:
        conn_str (str): The connection string to the database.
        merged_hourly_sums (pd.DataFrame): The merged hourly sums DataFrame.
        table (str): The name of the table to update.
        site (str): The name of the site.

    Returns:
        None, None, None
    """
    sql = """
    WITH insert_attempt AS (
        INSERT INTO fuel.TABLE(
            st_ts,
            end_ts,
            gen_kwh,
            delta1,
            delta2,
            kwh_l_dg1,
            kwh_l_dg2
        )
        VALUES
        ON CONFLICT (st_ts)
        DO UPDATE SET
            end_ts = EXCLUDED.end_ts,
            gen_kwh = EXCLUDED.gen_kwh,
            delta1 = EXCLUDED.delta1,
            delta2 = EXCLUDED.delta2,
            kwh_l_dg1 = EXCLUDED.kwh_l_dg1,
            kwh_l_dg2 = EXCLUDED.kwh_l_dg2
        RETURNING xmax = 0 AS inserted
    )
    SELECT
        COUNT(*) FILTER (WHERE inserted) AS inserted_count,
        COUNT(*) FILTER (WHERE NOT inserted) AS updated_count
    FROM insert_attempt;
        """.replace(
        "TABLE", table
    )

    merged_hourly_sums_notnull = merged_hourly_sums.where(
        pd.notnull(merged_hourly_sums), "null"
    )
    # date	hour	kwh	deltal1	deltal2	kWh/L1	kWh/L2
    #  0     1       2    3        4      5       6
    sql_vals = "VALUES "
    for v in merged_hourly_sums_notnull.values:
        ts = datetime.combine(v[0], datetime.min.time()).replace(hour=int(v[1]))

        ts_str = ts.isoformat()
        end_str = (ts + timedelta(hours=1)).isoformat()
        sql_vals += f" ('{ts_str}','{end_str}',{v[2]},{v[3]},{v[4]},{v[5]},{v[6]}),"

    sql = sql.replace("VALUES", sql_vals[:-1]).replace("\n", " ")
    res, err = sql_execute(sql, engine)
    # for debugging
    # merged_hourly_sums_notnull.to_csv(f"mhs_{site}.csv")
    return res, err


def update_bulk_fuel(engine, df, table):
    """
    Updates the bulk fuel data in the specified database table.

    This function processes a DataFrame containing bulk fuel data, checking each row
    against the latest timestamp in the database to determine if it should be
    inserted or updated. It constructs an SQL query to insert new data or update
    existing records based on the 'Unit Name', 'Time', and 'Event Name' columns.
    If a row's 'Value' field contains specific keywords, it extracts numerical
    values for 'liters_used' or 'liter_bought'. The function returns the result
    of the SQL execution.

    Args:
        engine (sqlalchemy.engine.Engine): The connection engine to use for the database.
        df (pd.DataFrame): The DataFrame containing bulk fuel data.
        table (str): The name of the database table to update.

    Returns:
        tuple: A tuple containing the result of the SQL execution and an error message
        (if any). Returns None and an error message if no new data is found to update.
    """

    res, err = sql_execute(f'select max("Time") from fuel.{table};', engine)
    if res:
        ts = datetime.strptime(res[0][0], "%Y-%m-%d %H:%M:%S")
    else:
        # all data will be saved
        ts = datetime.now() + timedelta(days=1)

    sql_vals = "VALUES "
    # 'Unit Name', 'Time', 'Event Name', 'Value', 'liters_used', 'liter_bought'
    found = False
    for row in df.itertuples(index=True, name="Row"):
        if row[2] <= ts:
            continue
        found = True
        lu = np.nan
        lb = np.nan
        val = row[4]
        if " Liter" in val:
            if " Fuel_Drop" in row[3]:
                lu = val.replace(" Liter", "")
            elif " Refueled" in row[3]:
                lb = val.replace(" Liter", "")
        if val == "nan":
            val = ""
        new_row = {
            "Unit Name": row[1],
            "Time": row[2],
            "Event Name": row[3],
            "Value": val,
            "liters_used": lu,
            "liter_bought": lb,
        }
        if isinstance(lu, float) and math.isnan(lu):
            lu = None
        if isinstance(lb, float) and math.isnan(lb):
            lb = None
        sql_vals += f" ('{row[1]}','{row[2]}','{row[3]}','{val}',{lu},{lb}),"

    if not found:
        return None, "Nothing to update"

    sql = """
    WITH insert_attempt AS (
        INSERT INTO fuel.TABLE ("Unit Name", "Time", "Event Name", "Value", liters_used, liter_bought)
        VALUES
        ON CONFLICT ("Unit Name", "Time", "Event Name")
        DO UPDATE SET
            "Value" = EXCLUDED."Value",
            liters_used = EXCLUDED.liters_used,
            liter_bought = EXCLUDED.liter_bought
        RETURNING xmax = 0 AS inserted
    )
    SELECT
        COUNT(*) FILTER (WHERE inserted) AS inserted_count,
        COUNT(*) FILTER (WHERE NOT inserted) AS updated_count
    FROM insert_attempt;
    """.replace(
        "TABLE", table
    )

    sql = (
        sql.replace("VALUES", sql_vals[:-1]).replace("\n", " ").replace("None", "null")
    )

    return sql_execute(sql, engine)


# TODO Rename this here and in `execute`
def update_takum_raw_db(token, start_ts):
    def set_date_range(st_dt, num_days, backfill=False):
        st = (st_dt + timedelta(minutes=1)).date().isoformat()
        st = st.replace("-", "")
        ed = datetime.now() + timedelta(days=num_days)
        if backfill:
            ed = st_dt + timedelta(days=num_days)
        # Leonics API has a limit of 10 days
        if ed - st_dt > timedelta(days=10):
            ed = st_dt + timedelta(days=10)
        ed = ed.date().isoformat()
        start_ts = ed
        ed = ed.replace("-", "")
        return st, ed

    num_days = 2
    max_dt_local = start_ts
    if start_ts is None:
        max_dt_azure, err = get_db_max_date(azure_defaultdb_engine)
        if err:
            logger.error(f"get_db_max_date Error occurred: {err}")
            exit(1)
        assert max_dt_azure is not None

        if local_defaultdb_engine is not None:
            max_dt_local, err = get_db_max_date(local_defaultdb_engine)
            if err:
                logger.error(f"get_db_max_date1 Error occurred: {err}")
                exit(1)
            assert max_dt_local is not None
        else:
            max_dt_local = None
    else:
        max_dt_azure = start_ts
        max_dt_local = start_ts

        # backfill 1 week
        # max_dt = max_dt - timedelta(days=7)
    st, ed = set_date_range(max_dt_azure, num_days)
    df_azure, err = api_leonics.getData(start=st, end=ed, token=token)
    if err:
        logger.error(f"api_leonics.getData Error occurred: {err}")
        exit(2)
    # Convert the 'datetime_column' to pandas datetime
    df_azure["DatetimeServer"] = pd.to_datetime(df_azure["DatetimeServer"])
    df_azure.columns = df_azure.columns.str.lower()
    res, err = update_leonics_db(max_dt_azure, df_azure, azure_defaultdb_engine)
    if err:
        logger.error(f"update_leonics_db Error occurred: {err}")
        exit(3)
    else:
        logger.info(f"AZURE ROWS UPDATED:   {len(res)}")

    if max_dt_local is not None:
        st, ed = set_date_range(max_dt_local, num_days)
        df_leonics, err = api_leonics.getData(start=st, end=ed, token=token)
        if err:
            logger.error(f"api_leonics.getData Error occurred: {err}")
            exit(2)
        # Convert the 'datetime_column' to pandas datetime
        df_leonics["DatetimeServer"] = pd.to_datetime(df_leonics["DatetimeServer"])
        df_leonics.columns = df_leonics.columns.str.lower()
        res, err = update_leonics_db(max_dt_local, df_leonics, local_defaultdb_engine)
        if err:
            logger.error(f"update_leonics_db Error occurred: {err}")
            exit(3)
        else:
            logger.info(f"LOCAL ROWS UPDATED:  {len(res)}")

    return start_ts


local_defaultdb_engine = None
azure_defaultdb_engine = None


def set_local_defaultdb_engine():
    global local_defaultdb_engine
    if local_defaultdb_engine is None:
        local_defaultdb_engine, _ = set_db_engine_by_name("postgresql", local=True)
    return local_defaultdb_engine


def set_azure_defaultdb_engine():
    global azure_defaultdb_engine
    if azure_defaultdb_engine is None:
        azure_defaultdb_engine, _ = set_db_engine_by_name("postgresql", local=False)
    return azure_defaultdb_engine


set_local_defaultdb_engine()
set_azure_defaultdb_engine()


def get_sm_weather_max_epoch(device_id, engine):
    """
    Retrieves the latest timestamp from the database. If the database is empty or an error occurs,
    returns None and the error.
    Args:
        device_id (int): The device ID to query the database for.
        engine (sqlalchemy.engine.Engine): The connection engine to use.
    Returns:
        tuple: A tuple containing the latest timestamp as an integer, and None if the query was successful, or an error message if it was not.
    """
    sql = f"select max(org_epoch) FROM solarman.weather where device_id = {device_id}"
    val, err = sql_execute(sql, engine)
    if err is not None:
        return None, err
    epoch = val[0][0]
    return epoch, None


def get_fuel_max_ts(site, engine):
    """
    Retrieves the latest start timestamp for a given site from the fuel database.

    Args:
        site (str): The name of the site to query the database for.
        engine (sqlalchemy.engine.Engine): The connection engine to use.

    Returns:
        tuple: A tuple containing the latest start timestamp as a datetime object,
        and None if the query was successful, or an error message if it was not.
    """

    sql = f"SELECT max(st_ts) FROM fuel.fuel_kwh_{site.lower()}"
    val, err = sql_execute(sql, engine)
    if err is not None:
        return None, err
    ts = val[0][0]
    return ts, None


def get_gb_epoch(serial_num, engine, max=True):
    """
    Retrieves the latest or earliest timestamp from the GB database for a given serial number.

    Args:
        serial_num (str): The serial number to query the database for.
        engine (sqlalchemy.engine.Engine): The connection engine to use.
        max (bool): Whether to select the maximum (latest) timestamp or the minimum (earliest).

    Returns:
        tuple: A tuple containing the selected timestamp as an integer, and None if the query was successful, or an error message if it was not.
    """
    max_str = "max" if max else "min"
    sql = f"select {max_str}(epoch_secs) FROM eyedro.gb_{serial_num}"
    res, err = sql_execute(sql, engine)
    if err is not None:
        return None, err
    epoch = res[0][0]
    return epoch, None


def set_db_engines():
    """
    Set the database engines based on the environment.

    If running on Azure, a local database engine is used. Otherwise, an Azure
    database engine is used. If running in a Docker container, a local database
    engine is also used.

    Returns:
        list: A list of database engines.
    """

    engines = []
    if const.is_running_on_azure():
        engines = [set_local_defaultdb_engine()]
    else:
        engines = [set_azure_defaultdb_engine()]
        if utils.prospect_running():
            engines.append(set_local_defaultdb_engine())
    return engines
