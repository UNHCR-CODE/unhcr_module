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

WIP backfill_prospect(start_ts=None, local=True) & prospect_backfill_key(func, start_ts, local, table_name): 
    These functions appear to be related to backfilling data into the Prospect API but are marked as "WIP" 
    (work in progress) and are not fully functional.
"""

from contextlib import contextmanager
from datetime import datetime
import logging
from types import SimpleNamespace
import pandas as pd
from sqlalchemy import create_engine, exc, orm, text

from unhcr import constants as const
from unhcr import api_prospect

if const.LOCAL:  # testing with local python files
    const, api_prospect, *rest = const.import_local_libs(
        mods=[["constants", "const"], ["api_prospect", "api_prospect"]]
    )

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
    return create_engine(
        connection_string,
        pool_size=const.SQLALCHEMY_POOL_SIZE,
        pool_timeout=const.SQLALCHEMY_POOL_TIMEOUT,
        pool_recycle=const.SQLALCHEMY_POOL_RECYCLE,
        max_overflow=const.SQLALCHEMY_MAX_OVERFLOW,
    )


@contextmanager
def get_db_session(engine):
    """Provide a transactional scope around a series of operations."""
    Session = orm.sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except exc.SQLAlchemyError:
        session.rollback()
        raise
    finally:
        session.close()


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

    with get_db_session(engine) as session:
        try:
            # Use SQLAlchemy's execute method
            result = session.execute(text(sql), params=data)

            # If it's a SELECT query, fetch results
            if sql.strip().upper().startswith("SELECT"):
                return result, None

            # For INSERT, UPDATE, DELETE return the result
            session.commit()
            return result, None

        except exc.SQLAlchemyError as db_error:
            session.rollback()
            error_msg = f"Database update failed: {str(db_error)}"
            logging.error(error_msg)
            return False, {
                "error_type": type(db_error).__name__,
                "error_message": str(db_error),
                "sql": sql,
            }
        except ValueError as val_error:
            session.rollback()
            error_msg = f"Data validation error: {str(val_error)}"
            logging.error(error_msg)
            return False, {
                "error_type": "ValidationError",
                "error_message": str(val_error),
                "sql": sql,
            }
        except Exception as e:
            session.rollback()
            error_msg = f"Unexpected error: {str(e)}"
            logging.error(error_msg)
            return False, {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "sql": sql,
            }
        except Exception as e:
            session.rollback()
            return False, {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "sql": sql,
            }
        finally:
            session.close()


default_engine = set_db_engine(const.TAKUM_RAW_CONN_STR)


def get_db_max_date(engine=default_engine, table_name="TAKUM_LEONICS_API_RAW"):
    """
    Retrieves the latest timestamp from the database. If the database is empty or an error occurs,
    returns None and the error.
    """

    try:
        dt, err = sql_execute(f"select max(DatetimeServer) FROM {table_name}", engine)
        assert err is None
        dt = dt.fetchall()
        val = dt[0][0]
        return datetime.strptime(val, "%Y-%m-%d %H:%M"), None
    except Exception as e:
        logging.error(f"Can not get DB max timsestanp   {e}")
        return None, e


def update_leonics_db(max_dt, df, table_name, key="DatetimeServer"):
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
    return update_rows(max_dt, df, table_name, key)


def update_rows(max_dt, df, table_name, key="DateTimeServer"):
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
    df_filtered = df[df[key] > threshold]
    l = len(df_filtered)
    df_filtered = df_filtered.drop_duplicates(subset=key, keep="first")
    print(l - len(df_filtered))
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
    #             logging.error(f'ERROR update_leonics_db: {res}')
    #             return None, 'Error: query result is not a string'
    #         logging.debug(f'ROWS UPDATED: {table_name}  {res.rowcount}')
    #         return res, None
    #     except Exception as e:
    #         print(f"Error occurred: {e}")
    # else:
    #     print("No data to insert.")

    # Construct the SQL query
    columns = ", ".join(
        df_filtered.columns
    )  # Get the column names as comma-separated values

    values = ", ".join(
        f"({', '.join(f'\'{val.strftime('%Y-%m-%d %H:%M')}\'' if isinstance(val, pd.Timestamp) else str(val) for val in df_filtered.loc[idx])})"
        for idx in df_filtered.index
    )
    values = values.replace("err", "NULL")
    values.replace("'err'", "NULL")
    # Full DB INSERT statement
    sql_query = f"INSERT INTO {table_name} ({columns}) VALUES {values}"
    sql_pred = " ON DUPLICATE KEY UPDATE "

    if default_engine.engine.name == "postgresql":
        sql_pred = " ON CONFLICT (datetimeserver) DO UPDATE SET "
        for col in df_filtered.columns:
            sql_pred += f"{col} = EXCLUDED.{col}, "
    else:
        for col in df_filtered.columns:
            sql_pred += f"{col} = VALUES({col}), "
    sql_pred = sql_pred[:-2] + ";"

    sql_query += sql_pred
    res, err = sql_execute(sql_query, default_engine)
    assert err is None
    if not hasattr(res, "rowcount"):
        logging.warning(f"ERROR update_leonics_db: {res}")
        return None, "Error: query result is not a cursor"
    logging.debug(f"ROWS UPDATED: {table_name}  {res.rowcount}")
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
        dt = dt.fetchall()
        val = dt[0][0]
        return datetime.strptime(val, "%Y-%m-%d %H:%M")


def update_prospect(start_ts=None, local=None, table_name=const.LEONICS_RAW_TABLE):
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

    logging.info(f"Starting update_prospect ts: {start_ts}  local = {local}")
    try:
        start_ts = prospect_get_start_ts(local, start_ts)
        res, err = sql_execute(
            f"select * FROM {table_name} where DatetimeServer >= '{start_ts}' order by DatetimeServer  limit 50000;",
            default_engine,
        )
        assert err is None
        # Fetch all results as a list of dictionaries
        rows = res.fetchall()

        # Convert the result to a Pandas DataFrame
        columns = res.keys()  # Get column names
        df = pd.DataFrame(rows, columns=columns)
        postfix = "sys_"
        # if local:
        #     postfix='raw_'
        df["external_id"] = df["external_id"].astype(str).apply(lambda x: postfix + x)

        res = api_prospect.api_in_prospect(df, local)
        if res is None:
            logging.error("Prospect API failed")
            return None, '"Prospect API failed"'
        logging.info(f"{res.status_code}:  {res.text}")

        # Save the DataFrame to a CSV file
        logger = logging.getLogger()
        if logger.getEffectiveLevel() < logging.INFO:
            sts = start_ts.replace(" ", "_").replace(":", "HM")
            sts += str(local)
            df.to_csv(f"sys_pros_{sts}.csv", index=False)
            # df.to_json(f'sys_pros_{sts}.json', index=False)

        logging.info("Data has been saved to 'sys_pros'")
        return res, None

    except Exception as e:
        logging.error(f"PROSPECT Error occurred: {e}")
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

    logging.info(f"Starting update_prospect ts: {start_ts}  local = {local}")
    try:
        prospect_backfill_key(api_prospect.get_prospect_url_key, start_ts, local)
    except Exception as e:
        logging.error(f"PROSPECT Error occurred: {e}")


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

    logging.info(f"\n\n{key}\n{url}\n{start_ts}")

    res, err = sql_execute(
        f"select * FROM {table_name} where DatetimeServer > '{start_ts}' order by DatetimeServer limit 1450",
        default_engine,
        # {'ts':start_ts}
    )
    assert err is None
    # Fetch all results as a list of dictionaries
    rows = res.fetchall()

    # Convert the result to a Pandas DataFrame
    columns = res.keys()  # Get column names
    df = pd.DataFrame(rows, columns=columns)
    postfix = "sys_"
    # if local:
    #     postfix='raw_'
    df["external_id"] = df["external_id"].astype(str).apply(lambda x: postfix + x)

    res = api_prospect.api_in_prospect(df, local)
    if res is None:
        logging.error("Prospect API failed, exiting")
        exit()
    logging.info(f"{res.status_code}:  {res.text}")

    sts = start_ts.replace(" ", "_").replace(":", "HM")
    sts += str(local)
    df.to_csv(f"sys_pros_{sts}.csv", index=False)

    # Save the DataFrame to a CSV file
    logger = logging.getLogger()
    if logger.getEffectiveLevel() < logging.INFO:
        sts = start_ts.replace(" ", "_").replace(":", "HM")
        sts += str(local)
        df.to_csv(f"sys_pros_{sts}.csv", index=False)
        # df.to_json(f'sys_pros_{sts}.json', index=False)

    logging.info(f"Data has been saved to 'sys_pros'   LOCAL: {local}")
