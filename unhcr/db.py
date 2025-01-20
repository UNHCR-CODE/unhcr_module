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

update_leonics_db(max_dt, df, table_name, key='DateTimeServer'): 
    Orchestrates the database update process. Retrieves the latest timestamp from the database, filters new data from the 
    input DataFrame, and inserts the new data into the specified table. Includes error handling.

update_rows(max_dt, df, table_name): 
    Inserts new data into the DB database. Filters the DataFrame, formats data, and performs a bulk INSERT with an 
    ON DUPLICATE KEY UPDATE clause. The ON DUPLICATE KEY UPDATE clause is excessively long and should be refactored.

update_prospect(start_ts=None, local=True): 
    Manages updates to the Prospect API. Retrieves the latest timestamp from Prospect, queries the database for newer records,
    and sends them to the API. This function could benefit from being broken down into smaller, more manageable functions.

set_db_engine(connection_string): 
    Creates and returns a SQLAlchemy engine with connection pooling for efficient database access. Pool parameters are 
    configurable via environment variables.

backfill_prospect(start_ts=None, local=True) & prospect_backfill_key(func, start_ts, local, table_name): 
    These functions appear to be related to backfilling data into the Prospect API but are marked as "WIP" 
    (work in progress) and are not fully functional.
"""
from contextlib import contextmanager
from datetime import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine, exc,orm, text

from unhcr import constants as const
from unhcr import api_prospect

if const.LOCAL:  # testing with local python files
    const, api_prospect, *rest = const.import_local_libs(
        mods=[["constants", "const"], ["api_prospect", "api_prospect"]]
    )

default_engine = None

# Create a connection pool
def set_db_engine(connection_string):
    global default_engine
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
    default_engine = create_engine(
        connection_string,
        pool_size=const.SQLALCHEMY_POOL_SIZE,
        pool_timeout=const.SQLALCHEMY_POOL_TIMEOUT,
        pool_recycle=const.SQLALCHEMY_POOL_RECYCLE,
        max_overflow=const.SQLALCHEMY_MAX_OVERFLOW
    )

    return default_engine

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
                "sql": sql
        }
        except ValueError as val_error:
            session.rollback()
            error_msg = f"Data validation error: {str(val_error)}"
            logging.error(error_msg)
            return False, {
                "error_type": "ValidationError",
                "error_message": str(val_error),
                "sql": sql
            }
        except Exception as e:
            session.rollback()
            error_msg = f"Unexpected error: {str(e)}"
            logging.error(error_msg)
            return False, {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "sql": sql
            }
        except Exception as e:
            session.rollback()
            return False, {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "sql": sql
            }
        finally:
            session.close()

set_db_engine(const.TAKUM_RAW_CONN_STR)

def get_db_max_date(engine=default_engine, table_name='TAKUM_LEONICS_API_RAW'):
    """
    Retrieves the most recent timestamp from the DB database.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine (optional, will use default if not provided)

    Returns:
        tuple: A tuple containing the most recent timestamp in the database and an error message if any.
    """
    try:
        dt, err = sql_execute(
            f"select max(DatetimeServer) FROM {table_name}", engine
        )
        assert err is None
        dt = dt.fetchall()
        val = dt[0][0]
        return datetime.strptime(val, "%Y-%m-%d %H:%M"), None
    except Exception as e:
        logging.error(f"Can not get DB max timsestanp   {e}")
        return None, e


def update_leonics_db(max_dt, df, table_name, key='DateTimeServer'):
    """
    Orchestrates the database update process. It retrieves the latest timestamp from the database,

    Args:
        max_dt (datetime): Maximum datetime for filtering records
        df (pandas.DataFrame): DataFrame to be updated in the database
        table_name (str): Name of the database table to update

    Returns:
        tuple: (success_flag, error_message_or_None)
    """

    # Existing update logic would go here
    # If no specific exception is raised, return success
    return update_rows(max_dt, df, table_name, key)


def update_rows(max_dt, df, table_name, key='DateTimeServer'):
    """
    Updates the specified DB table with new data from a DataFrame.

    This function processes and inserts new data into a DB table by filtering
    the input DataFrame for records with 'DateTimeServer' greater than the specified
    max_dt. The filtered data is formatted and used to generate a SQL bulk INSERT
    statement with an ON DUPLICATE KEY UPDATE clause to handle existing records.

    Args:
        max_dt (datetime): The maximum datetime to be used as a threshold for filtering new data.
        df (pandas.DataFrame): The DataFrame containing new data to be inserted into the database.
        table_name (str): The name of the DB table to update.

    Returns:
        tuple: A tuple containing the result of the SQL execution and an error message if any.
    """

    # Define the threshold datetime
    threshold = pd.to_datetime(max_dt.isoformat())
    # Filter rows where datetime_column is greater than or equal to the threshold
    df_filtered = df[df[key] > threshold]

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
    sql_pred = """ ON DUPLICATE KEY UPDATE
    BDI1_Power_P1_kW = VALUES(BDI1_Power_P1_kW),
    BDI1_Power_P2_kW = VALUES(BDI1_Power_P2_kW),
    BDI1_Power_P3_kW = VALUES(BDI1_Power_P3_kW),
    BDI1_Total_Power_kW = VALUES(BDI1_Total_Power_kW),
    BDI1_Freq = VALUES(BDI1_Freq),
    BDI1_ACinput_Voltage_L1 = VALUES(BDI1_ACinput_Voltage_L1),
    BDI1_ACinput_Voltage_L2 = VALUES(BDI1_ACinput_Voltage_L2),
    BDI1_ACinput_Voltage_L3 = VALUES(BDI1_ACinput_Voltage_L3),
    BDI1_ACinput_P1_kW = VALUES(BDI1_ACinput_P1_kW),
    BDI1_ACinput_P2_kW = VALUES(BDI1_ACinput_P2_kW),
    BDI1_ACinput_P3_kW = VALUES(BDI1_ACinput_P3_kW),
    BDI1_ACinput_Total_kW = VALUES(BDI1_ACinput_Total_kW),
    BDI1_Batt_Voltage = VALUES(BDI1_Batt_Voltage),
    BDI1_Today_Supply_AC_kWh = VALUES(BDI1_Today_Supply_AC_kWh),
    BDI1_Todate_Supply_AC_kWh = VALUES(BDI1_Todate_Supply_AC_kWh),
    BDI2_Power_P1_kW = VALUES(BDI2_Power_P1_kW),
    BDI2_Power_P2_kW = VALUES(BDI2_Power_P2_kW),
    BDI2_Power_P3_kW = VALUES(BDI2_Power_P3_kW),
    BDI2_Total_Power_kW = VALUES(BDI2_Total_Power_kW),
    BDI2_Freq = VALUES(BDI2_Freq),
    BDI2_ACinput_Voltage_L1 = VALUES(BDI2_ACinput_Voltage_L1),
    BDI2_ACinput_Voltage_L2 = VALUES(BDI2_ACinput_Voltage_L2),
    BDI2_ACinput_Voltage_L3 = VALUES(BDI2_ACinput_Voltage_L3),
    BDI2_ACinput_P1_kW = VALUES(BDI2_ACinput_P1_kW),
    BDI2_ACinput_P2_kW = VALUES(BDI2_ACinput_P2_kW),
    BDI2_ACinput_P3_kW = VALUES(BDI2_ACinput_P3_kW),
    BDI2_ACinput_Total_kW = VALUES(BDI2_ACinput_Total_kW),
    BDI2_Today_Batt_Chg_kWh = VALUES(BDI2_Today_Batt_Chg_kWh),
    BDI2_Todate_Batt_Chg_kWh = VALUES(BDI2_Todate_Batt_Chg_kWh),
    BDI2_Today_Batt_DisChg_kWh = VALUES(BDI2_Today_Batt_DisChg_kWh),
    BDI2_Todate_Batt_DisChg_kWh = VALUES(BDI2_Todate_Batt_DisChg_kWh),
    SCC1_PV_Voltage = VALUES(SCC1_PV_Voltage),
    SCC1_PV_Current = VALUES(SCC1_PV_Current),
    SCC1_PV_Power_kW = VALUES(SCC1_PV_Power_kW),
    SCC1_Chg_Voltage = VALUES(SCC1_Chg_Voltage),
    SCC1_Chg_Current = VALUES(SCC1_Chg_Current),
    SCC1_Chg_Power_kW = VALUES(SCC1_Chg_Power_kW),
    SCC1_Today_Chg_kWh = VALUES(SCC1_Today_Chg_kWh),
    SCC1_Today_PV_kWh = VALUES(SCC1_Today_PV_kWh),
    SCC1_Todate_Chg_kWh = VALUES(SCC1_Todate_Chg_kWh),
    SCC1_Todate_PV_kWh = VALUES(SCC1_Todate_PV_kWh),
    HVB1_Avg_V = VALUES(HVB1_Avg_V),
    HVB1_Batt_I = VALUES(HVB1_Batt_I),
    HVB1_SOC = VALUES(HVB1_SOC),
    LoadPM_Power_P1_kW = VALUES(LoadPM_Power_P1_kW),
    LoadPM_Power_P2_kW = VALUES(LoadPM_Power_P2_kW),
    LoadPM_Power_P3_kW = VALUES(LoadPM_Power_P3_kW),
    LoadPM_Total_P_kW = VALUES(LoadPM_Total_P_kW),
    LoadPM_Import_kWh = VALUES(LoadPM_Import_kWh),
    LoadPM_Today_Import_kWh = VALUES(LoadPM_Today_Import_kWh),
    DCgen_Alternator_Voltage = VALUES(DCgen_Alternator_Voltage),
    DCgen_Alternator_Current = VALUES(DCgen_Alternator_Current),
    DCgen_Alternator_Power_kW = VALUES(DCgen_Alternator_Power_kW),
    DCgen_LoadBattery_Voltage = VALUES(DCgen_LoadBattery_Voltage),
    DCgen_LoadBattery_Current = VALUES(DCgen_LoadBattery_Current),
    DCgen_LoadBattery_Power_kW = VALUES(DCgen_LoadBattery_Power_kW),
    DCgen_Alternator_Temp = VALUES(DCgen_Alternator_Temp),
    DCgen_Diode_Temp = VALUES(DCgen_Diode_Temp),
    DCgen_Max_Voltage = VALUES(DCgen_Max_Voltage),
    DCgen_Max_Current = VALUES(DCgen_Max_Current),
    DCgen_Low_Voltage_Start = VALUES(DCgen_Low_Voltage_Start),
    DCgen_High_Voltage_Stop = VALUES(DCgen_High_Voltage_Stop),
    DCgen_Low_Current_Stop = VALUES(DCgen_Low_Current_Stop),
    DCgen_Oil_Pressure = VALUES(DCgen_Oil_Pressure),
    DCgen_Coolant_Temp = VALUES(DCgen_Coolant_Temp),
    DCgen_StartingBatteryVoltage = VALUES(DCgen_StartingBatteryVoltage),
    DCgen_RPM = VALUES(DCgen_RPM),
    DCgen_Min_RPM = VALUES(DCgen_Min_RPM),
    DCgen_Max_RPM = VALUES(DCgen_Max_RPM),
    DCgen_Engine_Runtime = VALUES(DCgen_Engine_Runtime),
    DCgen_Ambient_Temp = VALUES(DCgen_Ambient_Temp),
    DCgen_RPM_Frequency = VALUES(DCgen_RPM_Frequency),
    DCgen_Throttle_Stop = VALUES(DCgen_Throttle_Stop),
    DCgen_Fuel_Level = VALUES(DCgen_Fuel_Level),
    DCgen_Total_kWh = VALUES(DCgen_Total_kWh),
    DCgen_Today_kWh = VALUES(DCgen_Today_kWh),
    FlowMeter_Fuel_Temp = VALUES(FlowMeter_Fuel_Temp),
    FlowMeter_Total_Fuel_consumption = VALUES(FlowMeter_Total_Fuel_consumption),
    FlowMeter_Today_Fuel_consumption = VALUES(FlowMeter_Today_Fuel_consumption),
    FlowMeter_Hourly_Fuel_consumptionRate = VALUES(FlowMeter_Hourly_Fuel_consumptionRate),
    ana1_Inv_Room_Temp = VALUES(ana1_Inv_Room_Temp),
    ana2_Inv_room_RH = VALUES(ana2_Inv_room_RH),
    ana3_Batt_Room_Temp = VALUES(ana3_Batt_Room_Temp),
    ana4_Batt_room_RH = VALUES(ana4_Batt_room_RH),
    ana5_Fuel_Level1 = VALUES(ana5_Fuel_Level1),
    ana6_Fuel_Level2 = VALUES(ana6_Fuel_Level2),
    Out1_CloseMC1 = VALUES(Out1_CloseMC1),
    Out2_StartGen = VALUES(Out2_StartGen),
    Out3_EmergencyStop = VALUES(Out3_EmergencyStop),
    Out4 = VALUES(Out4),
    Out5 = VALUES(Out5),
    Out6 = VALUES(Out6),
    Out7 = VALUES(Out7),
    Out8 = VALUES(Out8),
    In1_BDI_Fail = VALUES(In1_BDI_Fail),
    In2_ATS_status = VALUES(In2_ATS_status),
    In3_door_sw = VALUES(In3_door_sw),
    In4 = VALUES(In4),
    In5 = VALUES(In5),
    In6 = VALUES(In6),
    In7 = VALUES(In7),
    In8 = VALUES(In8);"""
    if default_engine.engine.name == 'postgresql':
        sql_pred = " ON CONFLICT (datetimeserver) DO UPDATE SET "
        for col in df_filtered.columns:
            sql_pred += f"{col} = EXCLUDED.{col}, "
        sql_pred = sql_pred[:-2] + ";"
    
    sql_query += sql_pred
    res, err = sql_execute(sql_query, default_engine)
    assert err is None
    if not hasattr(res, "rowcount"):
        logging.warning(f"ERROR update_leonics_db: {res}")
        return None, "Error: query result is not a cursor"
    logging.debug(f"ROWS UPDATED: {table_name}  {res.rowcount}")
    return res, None


def update_prospect(start_ts=None, local=None, table_name = const.LEONICS_RAW_TABLE):
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
        start_ts = api_prospect.prospect_get_start_ts(local, start_ts)
        res, err = sql_execute(
            f"select * FROM {table_name} where DatetimeServer > '{start_ts}' order by DatetimeServer",
            default_engine
        )
        assert err is None
        # Fetch all results as a list of dictionaries
        rows = res.fetchall()

        # Convert the result to a Pandas DataFrame
        columns = res.keys()  # Get column names
        df = pd.DataFrame(rows, columns=columns)

        df["external_id"] = df["external_id"].astype(str).apply(lambda x: "sys_" + x)

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
def prospect_backfill_key(func, start_ts, local=None, table_name = 'defaultdb.TAKUM_LEONICS_API_RAW'):
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
    sid = 1 if local else 421
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

    df["external_id"] = df["external_id"].astype(str).apply(lambda x: "sys_" + x)

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


###################################################
# Hey there - I've reviewed your changes and found some issues that need to be addressed.

# Blocking issues:

# Potential SQL injection vulnerability in query construction (e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:259)
# Overall Comments:

# Critical security vulnerability: The update_rows function uses string formatting for SQL queries which enables SQL injection attacks. Switch to using parameterized queries with SQLAlchemy's text() function and parameter binding.
# The ON DUPLICATE KEY UPDATE clause is hardcoded with a long list of columns. Consider generating this dynamically from the column list to improve maintainability and reduce potential errors.
# Here's what I looked at during the review
# 🟡 General issues: 2 issues found
# 🔴 Security: 1 blocking issue
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:202

# issue(security): Potential SQL injection vulnerability in query construction
#         }


# def update_rows(max_dt, df, table_name):
#     """
#     Updates the specified DB table with new data from a DataFrame.
# The current method of constructing SQL queries by directly formatting values is extremely risky. Replace with SQLAlchemy's parameterized query methods or prepared statements to prevent potential SQL injection attacks. The commented-out code shows a better approach with parameterized queries.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:259

# issue(security): Potential SQL injection vulnerability in query construction
#     )
#     values = values.replace("err", "NULL")
#     # Full DB INSERT statement
#     sql_query = f"INSERT INTO {table_name} ({columns}) VALUES {values}"
#     sql_query += """ ON DUPLICATE KEY UPDATE
#     BDI1_Power_P1_kW = VALUES(BDI1_Power_P1_kW),
# Replace manual string concatenation with SQLAlchemy's parameterized query methods. The commented-out approach using text() and params was closer to a secure implementation.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:185

# suggestion(code_refinement): Function has multiple responsibilities and complex logic
#         }


# def update_rows(max_dt, df, table_name):
#     """
#     Updates the specified DB table with new data from a DataFrame.
# Break down the function into smaller, more focused methods. Separate concerns like data filtering, SQL query generation, and execution.
