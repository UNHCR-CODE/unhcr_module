"""
Overview
    This script db.py extracts data from a Leonics API, stores it in a MySQL database (Aiven), and updates 
    a Prospect system with new entries. It uses requests for API interaction, pandas for data manipulation, 
    and sqlalchemy for database operations. The script is designed for incremental updates to both the database 
    and Prospect to avoid redundant data transfer.

Key Components
    mysql_execute(sql, data=None): 
        Executes SQL queries against the MySQL database. It handles session creation, execution, commit/rollback, 
        and closure.
update_mysql(max_dt, df, table_name): 
    Orchestrates the database update process. It retrieves the latest timestamp from the database, 
    fetches new data from the Leonics API since the last update, and inserts it into the database.
update_rows(max_dt, df, table_name): 
    Handles fetching data from the Leonics API via api_leonics.getData(), filters for new records based on the 
    max_dt timestamp, formats the data, and performs a bulk INSERT into the MySQL database.
update_prospect(start_ts=None, local=True): 
    Manages the Prospect update process. It retrieves the latest timestamp from Prospect, queries the database 
    for newer records, formats them, and sends them to the Prospect API using api_prospect.api_in_prospect().
prospect_get_key(func, local, start_ts=None): 
    Retrieves the Prospect API URL and key based on the local flag. It also gets the latest timestamp from 
    Prospect to avoid duplicates.
get_prospect_last_data(response): 
    Parses the Prospect API response to extract the latest timestamp and external_id.
backfill_prospect(start_ts=None, local=True): 
    Similar to update_prospect, but designed for backfilling data into Prospect, using prospect_backfill_key.
prospect_backfill_key(func, local, start_ts): 
    Retrieves a larger batch of data from the database (limited to 1450 rows) and sends it to Prospect, 
    primarily for backfilling purposes.
    
The script relies on constants.py for configuration, api_leonics.py for Leonics API interaction 
(not shown in the provided code), and api_prospect.py for Prospect API interaction. 
It includes error handling and logging.
"""

from datetime import datetime, timedelta
import json
import logging
import traceback
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import urllib3

# Suppress InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from unhcr import constants as const
from unhcr import api_prospect

if const.LOCAL:  # testing with local python files
    const, api_prospect, *rest = const.import_local_libs(
        mods=[["constants", "const"], ["api_prospect", "api_prospect"]]
    )


def mysql_execute(sql, data=None):
    """Execute a SQL query against the Aiven MySQL database.

    Args:
        sql (str): The SQL query to execute. May contain placeholders for data.
        data (dict, optional): A dictionary containing values to substitute into the SQL query.

    Returns:
        sqlalchemy.engine.result.ResultProxy: The result of the query execution.

    Raises:
        Exception: If any error occurs while executing the query.
    """

    engine = create_engine(const.AIVEN_TAKUM_CONN_STR)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        result = session.execute(sql, {"data": data})
        session.commit()
        return result
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def get_mysql_max_date():
    """Retrieves the most recent timestamp from the MySQL database.

    Returns:
        tuple: A tuple containing the most recent timestamp in the database and an error message if any.
    """
    try:
        dt = mysql_execute(
            text("select max(DatetimeServer) FROM defaultdb.TAKUM_LEONICS_API_RAW")
        )
        val = dt.scalar()
        return datetime.strptime(val, "%Y-%m-%d %H:%M"), None
    except Exception as e:
        logging.error(f"Can not get DB max timsestanp   {e}")
        return None, e


def update_mysql(max_dt, df, table_name):
    """
    Orchestrates the database update process. It retrieves the latest timestamp from the database,
    fetches new data from the Leonics API since the last update, and inserts it into the database.

    Args:
        max_dt (datetime): The maximum datetime to be used as a threshold for filtering new data.
        df (pandas.DataFrame): The DataFrame containing new data to be inserted into the database.
        table_name (str): The name of the database table to update.

    Returns:
        tuple: A tuple containing the result of the update operation and an error message, if any.
    """

    try:
        return update_rows(max_dt, df, table_name)
    except Exception as e:
        traceback.print_exc()
        logging.error(f"update_mysql Error occurred: {e}")
        return None, e


def update_rows(max_dt, df, table_name):
    """
    Updates the specified MySQL table with new data from a DataFrame.

    This function processes and inserts new data into a MySQL table by filtering
    the input DataFrame for records with 'DateTimeServer' greater than the specified
    max_dt. The filtered data is formatted and used to generate a SQL bulk INSERT
    statement with an ON DUPLICATE KEY UPDATE clause to handle existing records.

    Args:
        max_dt (datetime): The maximum datetime to be used as a threshold for filtering new data.
        df (pandas.DataFrame): The DataFrame containing new data to be inserted into the database.
        table_name (str): The name of the MySQL table to update.

    Returns:
        tuple: A tuple containing the result of the SQL execution and an error message if any.
    """

    df["DateTimeServer"] = pd.to_datetime(df["DateTimeServer"])
    # Define the threshold datetime
    threshold = pd.to_datetime(max_dt.isoformat())
    # Filter rows where datetime_column is greater than or equal to the threshold
    df_filtered = df[df["DateTimeServer"] > threshold]

    df_filtered.loc[:, "DateTimeServer"] = df_filtered["DateTimeServer"].dt.strftime(
        "%Y-%m-%d %H:%M"
    )

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
    #         res = mysql_execute(text(sql_query), param_list)
    #         if isinstance(res, str):
    #             logging.error(f'ERROR update_mysql: {res}')
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
    # Full MySQL INSERT statement
    sql_query = f"INSERT INTO {table_name} ({columns}) VALUES {values}"
    sql_query += """ ON DUPLICATE KEY UPDATE
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

    res = mysql_execute(text(sql_query))
    if isinstance(res, str):
        logging.error(f"ERROR update_mysql: {res}")
        return None, "Error: query result is not a string"
    logging.debug(f"ROWS UPDATED: {table_name}  {res.rowcount}")
    return res, None


def update_prospect(start_ts=None, local=True):
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
        prospect_get_key(api_prospect.get_prospect_url_key, local, start_ts)
    except Exception as e:
        logging.error(f"PROSPECT Error occurred: {e}")


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
        prospect_backfill_key(api_prospect.get_prospect_url_key, local, start_ts)
    except Exception as e:
        logging.error(f"PROSPECT Error occurred: {e}")


# WIP
def prospect_backfill_key(func, local, start_ts):
    """
    Retrieves data from the Prospect API and updates the MySQL database.

    This function constructs a URL and fetches data from the Prospect API using the provided
    function to get the necessary URL and API key. It then retrieves the latest timestamp
    from the API response, queries the MySQL database for newer records, and sends this data
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

    # response = requests.request("GET", url, headers=headers, data=payload, verify=const.VERIFY)
    # if start_ts is None:
    #     start_ts = get_prospect_last_data(response)
    # j = json.loads(response.text)
    # # json.dumps(j, indent=2)
    logging.info(f"\n\n{key}\n{url}\n{start_ts}")

    res = mysql_execute(
        text(
            "select * FROM defaultdb.TAKUM_LEONICS_API_RAW where DatetimeServer > :data order by DatetimeServer limit 1450"
        ),
        start_ts,
    )
    # Fetch all results as a list of dictionaries
    rows = res.fetchall()

    # Convert the result to a Pandas DataFrame
    columns = res.keys()  # Get column names
    df = pd.DataFrame(rows, columns=columns)

    df["external_id"] = df["external_id"].astype(str).apply(lambda x: "py_" + x)

    res = api_prospect.api_in_prospect(df, local)
    if res is None:
        logging.error("Prospect API failed, exiting")
        exit()
    logging.info(f"{res.status_code}:  {res.text}")

    sts = start_ts.replace(" ", "_").replace(":", "HM")
    sts += str(local)
    df.to_csv(f"py_pros_{sts}.csv", index=False)

    # Save the DataFrame to a CSV file
    logger = logging.getLogger()
    if logger.getEffectiveLevel() < logging.INFO:
        sts = start_ts.replace(" ", "_").replace(":", "HM")
        sts += str(local)
        df.to_csv(f"py_pros_{sts}.csv", index=False)
        # df.to_json(f'py_pros_{sts}.json', index=False)

    logging.info(f"Data has been saved to 'py_pros'   LOCAL: {local}")


def get_prospect_last_data(response):
    """
    Retrieves the latest timestamp from the Prospect API response.

    This function takes a Prospect API response, parses it, and returns the latest timestamp
    as a string in the format 'YYYY-MM-DD HH:MM:SS'.

    Args:
        response (requests.Response): The Prospect API response.

    Returns:
        str: The latest timestamp.

    """

    j = json.loads(response.text)
    # json.dumps(j, indent=2)
    # logging.info(f'\n\n{j['data'][0]}')
    res = ""
    idd = ""
    for d in j["data"]:
        if d["custom"]["DatetimeServer"] > res:
            res = d["custom"]["DatetimeServer"]
        if d["external_id"] > idd:
            idd = d["external_id"]
    return res


def prospect_get_key(func, local, start_ts=None):
    """
    Retrieves data from the Prospect API and updates the MySQL database.

    This function constructs a URL and fetches data from the Prospect API using the provided
    function to get the necessary URL and API key. It then retrieves the latest timestamp
    from the API response, queries the MySQL database for newer records, and sends this data
    back to the Prospect API. If the API call fails, it logs an error and exits the program.

    Args:
        func (callable): A function that returns the API URL and key based on the 'local' flag.
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
    url, key = func(local, out=True)
    sid = 1 if local else 421
    url += f"/v1/out/custom/?size=50&page=1&q[source_id_eq]={sid}&q[s]=created_at+desc"
    payload = {}
    headers = {
        "Authorization": f"Bearer {key}",
    }

    response = requests.request(
        "GET", url, headers=headers, data=payload, verify=const.VERIFY
    )
    if start_ts is None:
        start_ts = get_prospect_last_data(response)
    j = json.loads(response.text)
    # json.dumps(j, indent=2)
    logging.info(f"\n\n{key}\n{url}\n{start_ts}")
    start_ts

    res = mysql_execute(
        text(
            "select * FROM defaultdb.TAKUM_LEONICS_API_RAW where DatetimeServer > :data order by DatetimeServer"
        ),
        start_ts,
    )
    # Fetch all results as a list of dictionaries
    rows = res.fetchall()

    # Convert the result to a Pandas DataFrame
    columns = res.keys()  # Get column names
    df = pd.DataFrame(rows, columns=columns)

    df["external_id"] = df["external_id"].astype(str).apply(lambda x: "py_" + x)

    res = api_prospect.api_in_prospect(df, local)
    if res is None:
        logging.error("Prospect API failed, exiting")
        exit()
    logging.info(f"{res.status_code}:  {res.text}")

    # Save the DataFrame to a CSV file
    logger = logging.getLogger()
    if logger.getEffectiveLevel() < logging.INFO:
        sts = start_ts.replace(" ", "_").replace(":", "HM")
        sts += str(local)
        df.to_csv(f"py_pros_{sts}.csv", index=False)
        # df.to_json(f'py_pros_{sts}.json', index=False)

    logging.info("Data has been saved to 'py_pros'")


###################################################
# Hey there - I've reviewed your changes and found some issues that need to be addressed.

# Blocking issues:

# The SQL generation method is vulnerable to SQL injection (e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:128)
# Overall Comments:

# The update_rows function uses string interpolation for SQL queries which creates a SQL injection vulnerability. Use parameterized queries instead of direct string interpolation.
# The ON DUPLICATE KEY UPDATE clause contains a hardcoded list of all columns which is brittle and hard to maintain. Consider generating this dynamically from the table schema.
# Here's what I looked at during the review
# 🟡 General issues: 2 issues found
# 🔴 Security: 1 blocking issue
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:61

# suggestion(performance): Consider adding connection pool management for database connections
#     )


# def mysql_execute(sql, data=None):
#     """Execute a SQL query against the Aiven MySQL database.

# Using SQLAlchemy's connection pooling could improve database connection efficiency and reduce overhead of creating new sessions for each query.

# Suggested implementation:

# import mysql.connector
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.exc import SQLAlchemyError
# from contextlib import contextmanager

# # Create a connection pool
# def get_mysql_engine(connection_string):
#     """Create a SQLAlchemy engine with connection pooling."""
#     return create_engine(
#         connection_string, 
#         pool_size=10,  # Adjust based on your needs
#         max_overflow=20,
#         pool_timeout=30,
#         pool_recycle=1800  # Recycle connections after 30 minutes
#     )

# @contextmanager
# def get_db_session(engine):
#     """Provide a transactional scope around a series of operations."""
#     Session = sessionmaker(bind=engine)
#     session = Session()
#     try:
#         yield session
#         session.commit()
#     except SQLAlchemyError:
#         session.rollback()
#         raise
#     finally:
#         session.close()

# def mysql_execute(sql, data=None, engine=None):
#     """Execute a SQL query against the Aiven MySQL database with connection pooling.

#     :param sql: SQL query to execute
#     :param data: Optional data for parameterized queries
#     :param engine: SQLAlchemy engine (optional, will use default if not provided)
#     :return: Query results

#     # If no engine is provided, raise an error or use a default
#     if engine is None:
#         raise ValueError("Database engine must be provided")

#     try:
#         with get_db_session(engine) as session:
#             # Use SQLAlchemy's execute method
#             result = session.execute(sql, params=data)

#             # If it's a SELECT query, fetch results
#             if sql.strip().upper().startswith('SELECT'):
#                 return result.fetchall()

#             # For INSERT, UPDATE, DELETE return the result
#             return result

# You'll need to modify how the database connection is configured. Instead of using mysql.connector.connect(), you should create a SQLAlchemy engine using a connection string.

# Update any code that calls mysql_execute() to pass the SQLAlchemy engine.

# Example of creating the engine:

# # In your configuration or initialization
# mysql_engine = get_mysql_engine('mysql+mysqlconnector://username:password@host:port/database')
# Example of calling the updated function:
# results = mysql_execute("SELECT * FROM users", engine=mysql_engine)
# Key improvements:

# Connection pooling reduces connection overhead
# Proper session management with commit/rollback
# Flexible error handling
# Support for parameterized queries
# Ability to handle different query types
# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:128

# issue(security): The SQL generation method is vulnerable to SQL injection
#         return None, e


# def update_rows(max_dt, df, table_name):
#     """
#     Updates the specified MySQL table with new data from a DataFrame.
# The current string concatenation method for SQL queries is risky. Use parameterized queries consistently instead of direct string interpolation.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:464

# suggestion(code_refinement): Hardcoded source ID could be a configuration issue
#     return res


# def prospect_get_key(func, local, start_ts=None):
#     """
#     Retrieves data from the Prospect API and updates the MySQL database.
# Consider moving hardcoded source IDs (1 and 421) to a configuration file or constants to improve maintainability.

# Suggested implementation:

# from decouple import config  # Assuming python-decouple for configuration

# # Define source IDs in a centralized location
# SOURCE_IDS = {
#     'default': config('PROSPECT_DEFAULT_SOURCE_ID', default=1, cast=int),
#     'specific': config('PROSPECT_SPECIFIC_SOURCE_ID', default=421, cast=int)
# }

# def prospect_get_key(func, local, start_ts=None):
#     """
#     Retrieves data from the Prospect API and updates the MySQL database.

#     Uses configurable source IDs to improve maintainability.

# Install python-decouple package (pip install python-decouple)
# Create a .env file in the project root with:
# PROSPECT_DEFAULT_SOURCE_ID=1
# PROSPECT_SPECIFIC_SOURCE_ID=421
# Update any code that previously used hardcoded source IDs to use SOURCE_IDS['default'] or SOURCE_IDS['specific']
# Add .env to .gitignore to prevent committing sensitive configuration
