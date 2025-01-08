"""
Overview
    This Python script db.py is responsible for extracting data from a Leonics API, storing it in a MySQL database, and subsequently 
    updating a Prospect system with new entries. It uses the requests library for API interaction, pandas for data manipulation, 
    and sqlalchemy for database operations. The script is designed to incrementally update both the database and Prospect, 
    minimizing redundant data transfer.

Key Components
    mysql_execute(sql, data=None): 
        This function provides a reusable interface for executing SQL queries against the MySQL database. It handles session creation, 
        execution, commit, rollback, and closure, simplifying database interactions within the script.

    update_mysql(token): 
        This function orchestrates the database update process. It retrieves the latest timestamp from the database, fetches new data 
        from the Leonics API since the last update, and inserts the new data into the database.

    update_rows(max_dt, token): 
        This function handles the core logic of fetching data from the Leonics API using the api.getData() function, filtering for 
        new records based on the provided max_dt timestamp, formatting the data, and constructing and executing a bulk INSERT SQL query.

    update_prospect(start_ts=None, local=True): 
        This function manages the Prospect update process. It retrieves the latest timestamp from Prospect, queries the database 
        for newer records, formats the data, and sends it to the Prospect API using api.api_in_prospect().

    prospect_get_key(func, local): 
        This function retrieves the necessary URL and API key for interacting with the Prospect API based on the local flag. 
        It also retrieves the latest timestamp from Prospect to avoid sending duplicate data.

    get_prospect_last_data(response): 
        This helper function parses the Prospect API response and extracts the latest timestamp to determine which records need to be updated. 
        It also extracts the latest external_id.

The script uses the requests library for API interaction, pandas for data manipulation, and sqlalchemy for database operations. 
It relies on constants defined in constants.py and API interaction functions from api.py. It also includes error handling and logging.
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
from unhcr import api

def mysql_execute(sql, data=None):
    """Execute a SQL query against the Aiven MySQL database.

    Args:
        sql (str): The SQL query to execute. May contain placeholders for data.
        data (dict): A dictionary containing values to substitute into the SQL query.

    Returns:
        sqlalchemy.engine.result.ResultProxy: The result of the query execution.

    Raises:
        Exception: If any error occurs while executing the query.
    """
    engine = create_engine(const.AIVEN_TAKUM_CONN_STR)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        result = session.execute(text(sql), {"data": data})
        session.commit()
        return result
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def update_mysql(token):
    """
    Updates the MySQL database by fetching the latest data from the Leonics API and inserting it.

    This function retrieves the most recent timestamp from the MySQL database and uses it to
    fetch new data entries from the Leonics API. The new data is then inserted into the database.

    Args:
        token (str): The authentication token required for accessing the Leonics API.

    Raises:
        Exception: If an error occurs during the retrieval of the database timestamp or during
        the update process, an error is logged and the program exits with a specific error code.
    """

    try:
        dt = mysql_execute('select max(DatetimeServer) FROM defaultdb.TAKUM_LEONICS_API_RAW')
        val = dt.scalar()
        max_dt = datetime.strptime(val, '%Y-%m-%d %H:%M')
    except Exception as e:
        logging.error(f'Can not get DB max timsestanp   {e}')
        exit(999)

    try:
        update_rows(max_dt, token)
    except Exception as e:
        traceback.print_exc()
        logging.error(f"?????????????????update_mysql Error occurred: {e}")
        exit()


def update_rows(max_dt, token):
    """
    Updates the MySQL database by fetching the latest data from the Leonics API and inserting it.

    This function retrieves the most recent timestamp from the MySQL database and uses it to
    fetch new data entries from the Leonics API. The new data is then inserted into the database.

    Args:
        max_dt (datetime): The most recent timestamp in the MySQL database.
        token (str): The authentication token required for accessing the Leonics API.

    Raises:
        Exception: If an error occurs during the retrieval of the database timestamp or during
        the update process, an error is logged and the program exits with a specific error code.
    """
    from unhcr import api
    st = (max_dt + timedelta(minutes=1)).date().isoformat()
    st = st.replace('-', '')
    ed = (datetime.now() + timedelta(days=1)).date().isoformat()
    ed = ed.replace('-', '')
    df = api.getData(start=st,end=ed,token=token)
    # Convert the 'datetime_column' to pandas datetime
    df['DateTimeServer'] = pd.to_datetime(df['DateTimeServer'])
    logging.debug(f"1111111  {df['DateTimeServer'].dtype}")
    # Define the threshold datetime
    threshold = pd.to_datetime(max_dt.isoformat())
    logging.debug(f'222222   {threshold.isoformat()}')
    # Filter rows where datetime_column is greater than or equal to the threshold
    df_filtered = df[df['DateTimeServer'] > threshold]
    logging.debug('3333333')
    # Convert the 'datetime_column' back to string format
    #####df_filtered['DateTimeServer'] = df_filtered['DateTimeServer'].dt.strftime('%Y-%m-%d %H:%M')

    df_filtered.loc[:, 'DateTimeServer'] = df_filtered['DateTimeServer'].dt.strftime('%Y-%m-%d %H:%M')
    logging.debug('44444444')

    # Generate a SQL bulk INSERT statement
    table_name = 'TAKUM_LEONICS_API_RAW'  # replace with your actual table name

    # Construct the SQL query
    columns = ', '.join(df_filtered.columns)  # Get the column names as comma-separated values
    logging.debug('555555555')
    values = ', '.join(
        #f"({', '.join(f'\'{val}\'' if isinstance(val, str) else str(val) for val in df_filtered.loc[idx])})"

        f"({', '.join(f'\'{val.strftime('%Y-%m-%d %H:%M')}\'' if isinstance(val, pd.Timestamp) else str(val) for val in df_filtered.loc[idx])})"
        for idx in df_filtered.index
    )
    logging.debug('66666666')

    logging.debug(f'222222  {df_filtered['DateTimeServer'].dtype}')

    values = values.replace("err", 'NULL')
    # Full MySQL INSERT statement
    sql_query = f"INSERT INTO {table_name} ({columns}) VALUES {values};"
    res = mysql_execute(sql_query)
    if isinstance(res, str):
        logging.error(f'ERROR update_mysql: {res}')
        exit(777)
    logging.info(f'ROWS UPDATED: {table_name}  {res.rowcount}')

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

    logging.info(f'Starting update_prospect ts: {start_ts}  local = {local}')
    try:
        prospect_get_key(api.get_prospect_url_key, local)
    except Exception as e:
        logging.error(f"PROSPECT Error occurred: {e}")


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
    res = ''
    idd = ''
    for d in j['data']:
        if(d['custom']['DatetimeServer'] > res):
            logging.debug('dddddd')
            res = d['custom']['DatetimeServer']
        if (d['external_id'] > idd):
            logging.debug(f'iiiiii {d['custom']['DatetimeServer']}  {d['external_id']}')
            idd = d['external_id']
    logging.debug(f'!!!!!!!!!!!!!!!!!!!!!!!!!!!  {res,idd}')
    return res
def prospect_get_key(func, local):
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
    url += f'/v1/out/custom/?size=50&page=1&q[source_id_eq]={sid}&q[s]=created_at+desc'
    payload = {}
    headers = {
    'Authorization': f'Bearer {key}',
    }
    logging.debug(f'!!!!!!!!!!!!!!!!!!!!!  {headers}')

    response = requests.request("GET", url, headers=headers, data=payload, verify=const.VERIFY)
    start_ts = get_prospect_last_data(response)
    j = json.loads(response.text)
    # json.dumps(j, indent=2)
    logging.info(f'\n\n{key}\n{url}\n{start_ts}')
    start_ts

    res = mysql_execute(
        "select * FROM defaultdb.TAKUM_LEONICS_API_RAW where DatetimeServer > :data order by DatetimeServer",
        start_ts,
    )
    # Fetch all results as a list of dictionaries
    rows = res.fetchall()

    # Convert the result to a Pandas DataFrame
    columns = res.keys()  # Get column names
    df = pd.DataFrame(rows, columns=columns)

    df['external_id'] = df['external_id'].astype(str).apply(lambda x: 'py_' + x)


    res = api.api_in_prospect(df, local)
    if res is None:
        logging.error('Prospect API failed, exiting')
        exit()
    logging.info(f'{res.status_code}:  {res.text}')

    # Save the DataFrame to a CSV file
    logger = logging.getLogger()
    if logger.getEffectiveLevel() < logging.INFO:
        sts = start_ts.replace(' ','_').replace(':', 'HM')
        sts += str(local)
        df.to_csv(f'py_pros_{sts}.csv', index=False)
        #df.to_json(f'py_pros_{sts}.json', index=False)

    logging.info("Data has been saved to 'py_pros'")

###################################################
# Hey there - I've reviewed your changes and found some issues that need to be addressed.

# Blocking issues:

# Remove SSL verification warning suppression (e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:44)
# Hardcoded Prospect API key found. (e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:252)
# Overall Comments:

# Security Risk: Disabling SSL verification (verify=False) makes the application vulnerable to man-in-the-middle attacks. Please fix the underlying SSL certificate issues rather than bypassing verification.
# The error handling strategy is inconsistent throughout the codebase - mixing logging+exit() with exception raising. Consider implementing a consistent error handling approach using proper exception handling with specific exception types.
# Here's what I looked at during the review
# 🟡 General issues: 1 issue found
# 🔴 Security: 2 blocking issues, 1 other issue
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:55

# suggestion(bug_risk): Improve error handling to avoid abrupt exits
# from unhcr import constants as const
# from unhcr import api

# def mysql_execute(sql, data=None):
#     """Execute a SQL query against the Aiven MySQL database.

# Instead of using exit(), consider raising custom exceptions or returning error status that can be handled by the caller. This allows for more flexible error management and prevents unexpected termination of the application.

# Suggested implementation:

# from unhcr import constants as const
# from unhcr import api
# import mysql.connector
# from mysql.connector import Error as MySQLError

# class DatabaseExecutionError(Exception):
#     """Custom exception for database execution errors."""
#     pass

# def mysql_execute(sql, data=None):
#     """Execute a SQL query against the Aiven MySQL database.

#     Args:
#         sql (str): SQL query to execute
#         data (tuple, optional): Parameters for parameterized query

#     Returns:
#         list: Query results if applicable

#     Raises:
#         DatabaseExecutionError: If there's an issue executing the database query
#     """
#     try:
#         # Establish database connection (assuming connection details are managed elsewhere)
#         connection = api.get_mysql_connection()

#         with connection.cursor() as cursor:
#             if data:
#                 cursor.execute(sql, data)
#             else:
#                 cursor.execute(sql)

#             # Commit for write operations, fetch for read operations
#             if sql.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
#                 connection.commit()
#                 return cursor.rowcount
#             elif sql.strip().upper().startswith('SELECT'):
#                 return cursor.fetchall()

#             return None

#     except MySQLError as e:
#         # Log the error (consider using a proper logging mechanism)
#         print(f"MySQL Error: {e}")
#         raise DatabaseExecutionError(f"Database query failed: {e}")

#     except Exception as e:
#         # Catch any unexpected errors
#         print(f"Unexpected error during database execution: {e}")
#         raise DatabaseExecutionError(f"Unexpected database error: {e}")

#     finally:
#         # Ensure connection is closed
#         if 'connection' in locals() and connection.is_connected():
#             connection.close()

# This implementation assumes there's an api.get_mysql_connection() method to retrieve database connection details. If this doesn't exist, you'll need to implement it or modify the connection logic.
# Add proper logging instead of print statements in a production environment.
# Ensure that the necessary MySQL connector library is installed (mysql-connector-python).
# The function now returns:
# rowcount for write operations (INSERT/UPDATE/DELETE)
# fetchall() results for SELECT queries
# None for other types of queries
# Custom DatabaseExecutionError allows callers to catch and handle database-specific errors
# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:44

# issue(security): Remove SSL verification warning suppression
# import traceback
# import pandas as pd
# import requests
# from urllib3.exceptions import InsecureRequestWarning
# import urllib3
# # Suppress InsecureRequestWarning
# Disabling SSL verification warnings is a critical security risk. Instead, properly configure SSL certificates or investigate the root cause of certificate validation failures.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:164

# issue(security): Use proper SQL parameter binding

#     logging.debug(f'222222  {df_filtered['DateTimeServer'].dtype}')

#     values = values.replace("err", 'NULL')
#     # Full MySQL INSERT statement
#     sql_query = f"INSERT INTO {table_name} ({columns}) VALUES {values};"
# Replacing 'err' with NULL via string replacement is error-prone. Use SQLAlchemy's parameter binding or type-based NULL handling to prevent potential data integrity issues.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:252

# issue(security): Hardcoded Prospect API key found.
#     url += f'/v1/out/custom/?size=50&page=1&q[source_id_eq]={sid}&q[s]=created_at+desc'
#     payload = {}
#     headers = {
#     'Authorization': f'Bearer {key}',
#     }
#     logging.debug(f'!!!!!!!!!!!!!!!!!!!!!  {headers}')
# The Prospect API key is hardcoded in the headers dictionary. Store sensitive information like API keys securely, such as in environment variables or a dedicated secrets management service, and access them within the application. Avoid committing secrets directly into the codebase.
