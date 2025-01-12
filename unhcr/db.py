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
        This function handles the core logic of fetching data from the Leonics API using the api_leonics.getData() function, filtering for 
        new records based on the provided max_dt timestamp, formatting the data, and constructing and executing a bulk INSERT SQL query.

    update_prospect(start_ts=None, local=True): 
        This function manages the Prospect update process. It retrieves the latest timestamp from Prospect, queries the database 
        for newer records, formats the data, and sends it to the Prospect API using api_prospect.api_in_prospect().

    prospect_get_key(func, local): 
        This function retrieves the necessary URL and API key for interacting with the Prospect API based on the local flag. 
        It also retrieves the latest timestamp from Prospect to avoid sending duplicate data.

    get_prospect_last_data(response): 
        This helper function parses the Prospect API response and extracts the latest timestamp to determine which records need to be updated. 
        It also extracts the latest external_id.

The script uses the requests library for API interaction, pandas for data manipulation, and sqlalchemy for database operations. 
It relies on constants defined in constants.py and API interaction functions from api_leonics.py and api_prospect.py. 
It also includes error handling and logging.
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

if const.LOCAL: # testing with local python files
    const, api_prospect, *rest = const.import_local_libs(mods=[["constants", "const"], ["api_prospect", "api_prospect"]])
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

def get_mysql_max_date():

    """Retrieves the most recent timestamp from the MySQL database.

    Returns:
        tuple: A tuple containing the most recent timestamp in the database and an error message if any.
    """
    try:
        dt = mysql_execute('select max(DatetimeServer) FROM defaultdb.TAKUM_LEONICS_API_RAW')
        val = dt.scalar()
        return datetime.strptime(val, '%Y-%m-%d %H:%M'), None
    except Exception as e:
        logging.error(f'Can not get DB max timsestanp   {e}')
        return None, e

def update_mysql(max_dt, df):

    """
    Updates the MySQL database with new data entries.
    This function retrieves the latest timestamp from the MySQL database and updates
    it with new data entries from a DataFrame. It logs any errors encountered during
    the process.

    Args:
        max_dt (datetime): The maximum datetime to be used as a threshold for filtering new data.
        df (pandas.DataFrame): The DataFrame containing new data to be inserted into the database.
    Returns:
        tuple: A tuple containing the most recent timestamp in the database and an error message if any.
    """

    try:
        return update_rows(max_dt, df)
    except Exception as e:
        traceback.print_exc()
        logging.error(f"?????????????????update_mysql Error occurred: {e}")
        return None, e

def update_rows(max_dt, df):
    """
    Updates the MySQL database with new data entries.

    This function filters the input DataFrame to contain only rows where the 'DateTimeServer'
    column is greater than or equal to the input max_dt. It then converts the 'DateTimeServer'
    column back to string format and generates a SQL bulk INSERT statement. The statement is
    then executed against the MySQL database.

    Args:
        max_dt (datetime): The maximum datetime to be used as a threshold for filtering new data.
        df (pandas.DataFrame): The DataFrame containing new data to be inserted into the database.

    Returns:
        tuple: A tuple containing the result of the update operation and an error message, if any.
    """

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
        return None, 'Error: query result is not a string'
    logging.info(f'ROWS UPDATED: {table_name}  {res.rowcount}')
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

    logging.info(f'Starting update_prospect ts: {start_ts}  local = {local}')
    try:
        prospect_get_key(api_prospect.get_prospect_url_key, local)
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


    res = api_prospect.api_in_prospect(df, local)
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

# Direct string replacement for NULL values is error-prone (e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:166)
# Overall Comments:

# The update_rows() function is vulnerable to SQL injection attacks. Use parameterized queries instead of string concatenation when building the SQL statement.
# Add proper connection error handling in mysql_execute() to gracefully handle database connectivity issues.
# Here's what I looked at during the review
# 🟡 General issues: 2 issues found
# 🔴 Security: 1 blocking issue
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:164

# suggestion(performance): Excessive debug logging might impact performance
#     )
#     logging.debug('66666666')

#     logging.debug(f'222222  {df_filtered['DateTimeServer'].dtype}')

#     values = values.replace("err", 'NULL')
# Remove or conditionally include debug logging statements in production code to minimize overhead.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:166

# issue(security): Direct string replacement for NULL values is error-prone

#     logging.debug(f'222222  {df_filtered['DateTimeServer'].dtype}')

#     values = values.replace("err", 'NULL')
#     # Full MySQL INSERT statement
#     sql_query = f"INSERT INTO {table_name} ({columns}) VALUES {values};"
# Use proper SQL parameter binding or database-specific NULL handling to prevent potential SQL injection or data corruption.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/db.py:277

# suggestion(performance): Inefficient external_id transformation
#     columns = res.keys()  # Get column names
#     df = pd.DataFrame(rows, columns=columns)

#     df['external_id'] = df['external_id'].astype(str).apply(lambda x: 'py_' + x)


# Consider using vectorized operations like df['external_id'] = 'py_' + df['external_id'].astype(str) for better performance.
