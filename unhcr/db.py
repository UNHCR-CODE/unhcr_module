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

The script uses several constants defined in a separate constants.py file, including database connection string and API endpoints. 
It also relies on functions from a separate api.py file for interacting with the Leonics and Prospect APIs. The script includes 
error handling and logging to facilitate debugging and monitoring.
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

import constants as const
import api

def mysql_execute(sql, data=None):
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


# TODO Rename this here and in `update_mysql`
def update_rows(max_dt, token):
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
    logging.info(f'Starting update_prospect ts: {start_ts}  local = {local}')
    try:
        prospect_get_key(api.get_prospect_url_key, local)
    except Exception as e:
        logging.error(f"PROSPECT Error occurred: {e}")


def get_prospect_last_data(response):
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
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# Security concern: Disabling SSL verification (verify=False) is dangerous as it makes the application vulnerable to man-in-the-middle attacks. Consider fixing the SSL certificate issues properly instead.
# Error handling is inconsistent throughout the code - mixing logging+exit with exception raising. Consider implementing a consistent error handling strategy, preferably using proper exception handling with specific exception types.
# Here's what I looked at during the review
# 🟡 General issues: 3 issues found
# 🟡 Security: 1 issue found
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:_UNHCR\CODE\unhcr_module\unhcr\db.py:33

# suggestion(bug_risk): Improve error handling to avoid abrupt exits
#     finally:
#         session.close()

# def update_mysql(token):
#     try:
#         dt = mysql_execute('select max(DatetimeServer) FROM defaultdb.TAKUM_LEONICS_API_RAW')
# Instead of using exit(), consider raising custom exceptions or returning error status that can be handled by the caller. This allows for more flexible error management and prevents unexpected termination of the application.

# Resolve
# outdated
# e:_UNHCR\CODE\unhcr_module\unhcr\db.py:70

# suggestion(performance): Potential performance issue with datetime conversion
#     # Convert the 'datetime_column' back to string format
#     #####df_filtered['DateTimeServer'] = df_filtered['DateTimeServer'].dt.strftime('%Y-%m-%d %H:%M')

#     df_filtered.loc[:, 'DateTimeServer'] = df_filtered['DateTimeServer'].dt.strftime('%Y-%m-%d %H:%M')
#     logging.info('44444444')
#     # logging.info(df_filtered['DateTimeServer'].dtype)
# Multiple datetime conversions could be inefficient. Consider consolidating datetime transformations to minimize overhead and improve readability.

# Resolve
# e:_UNHCR\CODE\unhcr_module\unhcr\db.py:93

# issue(security): Risky string replacement for NULL values
#     logging.info(values[-40:])
#     logging.info('222222',df_filtered['DateTimeServer'].dtype)

#     values = values.replace("err", 'NULL')
#     # Full MySQL INSERT statement
#     sql_query = f"INSERT INTO {table_name} ({columns}) VALUES {values};"
# Replacing 'err' with NULL is error-prone. Use proper SQL parameter binding or type-based NULL handling to prevent potential data integrity issues.

# Resolve
# outdated
# e:_UNHCR\CODE\unhcr_module\unhcr\db.py:60

# suggestion(code_refinement): Remove debug logging statements
#     df = api.getData(start=st,end=ed,token=token)
#     # Convert the 'datetime_column' to pandas datetime
#     df['DateTimeServer'] = pd.to_datetime(df['DateTimeServer'])
#     logging.info('1111111',df['DateTimeServer'].dtype)
#     # Define the threshold datetime
#     threshold = pd.to_datetime(max_dt.isoformat())
# Numbered logging statements with sequential numbers suggest temporary debugging. These should be removed or replaced with meaningful log messages before production.
