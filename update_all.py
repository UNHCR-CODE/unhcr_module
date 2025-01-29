"""
Overview
    This script full_test.py integrates data from the Leonics API into UNHCR's MySQL and Prospect databases. 
    It authenticates with the Leonics API, retrieves data, and updates the databases based on conditional flags. 
    The script includes some commented-out code suggesting past or future integration with an Oracle database and AWS S3. 
    It appears designed for testing the integration process.

Key Components
    Authentication (api_leonics.checkAuth()): 
        Retrieves a token from the Leonics API, essential for subsequent database interactions. 
        The script exits if authentication fails.

    DB Update (db.update_leonics_db(...)): 
        Updates the Leonics Raw database with data fetched from the Leonics API. 
        Uses a timestamp to determine the range of data to fetch.

    Prospect Update (db.update_prospect(...)): 
        Updates the Prospect database. A local flag suggests different update paths for testing and production environments.
        Hardcoded timestamps are likely placeholders for testing.

    Conditional Logic: Boolean flags (UPDATE_DB, PROSPECT, ORACLE) 
        control which databases are updated, enabling targeted testing.

    Logging: 
        Uses the logging module to record key events and errors, aiding in debugging and monitoring.

    Configuration: 
        Uses a constants module (const) to manage environment-specific settings, such as database credentials 
        and API endpoints. Supports both local and production environments.

    Inactive Code: 
        Contains commented-out code related to Oracle and S3 integration, indicating potential future development or 
        removed features. This code should be removed for clarity.

    Versioning: 
        Includes a function (utils.get_module_version) to retrieve the module's version, useful for tracking 
        changes and deployments.
"""

from datetime import datetime, timedelta
import logging
import os
import pandas as pd
import re
import requests


from unhcr import constants as const
# OPTIONAL: set your own environment
##ef = const.load_env(r'E:\_UNHCR\CODE\unhcr_module\.env')
## print(ef)
# OPTIONAL: set your own environment
from unhcr import utils
from unhcr import db
from unhcr import api_leonics


#if const.LOCAL: # testing with local python files
mods = const.import_local_libs(mods=[["utils","utils"], ["constants", "const"], ["db", "db"], ["api_leonics", "api_leonics"]])
utils, const, db, api_leonics, *rest = mods

utils.log_setup(override=True)
logging.info(f"PROD: {const.PROD}, DEBUG: {const.DEBUG}, LOCAL: {const.LOCAL} {os.getenv('LOCAL')} .env file @: {const.environ_path}")

# just to test S3
# TODO waiting for new creds
# import s3
# # Call the function to list files in the 'Archive' folder
# s3.list_files_in_folder(s3.BUCKET_NAME, s3.FOLDER_NAME)
# exit()

logging.info(f"Process ID: {os.getpid()}   Log Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}")
ver, err = utils.get_module_version()
logging.info(f"Version: {ver}   Error: {err}")

BACKFILL_DT = False
start_ts = None
UPDATE_DB = True
PROSPECT = True
ORACLE = False

def set_db_engine(ename):
    #print(const.TAKUM_RAW_CONN_STR, const.LEONICS_RAW_TABLE)
    ### set to AZURE
    if db.default_engine.engine.name != ename:
        if ename == 'postgresql':
            const.TAKUM_RAW_CONN_STR =  os.getenv('AZURE_TAKUM_LEONICS_API_RAW_CONN_STR','xxxxxx')
            const.LEONICS_RAW_TABLE = os.getenv('AZURE_LEONICS_RAW_TABLE','pppppp')
        else:
            const.TAKUM_RAW_CONN_STR =  os.getenv('AIVEN_TAKUM_LEONICS_API_RAW_CONN_STR','zzzzz')
            const.LEONICS_RAW_TABLE = os.getenv('LEONICS_RAW_TABLE','qqqqq')
    return db.set_db_engine(const.TAKUM_RAW_CONN_STR)

if ORACLE:
    try:
        orc_table = 'ORC_TAKUM_LEONICS_API_RAW'

        match = re.search(r'INSERT INTO (\w+) \((.*?)\) VALUES', s)
        table_name = match[1]
        columns = match[2]
        columns = '"' + columns.replace(', ','", "') + '"'

        # Extract individual rows
        rows = re.findall(r'\((.*?)\)', s)

        # Format for Oracle INSERT ALL
        oracle_insert = f"INSERT ALL\n"
        for x, row in enumerate(rows):
            if x != 0:
                oracle_insert += f"    INTO {table_name} ({columns}) VALUES ({row})\n"
        oracle_insert += "SELECT * FROM dual;"

        with open("orc_sql.txt", "w") as file:
            file.write(oracle_insert)
    except Exception as e:
        logging.error(f"ORACLE Error occurred: {e}")


def set_date_range(st_dt, num_days):
    st = (st_dt + timedelta(minutes=1)).date().isoformat()
    st = st.replace('-', '')
    ed = datetime.now() + timedelta(days=num_days)
    if BACKFILL_DT:
        ed = st_dt + timedelta(days=num_days)
    # Leonics API has a limit of 10 days
    if ed - st_dt > timedelta(days=10):
        ed = st_dt + timedelta(days=10)
    ed = ed.date().isoformat()
    start_ts = ed
    ed = ed.replace('-', '')
    return st, ed

def execute(token, start_ts=None):
    if UPDATE_DB or PROSPECT:
        logging.debug('Retrieved Leonics token')
    if UPDATE_DB:
        start_ts = update_db(token, start_ts)
    if PROSPECT:
        # db.default_engine = set_db_engine('mysql')
        # res, err = db.update_prospect(local=False)
        # assert(res is not None)
        # assert(err is None)
        # logging.info(f"LOCAL: FALSE {res.status_code}:  {res.text}")
        db.default_engine = set_db_engine('postgresql')
        if BACKFILL_DT:
            db.update_prospect(start_ts=start_ts) #AZURE
        else:
            db.update_prospect() #AZURE


        url = "http://localhost:3000"

        try:
            response = requests.get(url)
            if response.status_code == 200:
                logging.info(f"Server at {url} is responding. Status code: {response.status_code}")
                db.default_engine = set_db_engine('postgresql')
                res, err = db.update_prospect(start_ts=start_ts, local=True)
                assert(res is not None)
                assert(err is None)
                logging.info(f"LOCAL: TRUE {res.status_code}:  {res.text}")
            else:
                logging.info(f"Server at {url} responded with status code: {response.status_code}")
                start_ts = None
        except requests.ConnectionError:
            logging.error(f"Server at {url} is not responding.")
            start_ts = None
    return start_ts

# TODO Rename this here and in `execute`
def update_db(token, start_ts):
    num_days = 2
    max_dt = start_ts
    if start_ts is None:
        db.default_engine = set_db_engine('postgresql')
        max_dt1, err = db.get_db_max_date(db.default_engine, const.LEONICS_RAW_TABLE)
        if err:
            logging.error(f"get_db_max_date Error occurred: {err}")
            exit(1)
        assert(max_dt1 is not None)
        db.default_engine = set_db_engine('mysql')
        max_dt, err = db.get_db_max_date(db.default_engine, const.LEONICS_RAW_TABLE)
        if err:
            logging.error(f"get_db_max_date1 Error occurred: {err}")
            exit(1)
        assert(max_dt is not None)

        # backfill 1 week
        #max_dt = max_dt - timedelta(days=7)
    st, ed = set_date_range(max_dt, num_days)
    db.default_engine = set_db_engine('mysql')
    df_leonics, err = api_leonics.getData(start=st,end=ed,token=token)
    if err:
        logging.error(f"api_leonics.getData Error occurred: {err}")
        exit(2)
    # Convert the 'datetime_column' to pandas datetime
    df_leonics['DatetimeServer'] = pd.to_datetime(df_leonics['DatetimeServer'])
    res, err = db.update_leonics_db(max_dt,df_leonics, const.LEONICS_RAW_TABLE)
    if err:
        logging.error(f"update_leonics_db Error occurred: {err}")
        exit(3)
    else:
        logging.info(f'ROWS UPDATED: {const.LEONICS_RAW_TABLE}  {res.rowcount}')

    db.default_engine = set_db_engine('postgresql')

    st, ed = set_date_range(max_dt1, num_days)
    db.default_engine = set_db_engine('postgresql')
    df_azure, err = api_leonics.getData(start=st,end=ed,token=token)
    if err:
        logging.error(f"api_leonics.getData Error occurred: {err}")
        exit(2)
    # Convert the 'datetime_column' to pandas datetime
    df_azure['DatetimeServer'] = pd.to_datetime(df_azure['DatetimeServer'])
    #df_azure['datetimeserver'] = df_leonics['DatetimeServer'].copy()
    #df_azure = df_azure.drop(columns=['DatetimeServer'])
    # we use all lowercase column names in AZURE
    df_azure.columns = df_azure.columns.str.lower()
    res, err = db.update_leonics_db(max_dt1,df_azure, const.LEONICS_RAW_TABLE, 'datetimeserver')
    assert(res is not None)
    assert(err is None)
    if err:
        logging.error(f"update_leonics_db Error occurred: {err}")
        exit(3)
    else:
        logging.info(f'ROWS UPDATED: {const.LEONICS_RAW_TABLE}  {res.rowcount}')
        
    return start_ts

token = api_leonics.checkAuth()
assert(token is not None)


    # if const.LOCAL:
    #     conn_str =const.PROSPECT_CONN_LOCAL_STR
    # else:
    #     conn_str =const.PROSPECT_CONN_AZURE_STR

execute(token)
exit(0)

#### This was to take AZURE takum_api_raw_data table data and update local Prospect
#### TODO: make this general purpose ---- note it has to wait for the Prospect ingestion 
# before looping to get the next start_ts --- pretty much have to check ingestion is complete, or do not loop
# it is an edge case if multiple scripts are updating the DB


#### Necessary when data_custom has no data --- start at oldest data
# db.prospect_engine = set_db_engine('postgresql')
# start_ts = datetime.strptime('2024-09-28 00:00', "%Y-%m-%d %H:%M")
# execute(token,start_ts)
# db.default_engine = set_db_engine('postgresql')


#db.prospect_engine = db.set_db_engine(const.PROS_CONN_AZURE_STR)

# while True:
#     db.prospect_engine = db.set_db_engine(const.PROS_CONN_LOCAL_STR)
#     sql = "select custom->>'datetimeserver', external_id from data_custom where external_id like 'sys_%' order by custom->>'datetimeserver' desc limit 1"
#     dt, err = db.sql_execute(sql, db.prospect_engine)
#     assert err is None
#     dt = dt.fetchall()
#     val = dt[0][0]
#     start_ts = datetime.strptime(val, "%Y-%m-%d %H:%M")
#     execute(token,start_ts)
#     pass

################################################################
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# Consider moving the boolean flags (UPDATE_DB, PROSPECT, ORACLE) into a configuration file or environment variables for better maintainability and flexibility.
# Remove commented-out code related to S3 and Oracle to improve code clarity. If these features are planned for future implementation, track them in issues/tickets instead.
# Replace assertions with proper error handling that includes descriptive error messages, and consider defining exit codes as named constants.
# Here's what I looked at during the review
# 🟢 General issues: all looks good
# 🟢 Security: all looks good
# 🟡 Testing: 3 issues found
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:113

# issue(testing): Missing edge case tests for `api_leonics.getData()`.
#             logging.error(f"get_db_max_date Error occurred: {err}")
#             exit(1)
#         assert(max_dt is not None)
#         st = (max_dt + timedelta(minutes=1)).date().isoformat()
#         st = st.replace('-', '')
#         ed = (datetime.now() + timedelta(days=1)).date().isoformat()
#         ed = ed.replace('-', '')
#         df, err = api_leonics.getData(start=st,end=ed,token=token)
#         if err:
# Consider adding tests for api_leonics.getData() with invalid or empty start/end dates, or cases where the date range results in no data. This ensures the function handles various scenarios gracefully.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:123

# issue(testing): Test `db.update_leonics_db` failure scenarios.
#             exit(2)
#         # Convert the 'datetime_column' to pandas datetime
#         df['DatetimeServer'] = pd.to_datetime(df['DatetimeServer'])
#         res, err = db.update_leonics_db(max_dt,df, const.LEONICS_RAW_TABLE)
#         assert(res is not None)
#         assert(err is None)
#         if err:
#             logging.error(f"update_leonics_db Error occurred: {err}")
# Include tests where db.update_leonics_db encounters errors, such as database connection issues or data integrity violations. Verify that errors are properly logged and handled.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:122

# issue(testing): Add test for DatetimeServer format variations.
#             logging.error(f"api_leonics.getData Error occurred: {err}")
#             exit(2)
#         # Convert the 'datetime_column' to pandas datetime
#         df['DatetimeServer'] = pd.to_datetime(df['DatetimeServer'])
#         res, err = db.update_leonics_db(max_dt,df, const.LEONICS_RAW_TABLE)
#         assert(res is not None)
# Include a test case where the 'DatetimeServer' column in the dataframe has a different format or contains invalid date/time values. This ensures the script handles potential data inconsistencies gracefully.

