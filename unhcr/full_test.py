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

    MySQL Update (db.update_mysql(...)): 
        Updates the MySQL database with data fetched from the Leonics API. 
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
import re
import os
import pandas as pd

from unhcr import constants as const
# OPTIONAL: set your own environment
##ef = const.load_env(r'E:\_UNHCR\CODE\unhcr_module\.env')
## print(ef)
# OPTIONAL: set your own environment
from unhcr import utils
from unhcr import db
from unhcr import api_leonics


if const.LOCAL: # testing with local python files
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

UPDATE_DB = True
PROSPECT = True
ORACLE = False

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

if UPDATE_DB or PROSPECT:
    token = api_leonics.checkAuth()
    assert(token is not None)

if token:
    logging.debug(f'Retrieved Leonics token')
    if UPDATE_DB:
        max_dt, err = db.get_mysql_max_date()
        if err:
            logging.error(f"get_mysql_max_date Error occurred: {err}")
            exit(1)
        assert(max_dt is not None)
        st = (max_dt + timedelta(minutes=1)).date().isoformat()
        st = st.replace('-', '')
        ed = (datetime.now() + timedelta(days=1)).date().isoformat()
        ed = ed.replace('-', '')
        df, err = api_leonics.getData(start=st,end=ed,token=token)
        if err:
            logging.error(f"api_leonics.getData Error occurred: {err}")
            exit(2)
        # Convert the 'datetime_column' to pandas datetime
        df['DateTimeServer'] = pd.to_datetime(df['DateTimeServer'])
        res, err = db.update_mysql(max_dt,df, const.LEONICS_RAW_TABLE)
        assert(res is not None)
        assert(err is None)
        if err:
            logging.error(f"update_mysql Error occurred: {err}")
            exit(3)
        else:
            logging.info(f'ROWS UPDATED: {const.LEONICS_RAW_TABLE}  {res.rowcount}')
else:
    logging.info('Failed to get Leonics token, exiting')
    exit()

if PROSPECT:
    # set start_time to highest DateTimeServer in Prospect
    # does AZURE
    ######db.update_prospect()
    # TODO get from out API
    res, err = db.update_prospect(local=True)
    assert(res is not None)
    assert(err is None)
    logging.info(f"LOCAL: TRUE {res.status_code}:  {res.text}")
    res, err = db.update_prospect(local=False)
    assert(res is not None)
    assert(err is None)
    logging.info(f"LOCAL: FALSE {res.status_code}:  {res.text}")

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
#             logging.error(f"get_mysql_max_date Error occurred: {err}")
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

# issue(testing): Test `db.update_mysql` failure scenarios.
#             exit(2)
#         # Convert the 'datetime_column' to pandas datetime
#         df['DateTimeServer'] = pd.to_datetime(df['DateTimeServer'])
#         res, err = db.update_mysql(max_dt,df, const.LEONICS_RAW_TABLE)
#         assert(res is not None)
#         assert(err is None)
#         if err:
#             logging.error(f"update_mysql Error occurred: {err}")
# Include tests where db.update_mysql encounters errors, such as database connection issues or data integrity violations. Verify that errors are properly logged and handled.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:122

# issue(testing): Add test for DateTimeServer format variations.
#             logging.error(f"api_leonics.getData Error occurred: {err}")
#             exit(2)
#         # Convert the 'datetime_column' to pandas datetime
#         df['DateTimeServer'] = pd.to_datetime(df['DateTimeServer'])
#         res, err = db.update_mysql(max_dt,df, const.LEONICS_RAW_TABLE)
#         assert(res is not None)
# Include a test case where the 'DateTimeServer' column in the dataframe has a different format or contains invalid date/time values. This ensures the script handles potential data inconsistencies gracefully.

