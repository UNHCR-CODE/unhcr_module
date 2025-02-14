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


# if ORACLE:
#     try:
#         orc_table = 'ORC_TAKUM_LEONICS_API_RAW'

#         match = re.search(r'INSERT INTO (\w+) \((.*?)\) VALUES', s)
#         table_name = match[1]
#         columns = match[2]
#         columns = '"' + columns.replace(', ','", "') + '"'

#         # Extract individual rows
#         rows = re.findall(r'\((.*?)\)', s)

#         # Format for Oracle INSERT ALL
#         oracle_insert = f"INSERT ALL\n"
#         for x, row in enumerate(rows):
#             if x != 0:
#                 oracle_insert += f"    INTO {table_name} ({columns}) VALUES ({row})\n"
#         oracle_insert += "SELECT * FROM dual;"

#         with open("orc_sql.txt", "w") as file:
#             file.write(oracle_insert)
#     except Exception as e:
#         logging.error(f"ORACLE Error occurred: {e}")


def execute(token, start_ts=None):
    if UPDATE_DB or PROSPECT:
        logging.debug('Retrieved Leonics token')
    if UPDATE_DB:
        start_ts = db.update_takum_raw_db(token, start_ts)
    if PROSPECT:
        # db.default_engine, const.LEONICS_RAW_TABLE = db.set_db_engine_by_name('mysql')
        # res, err = db.update_prospect(local=False)
        # assert(res is not None)
        # assert(err is None)
        # logging.info(f"LOCAL: FALSE {res.status_code}:  {res.text}")
        db.default_engine, const.LEONICS_RAW_TABLE = db.set_db_engine_by_name('postgresql')
        if BACKFILL_DT:
            db.update_prospect(start_ts=start_ts) #AZURE
        else:
            db.update_prospect(table_name=const.LEONICS_RAW_TABLE) #AZURE

        url = "http://localhost:3000"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logging.info(f"Server at {url} is responding. Status code: {response.status_code}")
                db.default_engine, const.LEONICS_RAW_TABLE = db.set_db_engine_by_name('postgresql')
                res, err = db.update_prospect(start_ts=start_ts, local=True, table_name= const.LEONICS_RAW_TABLE)
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

token = api_leonics.checkAuth()
assert(token is not None)

execute(token)
exit(0)
