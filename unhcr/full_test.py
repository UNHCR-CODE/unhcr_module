"""
Overview
    This Python script full_test.py orchestrates the process of fetching data from an API and updating two databases: 
    MySQL and Prospect. 
    It appears to be a test script for integrating data from the Leonics API into UNHCR's systems. 
    The script first authenticates with the Leonics API, then uses the retrieved token to update the databases. 
    It includes conditional logic to control which databases are updated based on boolean flags. 
    The script also contains commented-out code suggesting prior or planned integration with an Oracle database and AWS S3.

Key Components
    Authentication (api_leonics.checkAuth()): 
        This function is crucial as it retrieves a token from the Leonics API, which is required for subsequent database updates.
        The script exits if authentication fails.
    MySQL Update (db.update_mysql(token)): 
        This function updates a MySQL database using the token obtained from the Leonics API. 
        The specifics of the update process are not shown in this file.
    Prospect Update (db.update_prospect(start_ts, local)): 
        This function updates the Prospect database. It takes a start_ts parameter (likely a timestamp) and a local boolean flag, 
        suggesting different update paths depending on the context. 
        The hardcoded timestamp '2090-11-14' likely serves as a placeholder for testing purposes.
    Conditional Logic: 
        The script uses boolean variables (mysql, pros, orc) to control which parts of the update process are executed. 
        This allows for flexible testing of individual components.
    Error Handling: 
        Basic error handling is implemented for the Oracle integration (although currently inactive), demonstrating an awareness of potential issues.
        However, more robust error handling might be beneficial for the active MySQL and Prospect update processes.
    Circular Import Prevention: 
        The commented-out lines related to sys.path manipulation indicate an attempt to address potential circular import issues, 
        a common challenge in larger Python projects.
    Local vs. Production Imports: 
        The commented-out import statements suggest the script can be run with either local or production versions of the db and api modules. 
        This is a common pattern for facilitating development and testing.
"""

from datetime import datetime, timedelta
import logging
import re
import os
import pandas as pd

from unhcr import utils
utils.log_setup()

from unhcr import constants as const
from unhcr import db
from unhcr import api_leonics

# just to test S3
# TODO waiting for new creds
# import s3
# # Call the function to list files in the 'Archive' folder
# s3.list_files_in_folder(s3.BUCKET_NAME, s3.FOLDER_NAME)
# exit()

logging.info(f"Process ID: {os.getpid()}   Log Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}")
ver, err = utils.get_module_version()
logging.info(f"Version: {ver}   Error: {err}")

mysql = True
pros = True
orc = False

if orc:
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

logging.debug('1111111111111111111111111')
token = None
if mysql or pros:
    logging.debug('222222222222222222222')
    token = api_leonics.checkAuth()
    logging.debug('999999999999999999999999')

logging.debug(f'!!!!!!!!!!! {token}')
if token:
    if mysql:
        max_dt, err = db.get_mysql_max_date()
        if err:
            logging.error(f"get_mysql_max_date Error occurred: {err}")
            exit(1)
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
        db.update_mysql(max_dt,df)
else:
    logging.info('Failed to get Leonics token, exiting')
    exit()

if pros:
    # set start_time to highest DateTimeServer in Prospect
    # TODO get from out API
    db.update_prospect(start_ts='2090-11-14 01:52', local=True)
    db.update_prospect(start_ts='2090-11-14 01:48', local=False)

################################################################
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# Replace numbered debug logging statements (111111, 222222, etc.) with meaningful log messages that describe what's happening at each step. This will make troubleshooting much easier.
# Consider adding proper error handling around the database update operations. Currently only the authentication failure is handled, but other operations could also fail.
# Here's what I looked at during the review
# 🟢 General issues: all looks good
# 🟢 Security: all looks good
# 🟡 Testing: 5 issues found
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:69

# suggestion(testing): Hardcoded test parameters
# logging.info(f"Version: {ver}   Error: {err}")


# mysql = True
# pros = True
# orc = False

# The boolean flags mysql, pros, and orc are hardcoded. This limits the test coverage. Consider using test parameters or environment variables to control these flags, allowing for testing different combinations.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:106

# issue(testing): Missing assertions for `db.update_mysql()`

# logging.debug(f'!!!!!!!!!!! {token}')
# if token:
#     if mysql:
#         db.update_mysql(token)
# else:
#     logging.info('Failed to get Leonics token, exiting')
# The test calls db.update_mysql(token) but doesn't assert anything about the result. Add assertions to verify that the MySQL database is updated correctly. For example, check if specific rows are added or modified.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:115

# issue(testing): Missing assertions and unclear purpose of hardcoded timestamps
# if pros:
#     # set start_time to highest DateTimeServer in Prospect
#     # TODO get from out API
#     db.update_prospect(start_ts='2090-11-14 01:52', local=True)
#     db.update_prospect(start_ts='2090-11-14 01:48', local=False)

# ################################################################
# Similar to the MySQL update, this test lacks assertions. It's unclear why specific timestamps are hardcoded, especially so far in the future. Clarify the purpose of these timestamps and add assertions to verify the correct behavior of db.update_prospect() for both local=True and local=False scenarios.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:101

# issue(testing): Missing test for authentication failure
# token = None
# if mysql or pros:
#     logging.debug('222222222222222222222')
#     token = api_leonics.checkAuth()
#     logging.debug('999999999999999999999999')

# The test assumes api_leonics.checkAuth() will always succeed. Add a test case to simulate an authentication failure and verify that the script handles it gracefully, likely by logging an error and exiting.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:74

# suggestion(testing): Inactive Oracle code should be removed
# orc = False

# if orc:
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

# logging.debug('1111111111111111111111111')
# The entire if orc block is inactive and seems related to Oracle integration, which is currently disabled. Remove this dead code to improve readability and maintainability.
