"""
Overview
    This Python script full_test.py orchestrates the process of fetching data from an API and updating two databases: 
    MySQL and Prospect. 
    It appears to be a test script for integrating data from the Leonics API into UNHCR's systems. 
    The script first authenticates with the Leonics API, then uses the retrieved token to update the databases. 
    It includes conditional logic to control which databases are updated based on boolean flags. 
    The script also contains commented-out code suggesting prior or planned integration with an Oracle database and AWS S3.

Key Components
    Authentication (api.checkAuth()): 
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
    Commented-out Code: 
        The presence of commented-out code blocks related to Oracle and AWS S3 suggests potential future integrations or functionalities 
        that are not currently active. 
        This commented-out code provides valuable context for understanding the evolution and potential future direction of the system.
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

import logging
import re
import os

# circular issues
# import sys
# sys.path.append('E:/_UNHCR/CODE/unhcr_module/unhcr/')

from unhcr import db
from unhcr import api
from unhcr import utils

# test local
#import db
#import api
#import utils

# just to test S3
# TODO waiting for new creds
# import s3
# # Call the function to list files in the 'Archive' folder
# s3.list_files_in_folder(s3.BUCKET_NAME, s3.FOLDER_NAME)
# exit()

utils.log_setup()
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
    token = api.checkAuth()
    logging.debug('999999999999999999999999')

logging.debug(f'!!!!!!!!!!! {token}')
if token:
    if mysql:
        db.update_mysql(token)
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

# Please clean up debug/development artifacts: remove commented out code, debug print statements (11111..., 22222..., etc), and unused imports.
# Avoid hardcoding dates (e.g. '2090-11-14'). Consider making these configurable or deriving them from business logic.
# Here's what I looked at during the review
# 🟢 General issues: all looks good
# 🟢 Security: all looks good
# 🟡 Testing: 3 issues found
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:47

# issue(testing): Missing tests for core functionalities and edge cases.
# if orc:
# This file seems to contain integration or end-to-end test logic rather than unit tests. While this logic is valuable for manual testing, 
# it needs to be structured into proper test cases using a testing framework (like pytest). 
# This will allow for automated testing and ensure the code behaves as expected across different scenarios. Specifically, 
# we need tests for db.update_mysql, db.update_prospect, and api.checkAuth, including edge cases like invalid tokens, database errors, 
# and various responses from the Leonics API. For db.update_prospect, test cases should cover both local=True and local=False with different 
# start_ts values, including edge cases like invalid timestamps and future dates.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:66

# suggestion(testing): Avoid writing to files during tests.
#                 oracle_insert += f"    INTO {table_name} ({columns}) VALUES ({row})\n"
#         oracle_insert += "SELECT * FROM dual;"

#         with open("orc_sql.txt", "w") as file:
#             file.write(oracle_insert)
#     except Exception as e:
#         logging.info(f"ORACLE Error occurred: {e}")
# Writing to files within tests can lead to unexpected behavior and make tests less reliable. Instead of writing the oracle_insert string to a file, 
# assert its contents directly within the test case. This makes the test self-contained and easier to debug.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/full_test.py:88

# suggestion(testing): Address the TODO item.

# if pros:
#     # set start_time to highest DateTimeServer in Prospect
#     # TODO get from out API
#     db.update_prospect(start_ts='2090-11-14 01:52', local=True)
#     db.update_prospect(start_ts='2090-11-14 01:48', local=False)
# The TODO comment indicates a missing piece of functionality. It's important to address this before merging the pull request. 
# If fetching the start time from the API is not feasible within the scope of this PR, consider adding a placeholder 
# or mocking the API call for testing purposes.

# Suggested implementation:

# if pros:
#     # Fetch the latest timestamp from Prospect API
#     try:
#         # Assuming there's an API method to get the latest timestamp
#         latest_timestamp = get_latest_prospect_timestamp()

#         # Update prospects with the retrieved timestamp
#         db.update_prospect(start_ts=latest_timestamp, local=True)
#         db.update_prospect(start_ts=latest_timestamp, local=False)
#     except Exception as e:
#         # Fallback to a default timestamp if API call fails
#         default_timestamp = '2090-11-14 01:52'
#         logging.warning(f"Failed to fetch timestamp from API: {e}. Using default: {default_timestamp}")
#         db.update_prospect(start_ts=default_timestamp, local=True)
#         db.update_prospect(start_ts=default_timestamp, local=False)

# You'll need to implement the get_latest_prospect_timestamp() function. This could involve:

# Making an HTTP request to your Prospect API
# Querying a database
# Using an existing method in your API client
# Add error handling and logging

# Consider adding a configuration or environment variable for the default timestamp

# Potentially create a utility function for timestamp retrieval that can be reused

# Example implementation of get_latest_prospect_timestamp():

# def get_latest_prospect_timestamp():
#     """
#     Retrieve the latest timestamp from the Prospect API.

#     Returns:
#         str: Timestamp in 'YYYY-MM-DD HH:MM' format
#     """
#     try:
#         # Replace with actual API call
#         response = api_client.get_latest_timestamp()
#         return response.get('timestamp')
#     except Exception as e:
#         logging.error(f"Error fetching timestamp: {e}")
#         raise
