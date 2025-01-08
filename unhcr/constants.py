# sorcery: disable
"""
Overview
    This Python file (constants.py) defines and initializes various constants used for configuring connections to external services and APIs. 
    These services include Leonics, Aiven MySQL database, Eyedro S3 storage, and the Prospect API. 
    The file leverages environment variables loaded from a .env file to store sensitive information like API keys and connection strings.

Key Components
    Environment Variable Loading: The load_dotenv function from the python-dotenv library is used to load environment variables from a specified 
    .env file. This allows sensitive data to be kept separate from the codebase. The override=True argument ensures that environment variables
    defined in the .env file take precedence over existing system environment variables.

API Configuration: 
    Constants like LEONICS_BASE_URL, LEONICS_USER_CODE, LEONICS_KEY, BASE_URL, API_IN_KEY, and API_OUT_KEY store configuration details for 
    accessing different APIs. Separate constants are defined for local and production environments for the Prospect API 
    (LOCAL_BASE_URL, LOCAL_API_IN_KEY, LOCAL_API_OUT_KEY).

Database Connection String: 
    AIVEN_TAKUM_CONN_STR holds the connection string for the Aiven MySQL database.

S3 Storage Credentials: 
    ACCESS_KEY, SECRET_KEY, BUCKET_NAME, and FOLDER_NAME store credentials and configuration for interacting with Eyedro S3 storage.

SSL Verification: 
    The VERIFY constant is set to False, indicating that SSL certificate verification is disabled for Leonics API calls. 
    This is explicitly mentioned in a comment, suggesting potential security implications that should be addressed.
"""
import os
from dotenv import load_dotenv

# change the path to your .env file with the constants below
load_dotenv(r'E:\_UNHCR\CODE\unhcr_module\.env', override=True)

# Leonics API
LEONICS_BASE_URL = os.getenv('LEONICS_BASE_URL')
LEONICS_USER_CODE = os.getenv('LEONICS_USER_CODE')
LEONICS_KEY = os.getenv('LEONICS_KEY')

#Verify SSL --- note that leonic's cert does not verify
VERIFY=False

# Aiven Mysql DB
AIVEN_TAKUM_CONN_STR = os.getenv('AIVEN_TAKUM_LEONICS_API_RAW_CONN_STR')

#Eyedro S3
ACCESS_KEY = os.getenv('GB_AWS_ACCESS_KEY')
SECRET_KEY = os.getenv('GB_AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('GB_AWS_BUCKET_NAME')
FOLDER_NAME = os.getenv('GB_AWS_FOLDER_NAME')

# Prospect API
BASE_URL = os.getenv('PROS_BASE_URL')
API_IN_KEY = os.getenv('PROS_IN_API_KEY')
API_OUT_KEY = os.getenv('PROS_OUT_API_KEY')

# if your running a local instance of Prospect
LOCAL_BASE_URL = os.getenv('PROS_LOCAL_BASE_URL')
LOCAL_API_IN_KEY = os.getenv('PROS_IN_LOCAL_API_KEY')
LOCAL_API_OUT_KEY = os.getenv('PROS_OUT_LOCAL_API_KEY')

#SOLARMAN NIGERIA
SM_APP_ID = os.getenv('SM_APP_ID')
SM_APP_SECRET = os.getenv('SM_APP_SECRET')
#token will expire every 2 months
SM_BIZ_ACCESS_TOKEN = os.getenv('SM_BIZ_ACCESS_TOKEN')
SM_URL = os.getenv('SM_URL')
SM_TOKEN_URL = f"{SM_URL}/account/v1.0/token"
SM_HISTORY_URL = f"{SM_URL}/device/v1.0/historical?language=en"
