# sorcery: disable
"""
Overview
    This file, constants.py, centralizes configuration constants for the UNHCR module. It loads environment variables from a .env file 
    to populate these constants, which are used for connecting to various external services like Leonics API, Aiven MySQL database, 
    Eyedro S3 storage, and the Prospect API. It also dynamically imports local modules.

Key Components
Environment Variable Loading: 
    The load_dotenv function from the python-dotenv library is used to load environment variables from a specified 
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
    
Dynamic Module Loading: 
    The import_local_libs function dynamically imports other modules within the application based on the MOD_PATH and MODULES variables. 
    This allows for a more flexible and organized code structure.

Initialization: 
    The file concludes by calling load_env() and set_environ() to load the environment variables and initialize the constants, 
    respectively. This ensures that the necessary environment variables are available in other parts of the module.
"""
import os
import sys
import importlib
import logging

from dotenv import find_dotenv, load_dotenv

PROD=None
DEBUG=None
LOCAL=None

logging.debug(f"PROD: {PROD}, DEBUG: {DEBUG}, LOCAL: {LOCAL} ")

# Leonics API
LEONICS_BASE_URL = None
LEONICS_USER_CODE = None
LEONICS_KEY = None

# Verify SSL --- note that leonic's cert does not verify
VERIFY=None

# Aiven Mysql DB
AIVEN_TAKUM_CONN_STR = None

# Eyedro S3
ACCESS_KEY = None
SECRET_KEY = None
BUCKET_NAME = None
FOLDER_NAME = None

# Prospect API
BASE_URL = None
API_IN_KEY = None
API_OUT_KEY = None

# if your running a local instance of Prospect
LOCAL_BASE_URL = None
LOCAL_API_IN_KEY = None
LOCAL_API_OUT_KEY = None

# SOLARMAN NIGERIA
SM_APP_ID = None
SM_APP_SECRET = None
# token will expire every 2 months
SM_BIZ_ACCESS_TOKEN = None
SM_URL = None
SM_TOKEN_URL = None
SM_HISTORY_URL = None

environ_path = None

#sorcery skip
def set_environ():
    """
    Loads environment variables from a .env file and sets configuration constants for accessing external services and APIs.

    Environment variables are loaded from a .env file using the load_dotenv function from the python-dotenv library.
    The override=True argument ensures that environment variables defined in the .env file take precedence over existing system environment variables.

    The function sets constants for the Leonics API, Aiven MySQL database, Eyedro S3 storage, Prospect API, and Solarman Nigeria API.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Raises
    ------
    SystemExit
        Exits the program with error code 999 if the .env file is not found or cannot be loaded.
    """
    global PROD
    global DEBUG
    global LOCAL
    global LEONICS_BASE_URL
    global LEONICS_USER_CODE
    global LEONICS_KEY

    global VERIFY

    global AIVEN_TAKUM_CONN_STR

    global ACCESS_KEY
    global SECRET_KEY
    global BUCKET_NAME
    global FOLDER_NAME

    global BASE_URL
    global API_IN_KEY
    global API_OUT_KEY

    global LOCAL_BASE_URL
    global LOCAL_API_IN_KEY
    global LOCAL_API_OUT_KEY

    global SM_APP_ID
    global SM_APP_SECRET
    global SM_BIZ_ACCESS_TOKEN
    global SM_URL
    global SM_TOKEN_URL
    global SM_HISTORY_URL

    PROD=os.getenv('PROD') == '1'
    DEBUG=os.getenv('DEBUG') == '1'
    LOCAL = os.getenv('LOCAL') == '1' and not PROD

    logging.debug(f"PROD: {PROD}, DEBUG: {DEBUG}, LOCAL: {LOCAL} ")

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

    MOD_PATH = r'E:\_UNHCR\CODE\unhcr_module\unhcr'
    MODULES = [["utils", "utils"], ["constants", "const"], ['s3','s3'], ["db", "db"], ["api_leonics", "api_leonics"], ["api_prospect", "api_prospect"]]

def load_env(path = '.env'):
    """
    Load environment variables from a .env file.

    Parameters
    ----------
    path : str, optional
        The path to the .env file (default: '.env').

    Returns
    -------
    str or None
        The path to the .env file if it was found and loaded successfully, or None if not.

    Prints
    ------
    str
        A message indicating that the CONFIG file was not found or loaded.

    Notes
    -----
    The function uses the find_dotenv and load_dotenv functions from the dotenv library.
    It exits with a message if the .env file is not found or cannot be loaded.
    """
        
    global environ_path

    if env_file := find_dotenv(path):
        if load_dotenv(env_file, override=True):
            set_environ()
            environ_path = env_file[:-4]
            return env_file

    print(f"CONFIG file not found OR LOADED: {env_file}")
    return None

# if you want to load environment variables call these two function with the path to the env file.
load_env()

MOD_PATH = r'E:\_UNHCR\CODE\unhcr_module\unhcr'
MODULES = [["utils", "utils"], ["constants", "const"], ['s3','s3'], ["db", "db"], ["api_leonics", "api_leonics"], ["api_prospect", "api_prospect"]]

def import_local_libs(mpath=MOD_PATH, mods=MODULES):
    """
    Import local libraries by dynamically loading specified modules from a given path.

    Parameters
    ----------
    mpath : str, optional
        The directory path where the modules are located, by default `mod_path`.
    mods : list of lists
        A list of lists where each inner list contains two strings: the module's 
        file name (without '.py') and the name to assign the loaded module in globals().

    Returns
    -------
    list
        A list of loaded modules.

    Notes
    -----
    This function dynamically imports modules from the specified local directory,
    allowing their functions and variables to be accessed globally by assigning them
    to the globals() dictionary.
    """

    loaded_modules = []
    for mod in mods:
        module_name, global_name = mod
        if module_name in sys.modules:
            # Use the already loaded module
            loaded_modules.append(sys.modules[module_name])
            continue
        if module_name in loaded_modules:
            # Use the already loaded module
            continue

        # Construct the full path to the module file
        module_path = os.path.join(mpath, f"{module_name}.py")
        if not os.path.exists(module_path):
            print(f"Module file not found: {module_path}")
            continue

        try:
            # Load the module dynamically
            spec = importlib.util.spec_from_file_location(global_name, module_path)
            loaded_mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(loaded_mod)
            # exists = False
            # for val in loaded_modules:
            #     if val.__file__ == loaded_mod.__file__:
            #         exists = True
            #         break

            # if not exists:
            loaded_modules.append(loaded_mod)
        except Exception as e:
            print(f"Failed to load module {module_name}: {e}")

    # print(f"Load modules: {mpath}\n {mods}" )
    # print('!!!!!!!!!')
    # print(loaded_modules)
    # print('!!!!!!!!!')
    return tuple(loaded_modules)



##############################
# Hey there - I've reviewed your changes and found some issues that need to be addressed.

# Blocking issues:

# Hardcoded SSL verification disable is a security risk (e:/_UNHCR/CODE/unhcr_module/unhcr/constants.py:154)
# Overall Comments:

# SECURITY ISSUE: Disabling SSL verification (VERIFY=False) creates a significant security vulnerability. Either obtain a valid certificate from Leonics or implement certificate pinning if dealing with a known self-signed certificate.
# The generic exception handling in import_local_libs() should catch specific exceptions (e.g., ImportError, ModuleNotFoundError) rather than catching all exceptions. This will make debugging easier and prevent masking unexpected errors.
# Here's what I looked at during the review
# 🟡 General issues: 1 issue found
# 🔴 Security: 1 blocking issue
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/constants.py:154

# issue(security): Hardcoded SSL verification disable is a security risk
#     LEONICS_KEY = os.getenv('LEONICS_KEY')

#     #Verify SSL --- note that leonic's cert does not verify
#     VERIFY=False

#     # Aiven Mysql DB
# SSL verification should be configurable and default to True. Disabling SSL verification exposes the application to man-in-the-middle attacks and other security vulnerabilities.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/constants.py:283

# suggestion(bug_risk): Generic exception handling lacks specificity

#             # if not exists:
#             loaded_modules.append(loaded_mod)
#         except Exception as e:
#             print(f"Failed to load module {module_name}: {e}")

# Replace generic exception handling with specific exception types. Log the full traceback and consider more granular error handling to prevent silent failures.
