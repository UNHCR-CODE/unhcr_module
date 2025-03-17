# sorcery: disable
"""
Overview
    This file, constants.py, centralizes configuration constants for the UNHCR module. It loads environment variables from a .env file 
    to populate these constants, which are used for connecting to various external services like Leonics API, Aiven MySQL database, 
    Eyedro S3 storage, and the Prospect API. It also dynamically imports local modules.

Key Components
Environment Variable Loading: 
    Uses python-dotenv to load environment variables from a .env file, prioritizing these over system environment variables. 
    This supports secure storage of sensitive information.
API Configuration: 
    Stores API keys and base URLs for Leonics and Prospect APIs. Separate constants are defined for local and production environments 
    for the Prospect API.
Database Connection String: 
    TAKUM_RAW_CONN_STR holds the connection string for the Aiven MySQL database.
S3 Storage Credentials: 
    Constants store credentials for accessing Eyedro S3 storage, including access key, secret key, bucket name, and folder name.
SSL Verification: 
    VERIFY is set to False, disabling SSL verification for Leonics API calls. This is a potential security risk.
Dynamic Module Loading: 
    The import_local_libs function dynamically imports local modules based on the MOD_PATH and MODULES variables, 
    promoting a modular code structure.
Initialization: 
    The load_env and set_environ functions load the environment variables and populate the constants, respectively. 
    The env_cmdline_parser function allows specifying a different .env file via command-line arguments.
"""

from dotenv import find_dotenv, load_dotenv
import importlib
import logging
import optparse
import os
import requests
import sys
from unhcr import utils

# Define constants
PROD = None
DEBUG = None
LOCAL = None
AZURE_URL = None
MOD_PATH = None


# Leonics API
LEONICS_BASE_URL = None
LEONICS_USER_CODE = None
LEONICS_KEY = None

# Verify SSL --- note that leonic's cert does not verify
VERIFY = None

# Prospect DB
PROS_CONN_LOCAL_STR = None
PROS_CONN_AZURE_STR = None

# Aiven Mysql DB
TAKUM_RAW_CONN_STR = None
LEONICS_RAW_TABLE = None

# Fuel DB
AIVEN_FUEL_DB_CONN_STR=None
AZURE_FUEL_DB_CONN_STR=None

# DB connection pool
SQLALCHEMY_POOL_SIZE = None
SQLALCHEMY_POOL_TIMEOUT = None
SQLALCHEMY_POOL_RECYCLE = None
SQLALCHEMY_MAX_OVERFLOW = None

# Eyedro S3
ACCESS_KEY = None
SECRET_KEY = None
BUCKET_NAME = None
FOLDER_NAME = None

# Eyedro API
GB_API_V1_API_BASE_URL=None
GB_API_V1_GET_DATA=None
GB_API_V1_EMPTY_KEY=None
GB_API_V1_GET_DEVICE_LIST=None
GB_API_V1_USER_KEY=None

# Prospect API
BASE_URL = None
API_IN_KEY = None
API_OUT_KEY = None

# if your running a local instance of Prospect
LOCAL_BASE_URL = None
AZURE_BASE_URL = None
LOCAL_API_IN_KEY = None
AZURE_API_IN_KEY = None
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

def is_wsl():
    return "WSL_DISTRO_NAME" in os.environ or "WSL_INTEROP" in os.environ

def is_running_on_azure():
    try:
        response = requests.get(
            "http://169.254.169.254/metadata/instance?api-version=2021-02-01",
            headers={"Metadata": "true"},
            timeout=5,
        )
        return response.status_code == 200
    except requests.RequestException:
        return False
    
# sorcery skip
def set_environ():  # sourcery skip: extract-duplicate-method
    """
    Initialize and set global environment variables for the UNHCR module.

    This function retrieves various environment variables using `os.getenv` and assigns
    them to global constants, configuring the module to connect with external services
    such as Leonics API, Aiven MySQL database, Eyedro S3, and Prospect API. It also
    constructs specific URLs and manages configuration flags for production, debug, and
    local environments.

    Globals:
    --------
    PROD : bool
        Indicates if the module is running in production mode.
    DEBUG : bool
        Indicates if the module is running in debug mode.
    LOCAL : bool
        Indicates if the module is running in a local environment.
    AZURE_URL : str
        URL for the Azure VM.
    MOD_PATH : str
        Path to the directory where the modules are located.
    LEONICS_BASE_URL : str
        Base URL for the Leonics API.
    LEONICS_USER_CODE : str
        User code for Leonics API.
    LEONICS_KEY : str
        Key for Leonics API.
    VERIFY : bool
        SSL verification flag for Leonics API, set to False due to unverified certificate.
    PROS_CONN_LOCAL_STR: str
        Connection string for local instance of Prospect data_custom table
    PROS_CONN_AZURE_STR: str
        Connection string for Azure instance of Prospect data_custom table.
    TAKUM_RAW_CONN_STR : str
        Connection string for Aiven MySQL database Leonics raw data.
    LEONICS_RAW_TABLE : str
        Table name for Leonics raw data.
    AIVEN_FUEL_DB_CONN_STR : str
        Connection string for Aiven MySQL database Fuel data.
    AZURE_FUEL_DB_CONN_STR : str
        Connection string for Azure MySQL database Fuel data.
    SQLALCHEMY_POOL_SIZE : int
        Size of the connection pool for SQLAlchemy.
    SQLALCHEMY_POOL_TIMEOUT : int
        Timeout for the connection pool for SQLAlchemy.
    SQLALCHEMY_POOL_RECYCLE : int
        Recycle time for the connection pool for SQLAlchemy.
    SQLALCHEMY_MAX_OVERFLOW : int
        Maximum overflow for the connection pool for SQLAlchemy.
    ACCESS_KEY : str
        AWS access key for Eyedro S3.
    SECRET_KEY : str
        AWS secret key for Eyedro S3.
    BUCKET_NAME : str
        S3 bucket name for Eyedro.
    FOLDER_NAME : str
        S3 folder name for Eyedro.
    GB_API_V1_API_BASE_URL : str
        Base URL for GB API v1.
    GB_API_V1_GET_DATA : str
        Endpoint for getting data from GB API v1.
    GB_API_V1_EMPTY_KEY : str
        Empty key for GB API v1.
    GB_API_V1_GET_DEVICE_LIST : str
        Endpoint for getting device list from GB API v1.
    GB_API_V1_USER_KEY : str
        User key for GB API v1.
    BASE_URL : str
        Base URL for Prospect API.
    API_IN_KEY : str
        API key for incoming requests to Prospect.
    API_OUT_KEY : str
        API key for outgoing requests from Prospect.
    LOCAL_BASE_URL : str
        Base URL for local instance of Prospect.
    AZURE_BASE_URL : str
        Base URL for Azure instance of Prospect.
    LOCAL_API_IN_KEY : str
        API key for incoming requests to local Prospect instance.
    AZURE_API_IN_KEY : str
        API key for incoming requests to Azure Prospect instance.
    LOCAL_API_OUT_KEY : str
        API key for outgoing requests from local Prospect instance.
    SM_APP_ID : str
        Application ID for SOLARMAN NIGERIA.
    SM_APP_SECRET : str
        Application secret for SOLARMAN NIGERIA.
    SM_BIZ_ACCESS_TOKEN : str
        Business access token for SOLARMAN NIGERIA, expires every 2 months.
    SM_URL : str
        Base URL for SOLARMAN NIGERIA.
    SM_TOKEN_URL : str
        URL for obtaining SOLARMAN NIGERIA token.
    SM_HISTORY_URL : str
        URL for accessing SOLARMAN NIGERIA historical data.
    """

    global PROD
    global DEBUG
    global LOCAL
    global MOD_PATH
    global AZURE_URL
    global LEONICS_BASE_URL
    global LEONICS_USER_CODE
    global LEONICS_KEY

    global VERIFY

    global PROS_CONN_LOCAL_STR
    global PROS_CONN_AZURE_STR
    global TAKUM_RAW_CONN_STR
    global LEONICS_RAW_TABLE
    global AIVEN_FUEL_DB_CONN_STR
    global AZURE_FUEL_DB_CONN_STR

    global SQLALCHEMY_POOL_SIZE
    global SQLALCHEMY_POOL_TIMEOUT
    global SQLALCHEMY_POOL_RECYCLE
    global SQLALCHEMY_MAX_OVERFLOW

    global ACCESS_KEY
    global SECRET_KEY
    global BUCKET_NAME
    global FOLDER_NAME
    
    global GB_API_V1_API_BASE_URL
    global GB_API_V1_GET_DATA
    global GB_API_V1_EMPTY_KEY
    global GB_API_V1_GET_DEVICE_LIST
    global GB_API_V1_USER_KEY

    global BASE_URL
    global API_IN_KEY
    global API_OUT_KEY

    global LOCAL_BASE_URL
    global AZURE_BASE_URL
    global LOCAL_API_IN_KEY
    global AZURE_API_IN_KEY
    global LOCAL_API_OUT_KEY

    global SM_APP_ID
    global SM_APP_SECRET
    global SM_BIZ_ACCESS_TOKEN
    global SM_URL
    global SM_TOKEN_URL
    global SM_HISTORY_URL

    PROD = os.getenv("PROD", "PROD missing") == "1"
    DEBUG = os.getenv("DEBUG", "DEBUG missing") == "1"
    LOCAL = os.getenv("LOCAL", "LOCAL missing") == "1" and not PROD
    AZURE_URL = os.getenv("AZURE_URL", "AZURE_URL missing")
    # change in .env file to your path to use local modules
    if is_wsl():
        MOD_PATH = os.getenv("MOD_PATH_WSL", "MOD_PATH_WSL missing")
    else:
        MOD_PATH = os.getenv("MOD_PATH", "MOD_PATH missing")
    MODULES = [
        ["utils", "utils"],
        ["constants", "const"],
        ["s3", "s3"],
        ["db", "db"],
        ["api_leonics", "api_leonics"],
        ["api_prospect", "api_prospect"],
    ]
    logging.debug(f"PROD: {PROD}, DEBUG: {DEBUG}, LOCAL: {LOCAL} ")

    # Leonics API
    # sorcery skip
    LEONICS_BASE_URL = os.getenv("LEONICS_BASE_URL", "LEONICS_BASE_URL missing")
    LEONICS_USER_CODE = os.getenv("LEONICS_USER_CODE", "LEONICS_USER_CODE missing")
    LEONICS_KEY = os.getenv("LEONICS_KEY", "LEONICS_KEY missing")

    # Verify SSL --- note that leonic's cert does not verify
    # sorcery skip
    VERIFY = False

    # Prospect DB
    PROS_CONN_LOCAL_STR = os.getenv(
        "PROS_CONN_LOCAL_STR", "PROS_CONN_LOCAL_STR missing"
    )
    PROS_CONN_AZURE_STR = os.getenv(
        "PROS_CONN_AZURE_STR", "PROS_CONN_AZURE_STR missing"
    )
    

    # Aiven Mysql DB
    TAKUM_RAW_CONN_STR = os.getenv(
        "AIVEN_TAKUM_LEONICS_API_RAW_CONN_STR",
        "AIVEN_TAKUM_LEONICS_API_RAW_CONN_STR missing",
    )
    LEONICS_RAW_TABLE = os.getenv("LEONICS_RAW_TABLE", "LEONICS_RAW_TABLE missing")
    
    AIVEN_FUEL_DB_CONN_STR = os.getenv(
        "AIVEN_FUEL_DB_CONN_STR", "AIVEN_FUEL_DB_CONN_STR missing"
    )
    AZURE_FUEL_DB_CONN_STR = os.getenv(
        "AZURE_FUEL_DB_CONN_STR", "AZURE_FUEL_DB_CONN_STR missing"
    )

    SQLALCHEMY_POOL_SIZE = int(
        os.getenv("SQLALCHEMY_POOL_SIZE", "SQLALCHEMY_POOL_SIZE missing") or 5
    )
    SQLALCHEMY_POOL_TIMEOUT = int(
        os.getenv("SQLALCHEMY_POOL_TIMEOUT", "SQLALCHEMY_POOL_TIMEOUT missing") or 30
    )
    SQLALCHEMY_POOL_RECYCLE = int(
        os.getenv("SQLALCHEMY_POOL_RECYCLE", "SQLALCHEMY_POOL_RECYCLE missing") or 3600
    )
    SQLALCHEMY_MAX_OVERFLOW = int(
        os.getenv("SQLALCHEMY_MAX_OVERFLOW", "SQLALCHEMY_MAX_OVERFLOW missing") or 10
    )

    # Eyedro S3
    ACCESS_KEY = os.getenv("GB_AWS_ACCESS_KEY", "GB_AWS_ACCESS_KEY missing")
    SECRET_KEY = os.getenv("GB_AWS_SECRET_KEY", "GB_AWS_SECRET_KEY missing")
    BUCKET_NAME = os.getenv("GB_AWS_BUCKET_NAME", "GB_AWS_BUCKET_NAME missing")
    FOLDER_NAME = os.getenv("GB_AWS_FOLDER_NAME", "GB_AWS_FOLDER_NAME missing")
    
    # Eyedro API
    GB_API_V1_API_BASE_URL = os.getenv("GB_API_V1_API_BASE_URL", "GB_API_V1_API_BASE_URL missing")
    GB_API_V1_GET_DATA = os.getenv("GB_API_V1_GET_DATA", "GB_API_V1_GET_DATA missing")
    GB_API_V1_EMPTY_KEY = os.getenv("GB_API_V1_EMPTY_KEY", "GB_API_V1_EMPTY_KEY missing")
    GB_API_V1_GET_DEVICE_LIST = os.getenv("GB_API_V1_GET_DEVICE_LIST", "GB_API_V1_GET_DEVICE_LIST missing")
    GB_API_V1_USER_KEY = os.getenv("GB_API_V1_USER_KEY", "GB_API_V1_USER_KEY missing")

    # Prospect API
    BASE_URL = os.getenv("PROS_BASE_URL", "PROS_BASE_URL missing")
    API_IN_KEY = os.getenv("PROS_IN_API_KEY", "PROS_IN_API_KEY missing")
    API_OUT_KEY = os.getenv("PROS_OUT_API_KEY", "PROS_OUT_API_KEY missing")

    # if your running a local instance of Prospect
    LOCAL_BASE_URL = os.getenv("PROS_LOCAL_BASE_URL", "PROS_LOCAL_BASE_URL missing")
    AZURE_BASE_URL = os.getenv("PROS_AZURE_BASE_URL", "PROS_AZURE_BASE_URL missing")
    LOCAL_API_IN_KEY = os.getenv(
        "PROS_IN_LOCAL_API_KEY", "PROS_IN_LOCAL_API_KEY missing"
    )
    AZURE_API_IN_KEY = os.getenv(
        "PROS_IN_AZURE_API_KEY", "PROS_IN_AZURE_API_KEY missing"
    )
    LOCAL_API_OUT_KEY = os.getenv(
        "PROS_OUT_LOCAL_API_KEY", "PROS_OUT_LOCAL_API_KEY missing"
    )


    # SOLARMAN NIGERIA
    SM_APP_ID = os.getenv("SM_APP_ID", "SM_APP_ID missing")
    SM_APP_SECRET = os.getenv("SM_APP_SECRET", "SM_APP_SECRET missing")
    # token will expire every 2 months
    SM_BIZ_ACCESS_TOKEN = os.getenv(
        "SM_BIZ_ACCESS_TOKEN", "SM_BIZ_ACCESS_TOKEN missing"
    )
    SM_URL = os.getenv("SM_URL", "SM_URL missing")
    SM_TOKEN_URL = f"{SM_URL}/account/v1.0/token"
    SM_HISTORY_URL = f"{SM_URL}/device/v1.0/historical?language=en"

    if is_running_on_azure():
        PROS_CONN_AZURE_STR = PROS_CONN_LOCAL_STR.replace(AZURE_URL, "localhost")
        AZURE_FUEL_DB_CONN_STR = AZURE_FUEL_DB_CONN_STR.replace(AZURE_URL, "localhost")
        AZURE_BASE_URL = AZURE_BASE_URL.replace(AZURE_URL, "localhost")

def load_env(path=".env"):
    """
    Load environment variables from a specified .env file.

    Parameters
    ----------
    path : str, optional
        The path to the .env file (default is ".env").

    Returns
    -------
    str or None
        Returns the path to the .env file without the extension if the file
        is found and loaded successfully; otherwise, returns None.

    Notes
    -----
    This function sets the global variable `environ_path` to the path of the
    found .env file. If the file is loaded successfully, it calls the
    `set_environ` function to populate environment variables.

    Raises
    ------
    None
    """

    global environ_path

    environ_path = find_dotenv(path)
    if environ_path == "" or path not in [".env", environ_path]:
        return None

    if found := load_dotenv(environ_path, override=True):
        set_environ()
        return environ_path[:-4]
    return None


def env_cmdline_parser():
    """
    Parse command-line arguments to set environment and logging options.

    This function uses the `optparse` module to define and parse command-line
    options for specifying the path to the environment directory and the logging
    level. It validates the provided logging level against a set of predefined
    valid levels.

    Returns
    -------
    optparse.Values or None
        An object containing the parsed command-line options if successful,
        or None if an error occurs during parsing.

    Raises
    ------
    None
    """

    parser = optparse.OptionParser()
    # parser = argparse.ArgumentParser(description="Process some environment and log options.")
    # Add command-line arguments
    parser.add_option(
        "--env",
        dest="env",
        default=".env",
        type="string",
        help="Path to environment directory",
    )
    parser.add_option(
        "--log",
        dest="log",
        default="INFO",
        type="string",
        help="Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    # Parse pytest arguments first by letting pytest handle them -- these are the ones we use
    original_args = list(sys.argv)

    # Remove pytest's own options from the argument list
    sys.argv = [
        arg
        for arg in sys.argv
        if arg not in ["-v", "--cov=..", "--cov-report=html", "--cache-clear"]
    ]

    logging.debug(f"Modified args: {sys.argv}     Original args:   {original_args}")
    try:
        # Now, parse custom arguments
        (options, args) = parser.parse_args()
        sys.argv = original_args
        logging.debug(f"Modified args: {sys.argv}     Original args:   {original_args}")
        # List of valid choices
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        # Validate the log level
        if options.log not in valid_log_levels:
            parser.error(
                f"Invalid log level: {options.log}. Valid options are: {', '.join(valid_log_levels)}"
            )

        return options
    except Exception as e:
        print(f"ERROR: {e}")
        return None


utils.log_setup(override=True)
# loads environment variables and sets constants
args = utils.create_cmdline_parser()
if args is None:
    exit(999)
res = load_env(args.env)
if res is None:
    print("No .env file found or could not be loaded")
    exit(999)

MODULES = [
    ["err_handler", "err_handler"],
    ["app_utils", "app_utils"],
    ["utils", "utils"],
    ["constants", "const"],
    ["db", "db"],
    ["api_leonics", "api_leonics"],
    ["api_prospect", "api_prospect"],
    ["nigeria_sm_fuel", "sm_fuel"],
    ["s3", "s3"],
]


def import_local_libs(mpath=MOD_PATH, mods=MODULES):
    """
    Dynamically imports local modules from the specified local directory, allowing their functions and variables to be accessed globally by assigning them to the globals() dictionary.

    Parameters
    ----------
    mpath : str, optional
        The directory path where the modules are located, by default MOD_PATH.
    mods : list of lists
        A list of lists where each inner list contains two strings: the module's file name (without '.py') and the name to assign the loaded module in globals().

    Returns
    -------
    list
        A list of loaded modules.

    Notes
    -----
    This function dynamically imports modules from the specified local directory, allowing their functions and variables to be accessed globally by assigning them to the globals() dictionary.
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
            loaded_modules.append(loaded_mod)
        except Exception as e:
            logging.info(f"Failed to load module {module_name}: {e}")
            print(f"Failed to load module {module_name}: {e}")

    return tuple(loaded_modules)
