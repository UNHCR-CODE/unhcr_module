"""
Overview
    This file (utils.py) within the unhcr module provides a set of utility functions for data processing, 
    logging setup, module version retrieval, and dynamic module importing. It focuses on cleaning JSON-like data, 
    configuring logging, and managing module functionalities.

Key Components
    ts2Epoch(dt, offset_hrs=0):
        Convert a date string to epoch time in seconds, adjusted for a given time offset, returns epoch in seconds

    filter_nested_dict(obj, val=-0.999): 
        Recursively removes a specified value (val, defaulting to -0.999) from nested dictionaries and lists. 
        This function is crucial for cleaning JSON-like data by removing placeholder values representing missing 
        or invalid data.

    log_setup(level=None): 
        This function configures the logging for the module. It allows setting the logging level via command-line 
        arguments (using --log followed by the desired level, e.g., INFO, DEBUG, etc.). If no level is provided, 
        it defaults to INFO. The logs are outputted to both the console and a file named 'unhcr.module.log'.

    str_to_float_or_zero(value): 
        This function attempts to convert a given value to a float. If the conversion fails due to a ValueError 
        or TypeError, it logs the error and returns 0.0. This provides a safe way to handle potential 
        data type issues during processing.

    get_module_version(name='unhcr_module'): 
        Retrieves the version number of the specified module (defaulting to 'unhcr_module'). 
        It returns the version number and any potential error message encountered during retrieval. 
        This is useful for tracking and managing module versions.
    
    is_version_greater_or_equal(ver): 
        Determines if a given version is greater than or equal to the module version.
        It compares a provided version string (ver) with the current module version. 
        It uses semantic versioning (major.minor.patch) for comparison.

    extract_data(data_list,site=None): 
        Extracts and returns site, table, fn, and label from a list of dictionaries. 
        It iterates over each dictionary in the given data_list. If the site parameter is None, 
        it assigns the values from the first dictionary's "site", "table", "fn", and "label" 
        (if available) keys to the respective variables. If the site parameter matches the 
        "site" key in any dictionary, it updates the table, fn, and label (if available) 
        variables with the values from that dictionary. Prints the extracted values for each 
        matching dictionary.
"""

from datetime import datetime, timedelta
from importlib.metadata import version
import logging
import optparse
import os
import sys


def config_log_handler(handler, level, formatter, logger):
    """
    Configure a log handler with the given level, formatter, and add it to the given logger.

    Parameters
    ----------
    handler : logging.Handler
        The log handler to be configured
    level : str
        The logging level to set for the handler
    formatter : logging.Formatter
        The formatter to set for the handler
    logger : logging.Logger
        The logger to add the handler to
    """
    handler.setLevel(getattr(logging, level))
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def log_setup(level="INFO", log_file="unhcr.module.log", override=False):
    # Check if the logger already has handlers to prevent adding duplicates
    """
    Configure the logging for the module. If level is None, it will look for a command-line argument --log followed by the desired level, e.g., INFO, DEBUG, etc.
    If no level is provided, it defaults to INFO. The logs are outputted to both the console and a file named 'unhcr.module.log'.

    Parameters
    ----------
    level : str, optional
        The desired logging level, by default 'INFO'
    log_file : str, optional
        The name of the log file, by default 'unhcr.module.log'
    override : bool, optional
        If True, it will clear the existing handlers and set up new ones, by default False

    Returns
    -------
    logging.Logger
        The configured logger
    """
    logger = logging.getLogger()
    if override:
        logger.handlers.clear()

    if logger.hasHandlers():
        return logger  # Return the logger if it already has handlers

    args = create_cmdline_parser(level) if level is None else level.upper()
    level = args
    if os.getenv("DEBUG") == "1":
        level = "DEBUG"
    # Validate logging level
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if level not in valid_levels:
        raise ValueError(
            f"Invalid logging level: {level}. Must be one of {valid_levels}."
        )

    # Create a formatter that outputs the log format
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console handler
    console_handler = logging.StreamHandler()
    config_log_handler(console_handler, level, formatter, logger)
    # File handler
    file_handler = logging.FileHandler(log_file)
    config_log_handler(file_handler, level, formatter, logger)
    # Set the overall logging level
    logger.setLevel(getattr(logging, level))

    return logger

# init logging
log_setup(override=True)


def ts2Epoch(dt, offset_hrs=0):
    """
    Convert a date string to epoch time in seconds, adjusted for a given time offset

    Parameters
    ----------
    dt : str
        Date string in format %Y-%m-%dT%H:%M:%S
    offset_hrs : int
        Number of hours to offset the epoch time by

    Returns
    -------
    int
        The epoch time in seconds
    """
    p = '%Y-%m-%dT%H:%M:%S'
    epoch = datetime(1970, 1, 1)
    e = (datetime.strptime(dt, p) - timedelta(hours= offset_hrs) - epoch).total_seconds()
    return int(e)


def filter_nested_dict(obj, val=-0.999):
    """
    Recursively remove all entries of a nested dict that have a value equal to val. If the object is a dictionary,
    it creates a new dictionary containing only key-value pairs where the value is not equal to val. If the object is a list,
    it creates a new list containing only items that are not equal to val. Otherwise, it returns the object unchanged.
    The default value for val is -0.999. This function is crucial for cleaning JSON-like data by removing a specific placeholder value
    representing missing or unwanted data.

    Parameters
    ----------
    obj : dict | list
        The object to be filtered
    val : any, optional
        The value to be removed from the object, by default -0.999

    Returns
    -------
    dict | list
        The filtered object
    """
    if isinstance(obj, dict):
        return {k: filter_nested_dict(v) for k, v in obj.items() if v != val}
    elif isinstance(obj, list):
        return [filter_nested_dict(item) for item in obj]
    else:
        return obj


def create_cmdline_parser(level="INFO"):
    """
    Creates a command-line parser to read environment and logging options from the command line.
    This version is designed to work alongside pytest's command-line arguments.

    Parameters
    ----------
    level : str, optional
        The default logging level, by default 'INFO'

    Returns
    -------
    optparse.Values or None
        An object containing the parsed command-line options if successful,
        or None if an error occurs during parsing.

    Raises
    ------
    None
    """
    # Create the option parser for custom arguments
    parser = optparse.OptionParser()

    # Add your custom arguments
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
        default=level,
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
    logging.debug(f"{sys.argv}        {original_args}")
    try:
        # Now, parse custom arguments
        (options, args) = parser.parse_args()
        sys.argv = original_args
        logging.debug(f"{sys.argv}        {original_args}")
        # Validate the logging level
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if options.log not in valid_log_levels:
            parser.error(
                f"Invalid log level: {options.log}. Valid options are: {', '.join(valid_log_levels)}"
            )

        return options
    except Exception as e:
        print(f"ERROR: {e}")
        return None


def str_to_float_or_zero(value):
    """
    Safely convert a value to a float, or return 0.0 if there is an error.

    Args:
        value: The value to convert to a float.

    Returns:
        float: The converted value, or 0.0 if the conversion fails.
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        logging.error(f"str_to_float_or_zero !!!!!!  {ValueError}, {TypeError}")
        return 0.0


def get_module_version(name="unhcr_module"):
    """
    Retrieve the version number of the specified module (default: 'unhcr_module').

    Returns a tuple containing the version number as a string and an error message
    (if any). If the version number cannot be retrieved, the returned version number
    is None and the error message is set to the caught exception.

    Parameters:
        name (str): The name of the module to retrieve the version for (default: 'unhcr_module').

    Returns:
        tuple: (version_number, error_message)
    """
    v_number = None
    err = None
    try:
        v_number = version("unhcr_module")
    except Exception as e:
        err = str(e)
    return v_number, err

def is_version_greater_or_equal(ver):
    # Remove the "v_" prefix from version2 if it exists
    """
    Determine if a given version is greater than or equal to the module version.

    This function compares a provided version string (ver) with the current module
    version. It uses semantic versioning (major.minor.patch) for comparison.

    Parameters:
        ver (str): The version string to compare against the module version.

    Returns:
        bool: True if the provided version is greater than or equal to the module 
        version, False otherwise. If an error occurs while retrieving the module 
        version, it logs the error and returns False.
    """

    version, err = get_module_version()
    if err:
        logging.error(f"get_module_version Error occurred: {err}")
        return False

    version.lstrip("v_")

    # Split both version strings into major, minor, and patch
    parts1 = list(map(int, ver.split(".")))
    parts2 = list(map(int, version.split(".")))

    # Compare each component: major, minor, patch
    return parts1 >= parts2


def extract_data(data_list,site=None):
    """
    Extracts and returns site, table, fn, and label from a list of dictionaries.

    Iterates over each dictionary in the given data_list. If the site parameter is None, 
    it assigns the values from the first dictionary's "site", "table", "fn", and "label" 
    (if available) keys to the respective variables. If the site parameter matches the 
    "site" key in any dictionary, it updates the table, fn, and label (if available) 
    variables with the values from that dictionary. Prints the extracted values for each 
    matching dictionary.

    Parameters
    ----------
    data_list : list of dict
        A list of dictionaries containing keys "site", "table", "fn", and optionally "label".
    site : str, optional
        The site to search for within the data_list. If None, uses the first site's data.

    Returns
    -------
    site : str
        The extracted site value.
    table : str
        The extracted table value.
    fn : str
        The extracted fn (function) value.
    label : str
        The extracted label value (if available).
    """

    for key in data_list:
        if site is None:
            if key.has_key("site"):
                site = key["site"]
            else:
                site = list(key.keys())[0]
            table = key["table"]
            fn = key["fn"]
            if key.has_key("label"):
                label = key["label"]
            print(site, table, fn, label)
        elif site == key["site"]:
            table = key["table"]
            fn = key["fn"]
            label = key["label"]
            print(site, table, fn)
    return site,table,fn,label