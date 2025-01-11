"""
Overview
    This file (utils.py) within the unhcr module provides a set of utility functions for data processing, logging setup, module version retrieval, 
    and dynamic module importing. It focuses on cleaning JSON-like data, configuring logging, and managing module functionalities.

Key Components
    filter_nested_dict(obj, val=-0.999): 
        Recursively removes a specified value (val, defaulting to -0.999) from nested dictionaries and lists. 
        This function is crucial for cleaning JSON-like data by removing placeholder values representing missing or invalid data.

    log_setup(level=None): 
        This function configures the logging for the module. It allows setting the logging level via command-line arguments 
        (using --log followed by the desired level, e.g., INFO, DEBUG, etc.). If no level is provided, it defaults to INFO. 
        The logs are outputted to both the console and a file named 'unhcr.module.log'.

    str_to_float_or_zero(value): 
        This function attempts to convert a given value to a float. If the conversion fails due to a ValueError or TypeError, 
        it logs the error and returns 0.0. This provides a safe way to handle potential data type issues during processing.
        
    get_module_version(name='unhcr_module'): 
        Retrieves the version number of the specified module (defaulting to 'unhcr_module'). 
        It returns the version number and any potential error message encountered during retrieval. 
        This is useful for tracking and managing module versions.

    import_libs(mpath, mods): 
        Dynamically imports modules, checking first if the module is already loaded. 
        This function supports importing modules from both the local directory and the unhcr package.

    load_env(path = '.env'): 
        Loads environment variables from a .env file. Exits the program if the file is not found or cannot be loaded.
"""

import argparse
from dotenv import find_dotenv, load_dotenv
import importlib
import logging
import os
import sys

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

def log_setup(level=None, log_file="unhcr.module.log"):
    """
    Example usage:
        logger = log_setup()
        logger.info("Logger is successfully set up!")
        logging.critical(f"CRITICAL:  Logging level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}")

    Set up logging for the module. If level is None, it parses command-line arguments to determine the logging level.
    If no arguments are provided, it defaults to INFO level.
    
    Usage:
        script.py --log INFO
    
    Args:
        level (str, optional): The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to None.
        log_file (str): The name of the log file. Defaults to 'unhcr.module.log'.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Check if the logger already has handlers to prevent adding duplicates
    logger = logging.getLogger()
    if logger.hasHandlers():
        return logger  # Return the logger if it already has handlers

    level = create_cmdline_parser() if level is None else level.upper()
    # Validate logging level
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if level not in valid_levels:
        raise ValueError(f"Invalid logging level: {level}. Must be one of {valid_levels}.")

    # Create a formatter that outputs the log format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Console handler
    console_handler = logging.StreamHandler()
    config_log_handler(console_handler, level, formatter, logger)
    # File handler
    file_handler = logging.FileHandler(log_file)
    config_log_handler(file_handler, level, formatter, logger)
    # Set the overall logging level
    logger.setLevel(getattr(logging, level))

    # Log the effective logging level
    logger.debug(f"DEBUG: Logging level set to {level}")
    logger.info(f"INFO: Logging level set to {level}")
    logger.warning(f"WARNING: Logging level set to {level}")
    logger.error(f"ERROR: Logging level set to {level}")

    return logger

def create_cmdline_parser():
    """
    Parse the command-line arguments and return the logging level as a string.
    If no arguments are provided, it defaults to INFO level.

    Returns
    -------
    str
        The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """

    default_level = 'INFO'
    if os.getenv('DEBUG') == '1':
        default_level = 'DEBUG'
    parser = argparse.ArgumentParser(description="Set logging level")
    parser.add_argument(
        "--log", 
        default=default_level, 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )

    # Add default logging arguments if none provided
    l = len(sys.argv)
    if l == 1:  # handle vscode debugging
        sys.argv.extend(["--log", default_level])
    elif l == 2:
        sys.argv[1] = '--log'
        sys.argv.append(default_level)

    args = parser.parse_args()
    return args.log.upper()

def config_log_handler(handler, level, formatter, logger):
    """
    Configure a log handler with the specified level and formatter.

    Parameters
    ----------
    handler : logging.Handler
        The log handler to configure.
    level : str
        The logging level to set on the handler (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).
    formatter : logging.Formatter
        The log formatter to use on the handler.
    logger : logging.Logger
        The logger that the handler will be added to.

    Returns
    -------
    None
    """
    handler.setLevel(getattr(logging, level))
    handler.setFormatter(formatter)
    logger.addHandler(handler)

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
        logging.error(f'str_to_float_or_zero !!!!!!  {ValueError}, {TypeError}')
        return 0.0

def get_module_version(name='unhcr_module'):
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
    from importlib.metadata import version
    v_number = None
    err = None
    try:
        v_number = version("unhcr_module")
    except Exception as e:
        err = str(e)
    return v_number, err

def import_libs(mpath, mods):
    """
    Dynamically import a module from either the local directory or the unhcr package.

    Args:
        mpath (str): The module path to search for the local module.
        mods (List[Tuple[str, str]]): A list of tuples containing the module name to import 
            and the module name to import as.

    Returns:
        module: The imported module.
    """
    
    for mod in mods:
        if mod[0] in sys.modules:
            return sys.modules[mod[0]]

        module_path = os.path.join(mpath, f"{mod[0]}.py")
        spec = importlib.util.spec_from_file_location(mod[1], module_path)
        loaded_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(loaded_mod)
        globals()[mod[1]] = loaded_mod

#sorcery skip
def load_env(path = '.env'):
    """Load environment variables from a .env file.

    Parameters
    ----------
    path : str
        The path to the .env file (default: '.env').

    Returns
    -------
    str
        The path to the .env file if it was found and loaded successfully, or None if not.

    Raises
    ------
    SystemExit
        Exits the program with error code 999 if the .env file is not found or cannot be loaded.
    """
    if env_file := find_dotenv(path):
        if load_dotenv(env_file, override=True):
            return env_file

    print(f"CONFIG file not found OR LOADED: {env_file}")
    exit(999)

load_env()
log_setup()

##################
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# Consider removing the redundant logging statements in log_setup() that log the same message at different levels. A single log message at the appropriate level would be clearer and more efficient.
# The import_libs() function should include try/except error handling around module imports to gracefully handle import failures and provide meaningful error messages.
# Here's what I looked at during the review
# 🟢 General issues: all looks good
# 🟢 Security: all looks good
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
