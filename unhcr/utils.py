"""
Overview
    This Python file (utils.py) provides a utility function filter_json designed to recursively remove a specific value (-0.999 by default) 
    from JSON-like data structures (dictionaries and lists). This is likely used for data cleaning or preprocessing, where the value 
    -0.999 represents missing or invalid data.

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
"""

import argparse
import logging
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
        return {k: filter_json(v) for k, v in obj.items() if v != val}
    elif isinstance(obj, list):
        return [filter_json(item) for item in obj]
    else:
        return obj

def log_setup(level=None):
    """
    Set up logging for the module. If level is None, it parses the command-line arguments and sets the logging level to the specified value. 
    If no arguments are specified, it sets the logging level to INFO.
    Usage:
            full_test.py --log INFO
    :param level: the logging level (default None)
    """
    if level is None:
        # Parse the command-line arguments
        parser = argparse.ArgumentParser(description="Set logging level")
        parser.add_argument(
            "--log", 
            default="INFO", 
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help="Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)"
        )
        if len(sys.argv) == 1:
            sys.argv.append('--log')
            sys.argv.append('INFO')
        elif sys.argv[1] == 'none':
            sys.argv[1] = '--log'
            sys.argv.append('INFO')

        args = parser.parse_args()

    # Create a custom logger
    logger = logging.getLogger()

    # Create a formatter that outputs the log format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Create a console handler to log to the terminal
    console_handler = logging.StreamHandler()
    level = getattr(logging, args.log.upper(), logging.WARNING)
    console_handler.setLevel(level)  # Set level to INFO for console
    console_handler.setFormatter(formatter)

    # Create a file handler to log to a file
    file_handler = logging.FileHandler('unhcr.module.log')
    file_handler.setLevel(level)  # Set level to INFO for file
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Set the overall logging level
    logger.setLevel(level)
    logging.debug(f"DEBUG:  Logging level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}")
    logging.info(f"INFO:  Logging level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}")
    logging.warning(f"WARNING:  Logging level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}")
    logging.error(f"ERROR:  Logging level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}")
    logging.critical(f"CRITICAL:  Logging level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}")

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

##################
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# Function name mismatch: The docstring refers to filter_json but the implementation uses filter_nested_dict. These should be unified to prevent confusion.
# Bug in recursive call: filter_nested_dict needs to pass the val parameter in its recursive call (change filter_json(v) to filter_nested_dict(v, val))
# Here's what I looked at during the review
# 🟢 General issues: all looks good
# 🟢 Security: all looks good
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/utils.py:55

# issue(bug_risk): Incorrect recursive filtering of nested structures
#     dict | list
#         The filtered object
#     """
#     if isinstance(obj, dict):
#         return {k: filter_json(v) for k, v in obj.items() if v != val}
#     elif isinstance(obj, list):
# Modify recursive calls to preserve the filtering value: {k: filter_json(v, val) for k, v in obj.items() if v != val} and [filter_json(item, val) for item in obj]
