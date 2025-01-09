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

def log_setup(level=None, log_file="unhcr.module.log"):
    """
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

    if level is None:
        # Parse the command-line arguments
        parser = argparse.ArgumentParser(description="Set logging level")
        parser.add_argument(
            "--log", 
            default="INFO", 
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help="Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)"
        )

        # Add default logging arguments if none provided
        l = len(sys.argv)
        if l == 1:  # handle vscode debugging
            sys.argv.extend(["--log", "INFO"])
        else:
            sys.argv[1] = '--log'
            sys.argv.append('INFO')

        args = parser.parse_args()
        level = args.log.upper()
    else:
        level = level.upper()

    # Validate logging level
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if level not in valid_levels:
        raise ValueError(f"Invalid logging level: {level}. Must be one of {valid_levels}.")

    # Create a formatter that outputs the log format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(getattr(logging, level))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Set the overall logging level
    logger.setLevel(getattr(logging, level))

    # Log the effective logging level
    logger.debug(f"DEBUG: Logging level set to {level}")
    logger.info(f"INFO: Logging level set to {level}")
    logger.warning(f"WARNING: Logging level set to {level}")
    logger.error(f"ERROR: Logging level set to {level}")

    return logger

# Example usage:
# if __name__ == "__main__":
#     logger = log_setup()
#     logger.info("Logger is successfully set up!")
#     logging.critical(f"CRITICAL:  Logging level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}")

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
