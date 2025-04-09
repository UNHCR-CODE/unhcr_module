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

import csv
from datetime import datetime, timedelta
from importlib.metadata import version
import fnmatch
import glob
import logging
import optparse
import os
import platform
import requests
import socket
import sys
import tkinter as tk
from tkinter import messagebox

# Global variable to store the selected file
selected_file = None
log_file = 'unhcr.utils.log'


def is_wsl():
    """Detect if running in Windows Subsystem for Linux (WSL)."""
    return (
        "WSL_DISTRO_NAME" in os.environ
        or "WSL_INTEROP" in os.environ
        or "microsoft" in platform.uname().release.lower()
    )

def is_running_on_azure():
    #!!! for now just detect linux or ubuntu
    return is_linux() or is_ubuntu()
    """Detect if running on Azure VM by querying the Azure Instance Metadata Service."""
    try:
        response = requests.get(
            "http://169.254.169.254/metadata/instance?api-version=2021-02-01",
            headers={"Metadata": "true"},
            timeout=2,
        )
        return response.status_code == 200
    except requests.RequestException:
        return False

def is_linux():
    return platform.system() == "Linux" and not is_wsl()

def is_ubuntu():
    try:
        with open("/etc/os-release") as f:
            return "ubuntu" in f.read().lower()
    except FileNotFoundError:
        return False

# Function to display the dropdown populated with file names from a directory
def show_dropdown_from_directory(directory, file_pattern_filter=None):
    global selected_file 
    selected_file = None  # Local variable to store selected file

    def on_submit():
        global selected_file
        selected_value = combo.get()  # Get the selected file name from the dropdown
        if selected_value:
            selected_file = os.path.join(directory, selected_value)
        else:
            selected_file = None
        top.quit()  # Stop the Tkinter event loop

    def on_cancel():
        global selected_file
        selected_file = None
        top.quit()  # Stop the Tkinter event loop

    # Validate directory existence
    if not os.path.isdir(directory):
        messagebox.showerror("Error", "Invalid directory")
        return None

    # Get the list of matching files
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    if file_pattern_filter:
        files = [f for f in files if fnmatch.fnmatch(f, file_pattern_filter)]

    if not files:
        messagebox.showerror("Error", f"No files found matching '{file_pattern_filter}' in '{directory}'.")
        return None

    # Create the top-level window
    top = tk.Toplevel()
    top.title(f"Select a File ({directory})")
    top.geometry("600x300") 

    tk.Label(top, text="Select a file:").pack(padx=20, pady=10)

    # Create a dropdown (ComboBox)
    combo = tk.StringVar()
    dropdown = tk.OptionMenu(top, combo, *files)
    dropdown.pack(padx=20, pady=10)
    combo.set(files[0])  # Default to first file

    # Add buttons
    tk.Button(top, text="Submit", command=on_submit).pack(padx=20, pady=5)
    tk.Button(top, text="Cancel", command=on_cancel).pack(padx=20, pady=5)

    top.grab_set()  # Make modal
    top.focus_set()

    top.mainloop()  # Start event loop

    top.destroy()  # Destroy window after loop ends
    return selected_file  # Return selected file


# # Directory containing the files (change this to your desired directory)
# res = show_dropdown_from_directory(r'E:\_UNHCR\CODE\DATA','unifier_gb*.csv')
# print(res, selected_file)


def msgbox_yes_no(title="Confirmation", msg="Are you sure?", auto_yes=None):
    # Create a basic Tkinter window (it won't appear)
    root = tk.Tk()
    root.withdraw()  # Hide the root window

    # Display a Yes/No prompt
    res = messagebox.askyesno(title=title, message=msg)
        # If auto_yes is True, simulate pressing the "Enter" key
    if auto_yes:
        # Use event_generate to simulate the Enter key press (which selects "Yes")
        root.event_generate('<Return>')
        res = True if auto_yes > 0 else False  # Simulate Yes response

    root.destroy()  # Destroy the root window after use
    return res


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


def log_setup(log_file, level="INFO", override=False):
    # Check if the logger already has handlers to prevent adding duplicates
    """
    Configure the logging for the module. If level is None, it will look for a command-line argument --log followed by the desired level, e.g., INFO, DEBUG, etc.
    If no level is provided, it defaults to INFO. The logs are outputted to both the console and a file named 'unhcr.module.log'.

    Parameters
    ----------
    level : str, optional
        The desired logging level, by default 'INFO'
    log_file : str
        The name of the log file, by default 'unhcr.module.log'
    override : bool, optional
        If True, it will clear the existing handlers and set up new ones, by default False

    Returns
    -------
    logging.Logger
        The configured logger
    """
        # Validate logging level
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if level not in valid_levels:
        raise ValueError(
            f"Invalid logging level: {level}. Must be one of {valid_levels}."
        )

    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    logger = logging.getLogger()
    if override:
        logger.handlers.clear()
    elif logger.hasHandlers():
        return logger  # Return the logger if it already has handlers

    args = create_cmdline_parser(level) if level is None else level.upper()
    level = args
    if os.getenv("DEBUG") == "1":
        level = "DEBUG"


    # Create a formatter that outputs the log format
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console handler
    console_handler = logging.StreamHandler()
    config_log_handler(console_handler, level, formatter, logger)
    # File handler
    # TODO .env setting
    log_path = '~/code/logs/' if is_ubuntu() else 'E:/_UNHCR/CODE/LOGS'
    log_path = os.path.expanduser(log_path)  # expands ~ to /home/you or C:\Users\you
    log_file_path = os.path.join(log_path, log_file)
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    config_log_handler(file_handler, level, formatter, logger)
    # Set the overall logging level
    logger.setLevel(getattr(logging, level))

    return logger


# init logging
log_setup(log_file)


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
    p = "%Y-%m-%dT%H:%M:%S"
    epoch = datetime(1970, 1, 1)
    e = (datetime.strptime(dt, p) - timedelta(hours=offset_hrs) - epoch).total_seconds()
    return int(e)


def filter_nested_dict(obj, val=-0.999, remove_empty=False):
    """
    Recursively remove all entries from a nested dict or list that have a value equal to `val`.
    
    Parameters:
        obj : dict or list or primitive
        val : value to remove
        remove_empty : bool
            If True, also remove empty dicts/lists after filtering.
    
    Returns:
        Cleaned object with specified values (and optionally empties) removed.
    """
    if isinstance(obj, dict):
        result = {
            k: filter_nested_dict(v, val, remove_empty)
            for k, v in obj.items()
            if v != val
        }
        # Remove entries that ended up empty
        if remove_empty:
            result = {k: v for k, v in result.items() if not (v == {} or v == [])}
        return result

    elif isinstance(obj, list):
        result = [
            filter_nested_dict(item, val, remove_empty)
            for item in obj
            if item != val
        ]
        # Remove empty items
        if remove_empty:
            result = [item for item in result if item != {} and item != []]
        return result

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
        logging.error(f"create_cmdline_parser ERROR: {e}")
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
        v_number = version(name)
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
    return parts1 <= parts2


def extract_data(data_list, site=None):
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
    label = None
    for key in data_list:
        label = None
        if site is None:
            if "site" in key and "site" in data_list:
                site = data_list["site"]
            else:
                return None, None, None, None
            table = data_list["table"] if "table" in data_list else None
            fn = data_list["fn"] if "fn" in data_list else None
            label = data_list["label"] if "label" in data_list else None
            return site, table, fn, label
        elif site == key["site"]:
            table = key["table"]
            fn = key["fn"]
            if "label" in key:
                label = key["label"]
            return site, table, fn, label


def concat_csv_files(input_file, output_file, append=True):
    """
    Concatenates CSV files from a glob pattern into one file.
    
    Parameters
    ----------
    input_file : str
        A glob pattern to match input CSV files.
    output_file : str
        The output file path to write the concatenated CSV data.
    append : bool, optional
        Whether to append the input CSV files if the output file already exists.
        Defaults to True.
    
    Notes
    -----
    Headers will be written only if the output file does not exist.
    """
    csv_files = sorted(glob.glob(input_file))
    file_exists = os.path.exists(output_file)
    mode = "a" if append and file_exists else "w"

    with open(output_file, "a", newline="") as out_file:
        writer = csv.writer(out_file)

        for file in csv_files:
            with open(file, "r", newline="") as in_file:
                reader = csv.reader(in_file)
                headers = next(reader)  # Read the header row

                if not file_exists:  # Write headers only if file doesn't exist
                    writer.writerow(headers)
                    file_exists = True  # After first write, we have headers

                writer.writerows(reader)  # Append rows

    logging.debug("CSV files merged and appended if existing!")
    # Rename processed files
    for file in csv_files:
        # Extract the directory and filename separately
        directory, filename = os.path.split(file)
        new_name = os.path.join(directory, f"processed_{filename}")
        os.rename(file, new_name)


def is_port_in_use(port, host='127.0.0.1'):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex((host, port)) == 0


def prospect_running():
    ports = [3000, 3001]
    in_use = []
    err = ''
    for port in ports:
        if is_port_in_use(port):
            in_use.append(port) #print(f"Port {port} is in use ✅")
        else:
            err += f"Port {port} is free ❌\n"
    if err == '':
        err = None
    return in_use, err


# def prospect_running(url="http://localhost:3000"):
#     """
#     Check if the docker container is running by sending a GET request to the server URL.

#     Parameters
#     ----------
#     url : str, optional
#         The URL of the server, by default "http://localhost:3000"

#     Returns
#     -------
#     bool
#         True if the server is running, False otherwise
#     """
    
#     try:
#         response = requests.get(url, timeout=(5, 10))
#         if response.status_code > 205:
#             logging.info(f"Server at {url} responded with status code: {response.status_code}")
#             return False
#         else:
#             return True
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Server at {url} is not responding. ERROR: {e}")
#         return False
