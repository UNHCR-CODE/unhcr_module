import inspect
import json
import logging
import traceback
import psycopg2
import requests


def log_err(err, file_name, func_name, line_number, msg=None):
    err_str = f"ERROR: {err} : {file_name} : {func_name} : {line_number}"
    if msg:
        err_str += f" : MSG: {msg} "

    logging.error(err_str)

def request(func, msg=None):
    try:
        res = func()
        res.raise_for_status()
        return res, None
    except requests.exceptions.HTTPError as e:
        err = f"HTTP ERROR: {e} : "
        if hasattr(e, 'response'):
            err += str(e.response)
    except requests.exceptions.ConnectionError as e:
        err = f"Connection Error: {e} : "
    except requests.exceptions.Timeout as e:
        err = f"Timeout Error: {e}  : "
    except requests.exceptions.RequestException as e:
        err = f"Request Error: {e} : "

    # Get caller information
    caller_frame = inspect.currentframe().f_back
    frame_info = inspect.getframeinfo(caller_frame)
    caller_function = frame_info.function
    caller_filename = frame_info.filename
    caller_lineno = frame_info.lineno

    log_err(err, caller_filename, caller_function, caller_lineno, msg)
    return None, err


def parse_json(func, msg=None):
    try:
        return func(), None
    except ValueError as e:
        err =f"Invalid response ERROR: {e}"
    except json.JSONDecodeError as e:
        err = f"Error decoding JSON ERROR: {e}"
    except TypeError as e:
        # Handle cases where we can't convert or slice the serial number
        err = f"Error with type conversion in serial processing ERROR: {e}"
    except IndexError as e:
        # Handle cases where the string slicing goes out of bounds
        err = f"Error with string slicing in serial processing ERROR: {e}"
    except KeyError as e:
        # Handle cases where an expected key is missing
        err = f"Missing expected key in data structure ERROR: {e}"

    # Get caller information
    caller_frame = inspect.currentframe().f_back
    frame_info = inspect.getframeinfo(caller_frame)
    caller_function = frame_info.function
    caller_filename = frame_info.filename
    caller_lineno = frame_info.lineno

    log_err(err, caller_filename, caller_function, caller_lineno)
    return None, err

def err_details(e):
    tb_lines = traceback.format_exc().split("\n")[:20]  # Always capture first 20 lines of the traceback
    tb_err = f"\nTRACE:" + "\n".join(tb_lines)
    try:
        raise e
    except psycopg2.OperationalError as e:
        err = f"Database connection error: {e}"
    except psycopg2.DatabaseError as e:
        err = f"Database query error: {e}"
    except psycopg2.InterfaceError as e:
        err = f"Database interface error: {e}"
    except psycopg2.ProgrammingError as e:
        err = f"Database programming error: {e}"
    except psycopg2.DataError as e:
        err = f"Data error: {e}"
    except MemoryError as e:
        err = f"Memory error: {e}"
    except ValueError as e:
        err =f"Invalid response ERROR: {e}"
    except json.JSONDecodeError as e:
        err = f"Error decoding JSON ERROR: {e}"
    except TypeError as e:
        # Handle cases where we can't convert or slice the serial number
        err = f"Error with type conversion in serial processing ERROR: {e}"
    except IndexError as e:
        # Handle cases where the string slicing goes out of bounds
        err = f"Error with string slicing in serial processing ERROR: {e}"
    except KeyError as e:
        # Handle cases where an expected key is missing
        err = f"Missing expected key in data structure ERROR: {e}"
    except requests.exceptions.HTTPError as e:
        err = f"HTTP ERROR: {e} : "
        if hasattr(e, 'response'):
            err += str(e.response)
    except requests.exceptions.ConnectionError as e:
        err = f"Connection Error: {e} : "
    except requests.exceptions.Timeout as e:
        err = f"Timeout Error: {e}  : "
    except requests.exceptions.RequestException as e:
        err = f"Request Error: {e} : "
    except Exception as e:
        err = f"Unexpected error: {e}"
    # Get caller information
    caller_frame = inspect.currentframe().f_back.f_back
    frame_info = inspect.getframeinfo(caller_frame)
    caller_function = frame_info.function
    caller_filename = frame_info.filename
    caller_lineno = frame_info.lineno
    err += tb_err
    log_err(err, caller_filename, caller_function, caller_lineno)

    return err


def error_wrapper(func,  msg=None):
    try:
        res = func()
        return res, None
    except Exception as e:
        return None, err_details(e)

