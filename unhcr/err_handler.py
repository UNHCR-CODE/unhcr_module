import inspect
import logging
import requests


class APIRequestError(Exception):
    """Base class for API request exceptions"""
    pass

class InvalidResponseError(APIRequestError):
    """Raised when the API response is invalid"""
    pass

class NetworkError(APIRequestError):
    """Raised when a network error occurs"""
    pass

class JSONDecodeError(APIRequestError):
    """Raised when there is an error decoding the JSON response"""
    pass

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

    logging.error(err, caller_filename, caller_function, caller_lineno)
    return None, err