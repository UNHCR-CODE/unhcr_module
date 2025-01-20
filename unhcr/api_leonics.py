"""
Overview
    This Python file (api_leonics.py) acts as an API client for interacting with the Leonics system. Its primary functions are handling 
    authentication, retrieving data within a specified timeframe, and submitting data to a prospect API endpoint. 
    The client retrieves an authentication token, validates it, uses it to fetch data, and then sends this data to another system.

Key Components
    getAuthToken(dt=None): 
        Retrieves an authentication token from the Leonics system. It takes an optional date parameter (dt) for specifying the current date. 
        If no date is provided, it defaults to the current date. The function constructs the authentication payload, including system credentials 
        and the provided date, and sends a POST request to the /auth endpoint.

    checkAuth(dt=None, x=0): 
        Checks the validity of the authentication token. It attempts to retrieve a token using getAuthToken(). 
        If successful, it verifies the token against the /check_auth endpoint. 
        It handles potential date-related issues by recursively calling itself with the next day's date if the token is invalid due to a date mismatch. 
        Includes a retry mechanism (up to 3 times) to handle potential transient errors.

    getData(start, end, token=None): 
        Retrieves data from the Leonics system within a specified time range using a valid authentication token. 
        It constructs the data request URL with start and end times and sends a GET request to the /data endpoint. 
        The retrieved data is parsed into a Pandas DataFrame and preprocessed to combine date and time columns. 
        It also includes code to send the retrieved data to a prospect API endpoint.
"""

from datetime import datetime, timedelta
import json
import logging
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import urllib3

# Suppress InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

from unhcr import constants as const

if const.LOCAL:  # testing with local python files
    const, *rest = const.import_local_libs(mods=[["constants", "const"]])


def getAuthToken(dt=None):
    """
    Retrieves an authentication token from the Leonics system. It takes an optional date parameter (dt) for specifying the current date.
    If no date is provided, it defaults to the current date. The function constructs the authentication payload, including system credentials
    and the provided date, and sends a POST request to the /auth endpoint.

    Parameters
    ----------
    dt: datetime.date
        The date to use for authentication. If not provided, defaults to the current date.

    Returns
    -------
    requests.Response
        The response object containing the authentication token in its JSON content.
    """
    if dt is None:
        dt = datetime.now().date()
    logging.info(f"Getting auth token for date: {dt}")
    # TODO this is not hardcoded --- constants.py gets them from the environ
    payload = {
        "SystemCode": "LEONICS",
        "CurrentDate": dt.isoformat(),
        "SiteId": "unhcr-001",
        "UserCode": const.LEONICS_USER_CODE,
        "Key": const.LEONICS_KEY,
    }  # sorcery: skip
    headers = {"Content-Type": "application/json"}
    return requests.post(
        f"{const.LEONICS_BASE_URL}/auth",
        json=payload,
        headers=headers,
        verify=const.VERIFY,
    )


# sorcery: skip
def checkAuth(dt=None, x=0):
    # TODO check 2 times as date maybe one day off due to tz
    """
    Checks the validity of the authentication token. It attempts to retrieve a token using getAuthToken().
    If successful, it verifies the token against the /check_auth endpoint.
    It handles potential date-related issues by recursively calling itself with the next day's date if the token is invalid due to a date mismatch.
    Includes a retry mechanism (up to 3 times) to handle potential transient errors.

    Parameters
    ----------
    dt: datetime.date
        The date to use for authentication. If not provided, defaults to the current date.
    x: int
        The number of retry attempts.

    Returns
    -------
    str or None
        The authentication token if valid, None otherwise.
    """
    if x > 2:
        return None
    res = getAuthToken(dt)
    if res.status_code != 200:
        return None

    # TODO if format changes this will break
    # sorcery: skip
    #Leonics does not send valid json -- res.json() is a string
    #print(type(res.json())
    j = json.loads('{'+res.text.replace('KEY: ','KEY": "') +'}')
    token = j.get("API-KEY") 
    url = url = f"{const.LEONICS_BASE_URL}/check_auth?API-KEY={token}"
    payload = {}
    headers = {}
    res = requests.request(
        "GET", url, headers=headers, data=payload, verify=const.VERIFY
    )
    if res.status_code != 200:
        if "is not today" not in res.text:
            return None
        dt = datetime.now().date() + timedelta(days=1)
        res = checkAuth(dt, x + 1)
        return res
    return token


def getData(start, end, token=None):
    """
    Retrieves data from the Leonics system within a specified time range. It requires a valid authentication token.
    It constructs the data request URL with the start and end times and sends a GET request to the /data endpoint.
    The retrieved data is parsed into a Pandas DataFrame and preprocessed to combine date and time columns.
    Note that the Leonics API allows getting data up to 11 days old.

    Parameters
    ----------
    start : str
        The start date of the time range in the format 'YYYYMMDD'.
    end : str
        The end date of the time range in the format 'YYYYMMDD'.
    token : str
        The authentication token obtained from the /auth endpoint.

    Returns
    -------
    pd.DataFrame or None
        The Pandas DataFrame containing the requested data, or None if the request fails.
    """
    if token is None:
        return None, "You must provide a token"
    url = (
        f"{const.LEONICS_BASE_URL}/data?API-KEY={token}&BEGIN={start}&END={end}&ZIP=NO"
    )
    payload = {}
    headers = {}
    try:
        res = requests.request(
            "GET", url, headers=headers, data=payload, verify=const.VERIFY
        )
        if res.status_code != 200:
            return None, res.status_code
        df = pd.DataFrame(res.json())
        df["DateTimeServer"] = df.apply(
            lambda row: str(row["A_DateServer"]) + " " + str(row["A_TimeServer"]),
            axis=1,
        )
        df = df.drop(columns=["A_DateServer", "A_TimeServer"])
        return df, None
    except Exception as e:
        logging.error("Leonics getData ERROR:", e)
        return None, e

########################################
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# There appears to be incomplete/dead code at the bottom of the file related to prospect API integration. This should either be completed or removed.
# Consider standardizing the error handling approach across functions - getData() returns (None, error_message) tuples while checkAuth() just returns None. A consistent pattern would improve maintainability.
# Here's what I looked at during the review
# 🟡 General issues: 4 issues found
# 🟡 Security: 1 issue found
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:_UNHCR\CODE\unhcr_module\unhcr\api_leonics.py:78

# suggestion(code_refinement): Recursive retry mechanism could be simplified


# # sorcery: skip
# def checkAuth(dt=None, x=0):
#     # TODO check 2 times as date maybe one day off due to tz
#     """
# The recursive approach for handling date mismatches could be replaced with a more straightforward iterative retry mechanism. Consider using a while loop or a dedicated retry decorator to make the error handling more explicit and easier to read.

# Suggested implementation:

# def checkAuth(dt=None, max_retries=2):
#     """
#     Authenticate with Leonics API, with built-in retry mechanism for date-related issues.

#     Args:
#         dt (datetime, optional): Date to use for authentication. Defaults to None.
#         max_retries (int, optional): Maximum number of retry attempts. Defaults to 2.

#     Returns:
#         requests.Response: Authentication response from Leonics API
#     """
#     for attempt in range(max_retries + 1):
#         try:
#             # Attempt authentication with potentially adjusted date
#             adjusted_dt = dt if dt is not None else None
#             response = getAuthToken(dt=adjusted_dt)

#             # Check if authentication was successful
#             if response.status_code == 200:
#                 return response

#             # If not successful, adjust date for next attempt
#             dt = dt - timedelta(days=1) if dt is not None else None

#         except Exception as e:
#             # Log or handle specific authentication errors if needed
#             if attempt == max_retries:
#                 raise

#     # This should not be reached due to max_retries, but added for completeness
#     raise AuthenticationError("Failed to authenticate after multiple attempts")

# You'll need to import timedelta from the datetime module
# Consider defining a custom AuthenticationError exception
# Add appropriate error logging if not already present
# Ensure getAuthToken can handle a None or adjusted date parameter
# Recommended imports at the top of the file:

# from datetime import timedelta, datetime

# class AuthenticationError(Exception):
#     """Custom exception for authentication failures"""
#     pass
# Resolve
# e:_UNHCR\CODE\unhcr_module\unhcr\api_leonics.py:122

# suggestion(edge_case_not_handled): Potential error handling improvement in getData
#     return token


# def getData(start, end, token=None):
#     """
#     Retrieves data from the Leonics system within a specified time range. It requires a valid authentication token.
# The function returns multiple types (DataFrame and error message/code), which can lead to inconsistent usage. Consider using a more structured error handling approach, such as raising custom exceptions or returning a Result/Either type.

# Suggested implementation:

# from urllib3.exceptions import InsecureRequestWarning
# import urllib3
# from typing import Optional, Union
# from dataclasses import dataclass

# class LeonicsAPIError(Exception):
#     """Custom exception for Leonics API related errors."""
#     def __init__(self, message: str, status_code: Optional[int] = None):
#         self.message = message
#         self.status_code = status_code
#         super().__init__(self.message)

# @dataclass
# class LeonicsResult:
#     """Wrapper class for Leonics API response."""
#     data: Optional[pd.DataFrame] = None
#     error: Optional[LeonicsAPIError] = None

#     @property
#     def is_success(self) -> bool:
#         return self.error is None

# def getData(start: datetime, end: datetime, token: Optional[str] = None) -> LeonicsResult:
#     """
#     Retrieves data from the Leonics system within a specified time range.

#     Args:
#         start (datetime): Start time for data retrieval
#         end (datetime): End time for data retrieval
#         token (Optional[str], optional): Authentication token. Defaults to None.

#     Returns:
#         LeonicsResult: A result object containing either the DataFrame or an error
#     """

#     try:
#         if res.status_code != 200:
#             if "is not today" not in res.text:
#                 return LeonicsResult(error=LeonicsAPIError(
#                     f"API request failed with status {res.status_code}", 
#                     res.status_code
#                 ))

#             # Retry authentication for tomorrow's date
#             dt = datetime.now().date() + timedelta(days=1)
#             auth_result = checkAuth(dt, x + 1)

#             if isinstance(auth_result, LeonicsResult):
#                 return auth_result

#             return LeonicsResult(error=LeonicsAPIError("Authentication retry failed"))

#         # Successful data retrieval
#         return LeonicsResult(data=pd.DataFrame())  # Assuming you'll populate the DataFrame here

#     except Exception as e:
#         logging.error(f"Unexpected error in getData: {e}")
#         return LeonicsResult(error=LeonicsAPIError(str(e)))

# You'll need to update other functions that call getData() to handle the new LeonicsResult return type
# Modify the DataFrame creation logic in the existing code to work with the new structure
# Update error handling in calling functions to check result.is_success and handle result.error appropriately
# Consider adding more specific error types if needed (e.g., AuthenticationError, DataRetrievalError)
# Resolve
# e:_UNHCR\CODE\unhcr_module\unhcr\api_leonics.py:142

# issue(security): Incomplete error handling for token validation
#     pd.DataFrame or None
#         The Pandas DataFrame containing the requested data, or None if the request fails.
#     """
#     if token is None:
#         return None, "You must provide a token"
#     url = (
# The token validation is too simplistic. There should be additional checks for token expiration, format validation, and more robust error reporting when a token is invalid or cannot be obtained.

# Resolve
# e:_UNHCR\CODE\unhcr_module\unhcr\api_leonics.py:164

# issue(bug_risk): Broad exception handling is risky
#         return df, None
#     except Exception as e:
#         logging.error("Leonics getData ERROR:", e)
#         return None, e

#     if df is None:
# Catching all exceptions without specific error handling can mask critical issues. Replace the broad exception catch with more specific exception handling to ensure proper error diagnosis and logging.

# Resolve
# e:_UNHCR\CODE\unhcr_module\unhcr\api_leonics.py:163

# suggestion(code_refinement): Logging could be more informative
#         df = df.drop(columns=["A_DateServer", "A_TimeServer"])
#         return df, None
#     except Exception as e:
#         logging.error("Leonics getData ERROR:", e)
#         return None, e

# Include more context in the error logging, such as the input parameters (start, end, token) to aid in debugging and tracing the source of errors.