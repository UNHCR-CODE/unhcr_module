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
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import urllib3

# Suppress InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

from unhcr import app_utils
from unhcr import constants as const
from unhcr import err_handler

mods = [["app_utils", "app_utils"], ["constants", "const"], ["err_handler", "err_handler"]]
res = app_utils.app_init(mods=mods, log_file="unhcr.api_leonics.log", version="0.4.8", level="INFO", override=False)
logger = res[0]
if const.LOCAL:  # testing with local python files
    logger, app_utils, const, err_handler = res


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
    logger.info(f"Getting auth token for date: {dt}")
    # TODO this is not hardcoded --- constants.py gets them from the environ
    payload = {
        "SystemCode": "LEONICS",
        "CurrentDate": dt.isoformat(),
        "SiteId": "unhcr-001",
        "UserCode": const.LEONICS_USER_CODE,
        "Key": const.LEONICS_KEY,
    }  # sorcery: skip
    headers = {"Content-Type": "application/json"}
    return err_handler.error_wrapper(
        lambda: requests.post(
            f"{const.LEONICS_BASE_URL}/auth",
            json=payload,
            headers=headers,
            verify=const.VERIFY,
        )
    )

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
    res, err = getAuthToken(dt)
    if err or res.status_code != 200:
        return None

    # TODO if format changes this will break
    # sorcery: skip
    # Leonics does not send valid json -- res.json() is a string
    # print(type(res.json())
    j = json.loads("{" + res.text.replace("KEY: ", 'KEY": "') + "}")
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
        df["DatetimeServer"] = df.apply(
            lambda row: str(row["A_DateServer"]) + " " + str(row["A_TimeServer"]),
            axis=1,
        )
        df = df.drop(columns=["A_DateServer", "A_TimeServer"])
        return df, None
    except Exception as e:
        logger.error("Leonics getData ERROR:", e)
        return None, e
