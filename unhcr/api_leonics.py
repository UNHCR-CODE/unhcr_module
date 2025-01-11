"""
Overview
    This Python file (api_leonics.py) provides an API client for interacting with the Leonics system. It handles authentication, data retrieval, 
    and data submission. The core functionality revolves around obtaining an authentication token, validating the token, 
    fetching data within a specified time frame, and sending data to a prospect API endpoint.

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
        Retrieves data from the Leonics system within a specified time range. It requires a valid authentication token. 
        It constructs the data request URL with the start and end times and sends a GET request to the /data endpoint. 
        The retrieved data is parsed into a Pandas DataFrame and preprocessed to combine date and time columns.
"""
from datetime import datetime, timedelta
import logging
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import urllib3
# Suppress InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

from unhcr import constants as const

if const.LOCAL: # testing with local python files
    const, *rest = const.import_local_libs(mods=[ ["constants", "const"]])

def getAuthToken(dt = None):
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
    logging.info(f'Getting auth token for date: {dt}')
    #TODO this is not hardcoded --- constants.py gets them from the environ
    payload = {"SystemCode": "LEONICS", "CurrentDate": dt.isoformat(), "SiteId": "unhcr-001", "UserCode": const.LEONICS_USER_CODE, "Key": const.LEONICS_KEY}  # sorcery: skip
    headers = {'Content-Type': 'application/json'}
    return requests.post(
        f"{const.LEONICS_BASE_URL}/auth", json=payload, headers=headers, verify=const.VERIFY
    )

# sorcery: skip
def checkAuth(dt=None, x=0):
    #TODO check 2 times as date maybe one day off due to tz
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
    token = res.json()[9:]
    url = url = f"{const.LEONICS_BASE_URL}/check_auth?API-KEY={token}"
    payload = {}
    headers = {}
    res = requests.request("GET", url, headers=headers, data=payload, verify=const.VERIFY)
    if res.status_code != 200:
        if ('is not today' not in res.text):
            return None
        dt = datetime.now().date() + timedelta(days=1)
        res = checkAuth(dt, x+1)
        return res
    return token

def getData(start, end, token=None):
    """
    Retrieves data from the Leonics system within a specified time range using a valid authentication token.
    Constructs a URL with the provided start and end times and sends a GET request to the /data endpoint.
    Parses the retrieved JSON data into a Pandas DataFrame and preprocesses it by combining date and time
    server columns into a single column named 'DateTimeServer'.

    Parameters
    ----------
    start : str
        The start date for data retrieval in the format 'YYYYMMDD'.
    end : str
        The end date for data retrieval in the format 'YYYYMMDD'.
    token : str, optional
        The authentication token required for accessing the data. If not provided, the function returns None.

    Returns
    -------
    pd.DataFrame or None
        A Pandas DataFrame containing the retrieved data with a combined 'DateTimeServer' column, or None
        if the request fails or the token is not provided.
    """

    if token is None:
        return None, 'You must provide a token'
    url = f"{const.LEONICS_BASE_URL}/data?API-KEY={token}&BEGIN={start}&END={end}&ZIP=NO"
    payload = {}
    headers = {}
    try:
        res = requests.request("GET", url, headers=headers, data=payload, verify=const.VERIFY)
        if res.status_code != 200:
            return None, res.status_code
        df = pd.DataFrame(res.json())
        df['DateTimeServer'] = df.apply(lambda row: str(row['A_DateServer']) + ' ' + str(row['A_TimeServer']), axis=1)
        df = df.drop(columns=['A_DateServer', 'A_TimeServer'])
        return df, None
    except Exception as e:
        logging.error('Leonics getData ERROR:', e)
        return None, e
    """
    Sends data to the prospect API's inbound endpoint.

    This function takes a Pandas DataFrame (df), converts it to JSON, and sends a POST request to the
    appropriate URL with the necessary headers, including the API key. It includes basic error handling.

    Parameters
    ----------
    df : pd.DataFrame
        The Pandas DataFrame containing the data to be sent to the Prospect API.
    local : bool, optional
        A flag indicating whether to send data to the local or external Prospect API. When True, sends to the local API. Default is True.

    Returns
    -------
    requests.Response or None
        The response from the Prospect API, or None if the request fails.
    """

    if df is None:
        return df
    try:
        url, key = get_prospect_url_key(local)
        url += '/v1/in/custom'

        json_str = df.to_json(orient='records')
        data = '{"data": ' + json_str +'}'

        headers = {
        'Authorization': f'Bearer {key}',
        'Content-Type': 'application/json'
        }

        return requests.request("POST", url, headers=headers, data=data, verify=const.VERIFY)
    #TODO more specific error trapping
    except Exception as e:
        logging.error('api_in_prospect ERROR', e)
        return None

########################################
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# There appears to be a misplaced function definition inside the getData docstring at the end of the file. This needs to be fixed as it's currently unreachable code.
# Consider removing the global SSL verification disable and properly handle certificates instead, as this is a security concern.
# The generic try-except block should be replaced with specific exception handling to properly handle different error cases.
# Here's what I looked at during the review
# 🟡 General issues: 2 issues found
# 🟢 Security: all looks good
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:_UNHCR\CODE\unhcr_module\unhcr\api_leonics.py:137

# suggestion(performance): Inefficient datetime column creation
#     if res.status_code != 200:
#         return None
#     df = pd.DataFrame(res.json())
#     df['DateTimeServer'] = df.apply(lambda row: str(row['A_DateServer']) + ' ' + str(row['A_TimeServer']), axis=1)
#     df = df.drop(columns=['A_DateServer', 'A_TimeServer'])
#     return df
# Using DataFrame.apply() with a lambda function can be slow for large datasets. Consider using more performant methods like pd.to_datetime() or vectorized string operations.

# Resolve
# e:_UNHCR\CODE\unhcr_module\unhcr\api_leonics.py:175

# issue(bug_risk): Overly broad exception handling

#         return requests.request("POST", url, headers=headers, data=data, verify=const.VERIFY)
#     #TODO more specific error trapping
#     except Exception as e:
#         logging.error('api_in_prospect ERROR', e)
#         return None
# Catching all exceptions without specific error handling can mask important errors and make debugging difficult. Implement more granular exception handling.