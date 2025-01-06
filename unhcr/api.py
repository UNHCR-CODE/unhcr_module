"""
Overview
    This Python file (api.py) provides an API client for interacting with the Leonics system and a prospect API. 
    It handles authentication, data retrieval, and data submission. The primary functions facilitate getting an authentication token, 
    checking its validity, retrieving data within a specified timeframe, and sending data to a prospect API endpoint.

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

    get_prospect_url_key(local, out=False): 
        Determines the correct URL and API key for interacting with the prospect API based on whether the request is for a local or external 
        service and whether it's an inbound or outbound operation.

    api_in_prospect(df, local=True): 
        Sends data to the prospect API's inbound endpoint. It takes a Pandas DataFrame (df), converts it to JSON, and sends a POST request to the 
        appropriate URL with the necessary headers, including the API key. It includes basic error handling.
"""
from datetime import datetime, timedelta
import logging
import pandas as pd
import requests
from urllib3.exceptions import InsecureRequestWarning
import urllib3
# Suppress InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

import constants as const

def getAuthToken(dt = None):
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
    if res.status_code == 200:
        logging.info(res.text)
    else:
        if ('is not today' not in res.text):
            return None
        dt = datetime.now().date() + timedelta(days=1)
        res = checkAuth(dt, x+1)
        return res
    return token

def getData(start, end, token=None):
    if token is None:
        return None
    url = url = f"{const.LEONICS_BASE_URL}/data?API-KEY={token}&BEGIN={start}&END={end}&ZIP=NO"
    payload = {}
    headers = {}
    res = requests.request("GET", url, headers=headers, data=payload, verify=const.VERIFY)
    if res.status_code != 200:
        return None
    df = pd.DataFrame(res.json())
    df['DateTimeServer'] = df.apply(lambda row: str(row['A_DateServer']) + ' ' + str(row['A_TimeServer']), axis=1)
    df = df.drop(columns=['A_DateServer', 'A_TimeServer'])
    return df

def get_prospect_url_key(local, out=False):
    url = const.LOCAL_BASE_URL
    key = const.LOCAL_API_IN_KEY
    if local == False:
        url = const.BASE_URL
        key = const.API_OUT_KEY if out else const.API_IN_KEY
    elif out:
        key = const.LOCAL_API_OUT_KEY

    logging.debug(f'ZZZZZZZZZZZZZZZ\nlocal  {local}\n out {out}\nkey {key}\nZZZZZZZZZZZZZZ')
    return url, key

# sorcery: skip
def api_in_prospect(df, local=True, ):  # sourcery skip: extract-method
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

# Consider converting TODO comments into tracked issues/tickets and add more context about what needs to be done. Leaving untracked TODOs in production code makes it likely they'll be forgotten.
# The error handling in api_in_prospect() and other functions could be more specific. Consider catching and handling specific exceptions (e.g., RequestException, ConnectionError) rather than using a broad Exception catch.
# Replace the debug logging statement containing 'ZZZZ' with a more descriptive and professional message that clearly indicates what's being logged.
# Here's what I looked at during the review
# 🟡 General issues: 2 issues found
# 🟢 Security: all looks good
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/api.py:55

# suggestion(code_refinement): Recursive authentication check could be simplified
#     )

# # sorcery: skip
# def checkAuth(dt=None, x=0):
#     #TODO check 2 times as date maybe one day off due to tz
#     if x > 2:
# Consider replacing the recursive approach with a more straightforward date validation mechanism that doesn't rely on multiple recursive calls.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/api.py:123

# suggestion(bug_risk): Implement more specific exception handling
#         }

#         return requests.request("POST", url, headers=headers, data=data, verify=const.VERIFY)
#     #TODO more specific error trapping
#     except Exception as e:
#         logging.error('api_in_prospect ERROR', e)
# Replace generic exception handling with specific exception types to improve error diagnostics and handling.