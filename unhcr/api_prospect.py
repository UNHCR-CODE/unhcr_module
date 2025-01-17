"""
Overview
    This file (api_prospect.py) provides a Python API client for interacting with the Prospect system. 
    It handles authentication and facilitates data retrieval within a specified timeframe, as well as data submission 
    to the Prospect API. It supports interacting with both local and external Prospect instances.

Key Components
    get_prospect_url_key(local, out=False): 
        Determines the correct URL and API key for interacting with the Prospect API based on whether the request is 
        for a local or external service and whether it's an inbound or outbound operation. The local flag indicates 
        whether to use local settings, and the out flag specifies whether to retrieve the outgoing API key.

    api_in_prospect(df, local=True): 
        Sends data to the prospect API's inbound endpoint. It takes a Pandas DataFrame (df), converts it to JSON, 
        and sends a POST request to the appropriate URL with the necessary headers, including the API key. 
        It includes basic error handling. The local flag determines whether to send data to the local or external API.

    get_prospect_last_data(response): 
        Parses the Prospect API response and extracts the latest timestamp from the returned data. This timestamp is
        used to retrieve newer records in subsequent calls.

    prospect_get_start_ts(local, start_ts=None): 
        Retrieves data from the Prospect API and determines the starting timestamp for data synchronization. 
        If start_ts is not provided, it fetches the latest timestamp from the API. The local flag indicates whether to 
        interact with the local or external Prospect instance.
"""
import json
import logging
import requests

from unhcr import constants as const

if const.LOCAL: # testing with local python files
    const, *rest = const.import_local_libs(mods=[ ["constants", "const"]])

def get_prospect_url_key(local, out=False):
    """
    Retrieves the Prospect API URL and key based on the provided flags.

    Parameters
    ----------
    local : bool
        A flag indicating whether to retrieve data from the local or external
        Prospect API. When True, retrieves from the local API.
    out : bool, optional
        A flag indicating whether to retrieve the outgoing API key. Default is
        False.

    Returns
    -------
    str, str
        A tuple containing the URL and key for the Prospect API.
    """
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


def get_prospect_last_data(response):
    """
    Retrieves the latest timestamp from the Prospect API response.

    This function takes a Prospect API response, parses it, and returns the latest timestamp
    as a string in the format 'YYYY-MM-DD HH:MM:SS'.

    Args:
        response (requests.Response): The Prospect API response.

    Returns:
        str: The latest timestamp.

    """

    j = json.loads(response.text)
    # json.dumps(j, indent=2)
    # logging.info(f'\n\n{j['data'][0]}')
    res = ""
    idd = ""
    for d in j["data"]:
        if d["custom"]["DatetimeServer"] > res:
            res = d["custom"]["DatetimeServer"]
        if d["external_id"] > idd:
            idd = d["external_id"]
    return res


def prospect_get_start_ts(local, start_ts=None):
    """
    Retrieves data from the Prospect API and updates the MySQL database.

    This function constructs a URL and fetches data from the Prospect API using the provided
    function to get the necessary URL and API key. It then retrieves the latest timestamp
    from the API response, queries the MySQL database for newer records, and sends this data
    back to the Prospect API. If the API call fails, it logs an error and exits the program.

    Args:
        func (callable): A function that returns the API URL and key based on the 'local' flag.
        local (bool): A flag indicating whether to retrieve data from the local or external
                      Prospect API. When True, retrieves from the local API.
        start_ts (str, optional): The timestamp to start retrieval from. If not provided (default),
                                  retrieves the latest timestamp from the Prospect API.

    Raises:
        SystemExit: Exits the program if the Prospect API call fails.

    Logs:
        Various debug and informational logs, including headers, keys, URLs, and response
        statuses. Also logs errors if API calls or database operations fail.
    """
    url, key = get_prospect_url_key(local, out=True)
    sid = 1 if local else 421
    url += f"/v1/out/custom/?size=50&page=1&q[source_id_eq]={sid}&q[s]=created_at+desc"
    payload = {}
    headers = {
        "Authorization": f"Bearer {key}",
    }

    response = requests.request(
        "GET", url, headers=headers, data=payload, verify=const.VERIFY
    )
    if start_ts is None:
        start_ts = get_prospect_last_data(response)
    j = json.loads(response.text)
    # json.dumps(j, indent=2)
    logging.info(f"\n\n{key}\n{url}\n{start_ts}")
    return start_ts

########################################
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# Consider moving API keys to environment variables or a secure secrets management service rather than storing them in constants.py
# Replace generic Exception handling with specific exceptions (e.g., requests.exceptions.RequestException) to properly handle different failure modes
# Here's what I looked at during the review
# 🟢 General issues: all looks good
# 🟡 Security: 2 issues found
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:91

# suggestion(security): Use specific exception handling instead of generic Exception

#         return requests.request("POST", url, headers=headers, data=data, verify=const.VERIFY)
#     #TODO more specific error trapping
#     except Exception as e:
#         logging.error('api_in_prospect ERROR', e)
#         return None
# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:89

# suggestion(security): Add response validation and error checking
#         'Content-Type': 'application/json'
#         }

#         return requests.request("POST", url, headers=headers, data=data, verify=const.VERIFY)
#     #TODO more specific error trapping
#     except Exception as e:
