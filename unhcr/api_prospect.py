"""
Overview
    This file (api_prospect.py) provides a Python API client for interacting with the Prospect system. 
    It handles authentication and facilitates data retrieval within a specified timeframe, as well as data submission to the Prospect API. 
    It supports interacting with both local and external Prospect instances.

Key Components
    get_prospect_url_key(local, out=False): 
        Determines the correct URL and API key for interacting with the prospect API based on whether the request is for a local or external 
        service and whether it's an inbound or outbound operation.

    api_in_prospect(df, local=True): 
        Sends data to the prospect API's inbound endpoint. It takes a Pandas DataFrame (df), converts it to JSON, and sends a POST request to the 
        appropriate URL with the necessary headers, including the API key. It includes basic error handling.
"""
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

########################################
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# Consider moving API keys to environment variables or a secrets management service rather than storing them in constants to improve security
# Replace generic Exception handling with specific exception types (e.g., requests.exceptions.RequestException) to better handle different failure modes
# Here's what I looked at during the review
# 🟢 General issues: all looks good
# 🟡 Security: 3 issues found
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:89

# suggestion(security): Use specific exception handling instead of generic Exception

#         return requests.request("POST", url, headers=headers, data=data, verify=const.VERIFY)
#     #TODO more specific error trapping
#     except Exception as e:
#         logging.error('api_in_prospect ERROR', e)
#         return None
# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:87

# suggestion(security): Add response validation and error checking
#         'Content-Type': 'application/json'
#         }

#         return requests.request("POST", url, headers=headers, data=data, verify=const.VERIFY)
#     #TODO more specific error trapping
#     except Exception as e:
# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:41

# issue(security): Avoid hardcoding API keys, use secure storage
#     str, str
#         A tuple containing the URL and key for the Prospect API.
#     """
#     url = const.LOCAL_BASE_URL
#     key = const.LOCAL_API_IN_KEY
#     if local == False:
