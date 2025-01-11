"""
Overview
    This file (api_prospect.py) provides a Python API client for interacting with the Prospect system. Its main purpose is to handle authentication, 
    retrieve data within a given timeframe, and submit data to the Prospect API.

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
# Hey there - I've reviewed your changes and found some issues that need to be addressed.

# Blocking issues:

# Hardcoded API key found. (e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:39)
# Hardcoded API keys found. (e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:42)
# Hardcoded API key found. (e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:44)
# Overall Comments:

# Replace the debug logging containing 'ZZZZ' with a more descriptive and professional message that clearly indicates what's being logged
# Consider catching and handling specific exceptions (e.g., RequestException, ConnectionError) rather than using a broad Exception catch in api_in_prospect()
# Here's what I looked at during the review
# 🟡 General issues: 1 issue found
# 🔴 Security: 3 blocking issues, 1 other issue
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:86

# suggestion(security): Use specific exception handling instead of generic Exception catch

#         return requests.request("POST", url, headers=headers, data=data, verify=const.VERIFY)
#     #TODO more specific error trapping
#     except Exception as e:
#         logging.error('api_in_prospect ERROR', e)
#         return None
# Catch specific exceptions like requests.exceptions.RequestException or ConnectionError to provide more precise error handling and logging

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:84

# suggestion(bug_risk): Consider adding response validation and error checking
#         'Content-Type': 'application/json'
#         }

#         return requests.request("POST", url, headers=headers, data=data, verify=const.VERIFY)
#     #TODO more specific error trapping
#     except Exception as e:
# Add checks for response status code and potential error conditions before returning the response

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:39

# issue(security): Hardcoded API key found.
#         A tuple containing the URL and key for the Prospect API.
#     """
#     url = const.LOCAL_BASE_URL
#     key = const.LOCAL_API_IN_KEY
#     if local == False:
#         url = const.BASE_URL
# The local API key is hardcoded in the get_prospect_url_key function. Avoid hardcoding sensitive information like API keys directly in your code. Consider storing them securely in environment variables or a dedicated secrets management service.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:42

# issue(security): Hardcoded API keys found.
#     key = const.LOCAL_API_IN_KEY
#     if local == False:
#         url = const.BASE_URL
#         key = const.API_OUT_KEY if out else const.API_IN_KEY
#     elif out:
#         key = const.LOCAL_API_OUT_KEY
# The API keys are hardcoded in the get_prospect_url_key function. Avoid hardcoding sensitive information like API keys directly in your code. Consider storing them securely in environment variables or a dedicated secrets management service.

# Resolve
# e:/_UNHCR/CODE/unhcr_module/unhcr/api_prospect.py:44

# issue(security): Hardcoded API key found.
#         url = const.BASE_URL
#         key = const.API_OUT_KEY if out else const.API_IN_KEY
#     elif out:
#         key = const.LOCAL_API_OUT_KEY

#     logging.debug(f'ZZZZZZZZZZZZZZZ\nlocal  {local}\n out {out}\nkey {key}\nZZZZZZZZZZZZZZ')
# The local API key is hardcoded in the get_prospect_url_key function. Avoid hardcoding sensitive information like API keys directly in your code. Consider storing them securely in environment variables or a dedicated secrets management service.
