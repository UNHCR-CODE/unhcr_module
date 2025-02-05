"""
Overview
    This file (api_prospect.py) provides a Python API client for interacting with the Prospect system. 
    It handles authentication and facilitates data retrieval within a specified timeframe, as well as data submission 
    to the Prospect API. It supports interacting with both local and external Prospect instances.

Key Components
    get_prospect_url_key(local=None, out=False): 
        Determines the correct URL and API key for interacting with the Prospect API based on whether the request is 
        for a local or external service and whether it's an inbound or outbound operation. The local flag indicates 
        whether to use local settings, and the out flag specifies whether to retrieve the outgoing API key.

    api_in_prospect(df, local=None): 
        Sends data to the prospect API's inbound endpoint. It takes a Pandas DataFrame (df), converts it to JSON, 
        and sends a POST request to the appropriate URL with the necessary headers, including the API key. 
        It includes basic error handling. The local flag determines whether to send data to the local or external API.

    get_prospect_last_data(response, key="datetimeserver"): 
        Parses the Prospect API response and extracts the latest timestamp from the returned data. This timestamp is
        used to retrieve newer records in subsequent calls.
"""

import json
import logging
import re
import requests

from unhcr import constants as const

if const.LOCAL:  # testing with local python files
    const, *rest = const.import_local_libs(mods=[["constants", "const"]])


def get_prospect_url_key(local=None, out=False):
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
    # AZURE
    if local is None:
        url = const.AZURE_BASE_URL
        if not out:
            key = const.AZURE_API_IN_KEY

    logging.debug(
        f"ZZZZZZZZZZZZZZZ\nlocal  {local}\n out {out}\nurl {url}\nZZZZZZZZZZZZZZ"
    )
    return url, key


def api_in_prospect(df, local=None):
    """
    Sends data to the prospect API's inbound custom endpoint.

    This function takes a Pandas DataFrame (df), converts it to JSON, and sends a POST request to the
    appropriate URL with the necessary headers, including the API key. It includes basic error handling.

    Parameters
    ----------
    df : pd.DataFrame
        The Pandas DataFrame containing the data to be sent to the Prospect API.
    local : bool, optional
        A flag indicating whether to send data to the local or external Prospect API. When True, sends to the local API.
        Default is None (AZURE).

    Returns
    -------
    requests.Response or None
        The response from the Prospect API, or None if the request fails.
    """

    def map_columns(df):
        # AZURE dependant on field order
        """
        Maps the columns of the dataframe to the correct column names in the
        correct order for the Azure API.

        Parameters
        ----------
        df : pd.DataFrame
            The Pandas DataFrame containing the data to be sent to the Azure API.

        Returns
        -------
        pd.DataFrame
            The mapped DataFrame with the columns in the correct order.
        """
        columns = [
            "In4",
            "In5",
            "In6",
            "In7",
            "In8",
            "Out4",
            "Out5",
            "Out6",
            "Out7",
            "Out8",
            "HVB1_SOC",
            "BDI1_Freq",
            "BDI2_Freq",
            "DCgen_RPM",
            "HVB1_Avg_V",
            "HVB1_Batt_I",
            "In3_door_sw",
            "In1_BDI_Fail",
            "DCgen_Max_RPM",
            "DCgen_Min_RPM",
            "Out1_CloseMC1",
            "Out2_StartGen",
            "DatetimeServer",
            "In2_ATS_status",
            "DCgen_Today_kWh",
            "DCgen_Total_kWh",
            "SCC1_PV_Current",
            "SCC1_PV_Voltage",
            "BDI1_Power_P1_kW",
            "BDI1_Power_P2_kW",
            "BDI1_Power_P3_kW",
            "BDI2_Power_P1_kW",
            "BDI2_Power_P2_kW",
            "BDI2_Power_P3_kW",
            "DCgen_Diode_Temp",
            "DCgen_Fuel_Level",
            "SCC1_Chg_Current",
            "SCC1_Chg_Voltage",
            "SCC1_PV_Power_kW",
            "ana2_Inv_room_RH",
            "ana5_Fuel_Level1",
            "ana6_Fuel_Level2",
            "BDI1_Batt_Voltage",
            "DCgen_Max_Current",
            "DCgen_Max_Voltage",
            "LoadPM_Import_kWh",
            "LoadPM_Total_P_kW",
            "SCC1_Chg_Power_kW",
            "SCC1_Today_PV_kWh",
            "ana4_Batt_room_RH",
            "BDI1_ACinput_P1_kW",
            "BDI1_ACinput_P2_kW",
            "BDI1_ACinput_P3_kW",
            "BDI2_ACinput_P1_kW",
            "BDI2_ACinput_P2_kW",
            "BDI2_ACinput_P3_kW",
            "DCgen_Ambient_Temp",
            "DCgen_Coolant_Temp",
            "DCgen_Oil_Pressure",
            "LoadPM_Power_P1_kW",
            "LoadPM_Power_P2_kW",
            "LoadPM_Power_P3_kW",
            "Out3_EmergencyStop",
            "SCC1_Todate_PV_kWh",
            "SCC1_Today_Chg_kWh",
            "ana1_Inv_Room_Temp",
            "BDI1_Total_Power_kW",
            "BDI2_Total_Power_kW",
            "DCgen_RPM_Frequency",
            "DCgen_Throttle_Stop",
            "FlowMeter_Fuel_Temp",
            "SCC1_Todate_Chg_kWh",
            "ana3_Batt_Room_Temp",
            "DCgen_Engine_Runtime",
            "BDI1_ACinput_Total_kW",
            "BDI2_ACinput_Total_kW",
            "DCgen_Alternator_Temp",
            "DCgen_Low_Current_Stop",
            "BDI1_ACinput_Voltage_L1",
            "BDI1_ACinput_Voltage_L2",
            "BDI1_ACinput_Voltage_L3",
            "BDI2_ACinput_Voltage_L1",
            "BDI2_ACinput_Voltage_L2",
            "BDI2_ACinput_Voltage_L3",
            "BDI2_Today_Batt_Chg_kWh",
            "DCgen_High_Voltage_Stop",
            "DCgen_Low_Voltage_Start",
            "LoadPM_Today_Import_kWh",
            "BDI1_Today_Supply_AC_kWh",
            "BDI2_Todate_Batt_Chg_kWh",
            "DCgen_Alternator_Current",
            "DCgen_Alternator_Voltage",
            "BDI1_Todate_Supply_AC_kWh",
            "DCgen_Alternator_Power_kW",
            "DCgen_LoadBattery_Current",
            "DCgen_LoadBattery_Voltage",
            "BDI2_Today_Batt_DisChg_kWh",
            "DCgen_LoadBattery_Power_kW",
            "BDI2_Todate_Batt_DisChg_kWh",
            "DCgen_StartingBatteryVoltage",
            "FlowMeter_Today_Fuel_consumption",
            "FlowMeter_Total_Fuel_consumption",
            "FlowMeter_Hourly_Fuel_consumptionRate",
        ]
        new_cols = df.columns.tolist()

        mapped = []
        for item in new_cols:
            # Find the closest match ignoring case
            match = next(
                (
                    a
                    for a in columns
                    if re.sub(r"_", "", a.lower()) == item.replace("_", "").lower()
                ),
                item,
            )
            mapped.append(match)
        print(len(mapped), len(new_cols), len(columns))

        for x in range(len(mapped)):
            df.rename(columns={new_cols[x]: mapped[x]}, inplace=True)
        return df

    if df is None:
        return df

    try:
        url, key = get_prospect_url_key(local)
        url += "/v1/in/custom"

        json_str = df.to_json(orient="records")
        data = '{"data": ' + json_str + "}"
        if len(data) == 0:
            return None
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

        return requests.request(
            "POST", url, headers=headers, data=data, verify=const.VERIFY
        )
    # TODO more specific error trapping
    except Exception as e:
        logging.error("api_in_prospect ERROR", e)
        return None


def get_prospect_last_data(response, key="datetimeserver"):
    """
    Retrieves the latest timestamp from the Prospect API response.

    This function takes a Prospect API response, parses it, and returns the latest timestamp
    as a string in the format 'YYYY-MM-DD HH:MM:SS'.

    Args:
        response (requests.Response): The Prospect API response.
        key (str, optional): The key to use for the timestamp column. Defaults to "datetimeserver".

    Returns:
        str: The latest timestamp.

    """

    j = json.loads(response.text)
    # print(json.dumps(j, indent=2))
    # logging.info(f'\n\n{j['data'][0]}')
    res = ""
    idd = ""
    try:
        if "custom" in j["data"][0] and "DatetimeServer" in j["data"][0]["custom"]:
            key = "DatetimeServer"
        for d in j["data"]:
            if d["external_id"] > idd:
                idd = d["external_id"]
                res = d["custom"][key]
    except Exception as e:
        logging.error(f"ERROR: get_prospect_last_data {e}")
    return res
