"""
   Overview:
    get_weather_data: Retrieves weather data from the Solarman API.
    get_devices: Retrieves a list of devices from the Solarman API.
    get_inverters: Retrieves a list of inverters from the Solarman API.
    get_realtime_data: Retrieves real-time data from the Solarman API.
    get_historical_data: Retrieves historical data from the Solarman API.
    get_energy_data: Retrieves energy-related data from the Solarman API.
    get_alarm_data: Retrieves alarm-related data from the Solarman API.
    get_device_info: Retrieves detailed information about a specific device from the Solarman API.
    get_site_info: Retrieves detailed information about a specific site from the Solarman API.
"""

import bisect
from datetime import datetime, UTC
import json
import logging
import pandas as pd
import requests

from sqlalchemy import text
from unhcr import constants as const
from unhcr import utils

# local testing ===================================
if const.LOCAL:  # testing with local python files
    const, utils, db, api_leonics, *rest = const.import_local_libs(
        mods=[
            ["constants", "const"],
            ["utils", "utils"],
            ["db", "db"],
            ["api_leonics", "api_leonics"],
        ]
    )

# Solarman API credentials (replace with your actual credentials)
APP_ID = const.SM_APP_ID
APP_SECRET = const.SM_APP_SECRET
BIZ_ACCESS_TOKEN = const.SM_BIZ_ACCESS_TOKEN
URL = const.SM_URL
TOKEN_URL = const.SM_TOKEN_URL
HISTORICAL_URL = const.SM_HISTORY_URL

# Constants for the Solarman API TODO: update by calling API
INVERTERS = [
    {
        "site": "ABUJA",
        "ABUJA": [
            {"deviceSn": "2309184208", "deviceId": 240031597},
            {"deviceSn": "2309200109", "deviceId": 240030990},
            {"deviceSn": "2309198007", "deviceId": 240031585},
            {"deviceSn": "2309194017", "deviceId": 240031431},
            {"deviceSn": "2306296090", "deviceId": 240030831},
            {"deviceSn": "2309182126", "deviceId": 240031338},
            {"deviceSn": "2309200154", "deviceId": 240030917},
        ],
        "table": "fuel_kwh_abuja",
        "fn": "ABUJA_OFFICE_DG1_and_DG2_TANK.csv",
        "label": "BIOHENRY - UNHCR ABUJA OFFICE DG1 and DG2",
    },
    {
        "site": "OGOJA_GH",
        "OGOJA_GH": [
            {"deviceSn": "2309182179", "deviceId": 240480864},
            {"deviceSn": "2309198004", "deviceId": 240481013},
            {"deviceSn": "2309188195", "deviceId": 240481631},
            {"deviceSn": "2309208178", "deviceId": 240481437},
            {"deviceSn": "2309200145", "deviceId": 240481716},
        ],
        "table": "fuel_kwh_ogoja_gh",
        "fn": "OGOJA_GH_DG1_and_DG2_TANK.csv",
        "label": "BIOHENRY - UNHCR OGOJA GUEST HOUSE DG1 AND DG2",
    },
    {
        "site": "OGOJA",
        "OGOJA": [
            {"deviceSn": "2309194019", "deviceId": 240321506},  # not in current API
            {"deviceSn": "2408202575", "deviceId": 240897791},
            {"deviceSn": "2405052283", "deviceId": 240844835},
            {"deviceSn": "2309188295", "deviceId": 240295039},
            {"deviceSn": "2309188310", "deviceId": 240294993},
            {"deviceSn": "2309188199", "deviceId": 240321874},
        ],
        "table": "fuel_kwh_ogoja",
        "fn": "OGOJA_OFFICE_DG1_and_DG2_TANK.csv",
        "label": "BIOHENRY – UNHCR OGOJA OFFICE DG1 and DG2",
    },
    {
        "site": "LAGOS",
        "LAGOS": [
            {"deviceSn": "2401110046", "deviceId": 240033551},
            {"deviceSn": "2306296095", "deviceId": 240033630},
            {"deviceSn": "2306290070", "deviceId": 240033712},
        ],
        "table": "fuel_kwh_lagos_office",
        "fn": "LAGOS_OFFICE_DG1_and_DG2_TANK.csv",
        "label": "BIOHENRY -UNHCR LAGOS OFFICE DG1 and DG2",
    },
]

SITE_ID = [
    {"ABUJA": 63086751},
    {"OGOJA": 63122873},
    {"OGOJA_GH": 63151411},
    {"LAGOS": 63087453},
]

SITE_LIST = [
    {
        "id": 63086751,
        "name": "UNHCR SOLAR- ABUJA",
        "locationLat": 9.057373333333334,
        "locationLng": 7.527456666666666,
        "locationAddress": "Asokoro Abuja",
        "regionNationId": 164,
        "regionLevel1": 2263,
        "regionLevel2": 27742,
        "regionLevel3": None,
        "regionLevel4": None,
        "regionLevel5": None,
        "regionTimezone": "Africa/Bangui",
        "type": "COMMERCIAL_ROOF",
        "gridInterconnectionType": "BATTERY_BACKUP",
        "installedCapacity": 54.45,
        "startOperatingTime": 1726527600.000000000,
        "stationImage": None,
        "createdDate": 1726590783.000000000,
        "batterySoc": 66.0,
        "networkStatus": "NORMAL",
        "generationPower": 12000.0,
        "lastUpdateTime": 1730387897.000000000,
        "contactPhone": None,
        "ownerName": None,
    },
    {
        "id": 63087453,
        "name": "LAGOS UNHCR PV",
        "locationLat": 6.443986666666666,
        "locationLng": 3.4104966666666665,
        "locationAddress": "Lagos",
        "regionNationId": 164,
        "regionLevel1": 2273,
        "regionLevel2": None,
        "regionLevel3": None,
        "regionLevel4": None,
        "regionLevel5": None,
        "regionTimezone": "Africa/Bangui",
        "type": "COMMERCIAL_ROOF",
        "gridInterconnectionType": "BATTERY_BACKUP",
        "installedCapacity": 39.05,
        "startOperatingTime": 1726599475.000000000,
        "stationImage": None,
        "createdDate": 1726599630.000000000,
        "batterySoc": 100.0,
        "networkStatus": "PARTIAL_OFFLINE",
        "generationPower": 443.0,
        "lastUpdateTime": 1730387919.000000000,
        "contactPhone": None,
        "ownerName": None,
    },
    {
        "id": 63122873,
        "name": "UNHCR SOLAR-OGOJA (OFFICE)",
        "locationLat": 6.64460417569606,
        "locationLng": 8.794103842595888,
        "locationAddress": "OGOJA GRA",
        "regionNationId": 164,
        "regionLevel1": 2257,
        "regionLevel2": 27650,
        "regionLevel3": None,
        "regionLevel4": None,
        "regionLevel5": None,
        "regionTimezone": "Africa/Bangui",
        "type": "COMMERCIAL_ROOF",
        "gridInterconnectionType": "BATTERY_BACKUP",
        "installedCapacity": 78.1,
        "startOperatingTime": 1727301600.000000000,
        "stationImage": None,
        "createdDate": 1727269316.000000000,
        "batterySoc": 96.0,
        "networkStatus": "NORMAL",
        "generationPower": 131.0,
        "lastUpdateTime": 1730387953.000000000,
        "contactPhone": None,
        "ownerName": None,
    },
    {
        "id": 63151411,
        "name": "UNHCR SOLAR-OGOJA (GH)",
        "locationLat": 6.6448633333333325,
        "locationLng": 8.793848333333333,
        "locationAddress": "OGOJA GRA",
        "regionNationId": 164,
        "regionLevel1": 2257,
        "regionLevel2": 27650,
        "regionLevel3": None,
        "regionLevel4": None,
        "regionLevel5": None,
        "regionTimezone": "Africa/Bangui",
        "type": "COMMERCIAL_ROOF",
        "gridInterconnectionType": "BATTERY_BACKUP",
        "installedCapacity": 78.1,
        "startOperatingTime": 1727862990.000000000,
        "stationImage": None,
        "createdDate": 1727863202.000000000,
        "batterySoc": 95.0,
        "networkStatus": "NORMAL",
        "generationPower": 13807.0,
        "lastUpdateTime": 1730387965.000000000,
        "contactPhone": None,
        "ownerName": None,
    },
]

WEATHER = {
    "ABUJA": {"deviceSn": "002502255400-001", "deviceId": 240093462},
    "LAGOS": {"deviceSn": "002502325494-001", "deviceId": 240355934},
    "OGOJA": {"deviceSn": "002502705488-001", "deviceId": 240464333},
    "OGOJA_GH": {"deviceSn": "002502295492-001", "deviceId": 240482343},
}

WEATHER_MAPPING = {
    "Environment Temp": ("temp_c", utils.str_to_float_or_zero),
    "Module Temp": ("panel_temp", utils.str_to_float_or_zero),
    "Environment Humidity": ("humidity", utils.str_to_float_or_zero),
    "Daily Rainfall": ("rainfall", utils.str_to_float_or_zero),
    "Irradiance": ("irr", str),
    "Daily Irradiance": ("daily_irr", str),
}


def get_weather_data(date_str, devices):
    """
    Retrieves weather data for specified devices and date.

    This function sends a POST request to the historical weather data API for each
    device in the provided list. It processes the response to extract relevant weather
    parameters and compiles them into a DataFrame.

    Parameters:
    date_str (str): The start date for data retrieval in 'YYYY-MM-DD' format.
    devices (list): A list of dictionaries, each containing 'deviceSn' and 'deviceId' keys.

    Returns:
    pd.DataFrame: A DataFrame containing the weather data, or None if no data is retrieved.
    """

    url = HISTORICAL_URL
    data = []
    first = True
    x = -1
    for device in devices:
        x += 1
        if x > 0:
            epoch_values = [item["epoch"] for item in data]
            first = False

        last_epoch = None
        logging.info(f"Device ID: {device}")

        payload = json.dumps(
            {
                "deviceSn": device["deviceSn"],
                "deviceId": device["deviceId"],
                "startTime": date_str,
                "endTime": "2099-01-01",
                "timeType": 1,
            }
        )
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "UNHCR_STEVE",
            "Authorization": f"Bearer {BIZ_ACCESS_TOKEN}",
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            j = json.loads(response.text)

            for item in j["paramDataList"]:
                # logging.info(item["collectTime"])
                e = round(int(item["collectTime"]) / 300) * 300
                e += 60 * 60  # add 1 hour for Africa
                if e == last_epoch:
                    continue
                last_epoch = e
                info = {
                    "device_id": str(j["deviceId"]),
                    "org_epoch": item["collectTime"],
                    "epoch": e,  # item["collectTime"]
                }

                wdata = []
                for x in range(len(item["dataList"])):
                    wdata.append(item["dataList"][x])
                for d in wdata:
                    if d["name"] == "SN":
                        item["sn"] = d["value"]
                        dt = datetime.fromtimestamp(
                            e, UTC
                        )  # depriciated datetime.utcfromtimestamp(e).strftime('%Y-%m-%d %H:%M')
                        info["ts"] = dt
                    elif d["name"] in WEATHER_MAPPING:
                        field, converter = WEATHER_MAPPING[d["name"]]
                        info[field] = converter(d["value"])

                if first:
                    data.append(info)
                else:
                    index = bisect.bisect_left(epoch_values, info["epoch"])
                    # Insert the new item at the correct index
                    data.insert(index, info)
                    epoch_values = [item["epoch"] for item in data]
        else:
            logging.error(
                f"get_weather_data ERROR: {response.status_code} {response.text}"
            )
            continue
    if not data:
        return None
    df = pd.DataFrame(data)

    # df["ts"] = df["ts"].dt.tz_localize(None)
    # # If the datetime index has timezone information
    # if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
    #     df.index = df.index.tz_localize(None)
    return df


def update_weather_db(df, epoch, engine):
    """
    Updates the weather database with new data.

    This function processes a DataFrame of weather data, adjusts data types, and inserts
    the data into the `solarman.weather` table. If any rows conflict on the primary key
    (device_id, ts), it updates the existing records with new values.

    Parameters:
    df (pd.DataFrame): The DataFrame containing weather data to be inserted or updated.
    engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine to connect to the database.

    Returns:
    tuple: A tuple containing the number of records inserted/updated and an error message, if any.
    """

    try:
        df["device_id"] = df["device_id"].astype("int32")  # int4
        df["org_epoch"] = df["org_epoch"].astype("int32")  # int4
        df["epoch"] = df["epoch"].astype("int32")  # int4
        df["ts"] = pd.to_datetime(df["ts"])  # Ensure timestamp format
        df["temp_c"] = df["temp_c"].astype("float32")  # float4
        df["panel_temp"] = df["panel_temp"].astype("float32")  # float4
        df["humidity"] = df["humidity"].astype("float32")  # float4
        df["rainfall"] = df["rainfall"].astype("float32")  # float4
        df["irr"] = df["irr"].astype("float32")  # float4
        df["daily_irr"] = df["daily_irr"].astype("float32")  # float4

        df = df[df["org_epoch"] >= epoch]

        df.to_sql(
            "temp_weather", engine, schema="solarman", if_exists="replace", index=False
        )

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                INSERT INTO solarman.weather (device_id, org_epoch, epoch, ts, temp_c, panel_temp, humidity, rainfall, irr, daily_irr)
                SELECT device_id, org_epoch, epoch, ts, temp_c, panel_temp, humidity, rainfall, irr, daily_irr FROM solarman.temp_weather
                ON CONFLICT (device_id, ts) DO UPDATE 
                SET org_epoch = EXCLUDED.org_epoch,
                    epoch = EXCLUDED.epoch,
                    temp_c = EXCLUDED.temp_c,
                    panel_temp = EXCLUDED.panel_temp,
                    humidity = EXCLUDED.humidity,
                    rainfall = EXCLUDED.rainfall,
                    irr = EXCLUDED.irr,
                    daily_irr = EXCLUDED.daily_irr;
            """
                )
            )
            conn.commit()
            return len(df), None
    except Exception as e:
        return None, f"update_weather_db ERROR: {e}"
