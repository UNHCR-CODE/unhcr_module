import bisect
from datetime import datetime, timedelta, UTC
import json
import logging
import os
import pandas as pd
import requests

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

INVERTERS = [
    {
        "ABUJA": [
            {"deviceSn": "2309184208", "deviceId": 240031597},
            {"deviceSn": "2309200109", "deviceId": 240030990},
            {"deviceSn": "2309198007", "deviceId": 240031585},
            {"deviceSn": "2309194017", "deviceId": 240031431},
            {"deviceSn": "2306296090", "deviceId": 240030831},
            {"deviceSn": "2309182126", "deviceId": 240031338},
            {"deviceSn": "2309200154", "deviceId": 240030917},
        ]
    },
    {
        "OGOJA_GH": [
            {"deviceSn": "2309182179", "deviceId": 240480864},
            {"deviceSn": "2309198004", "deviceId": 240481013},
            {"deviceSn": "2309188195", "deviceId": 240481631},
            {"deviceSn": "2309208178", "deviceId": 240481437},
            {"deviceSn": "2309200145", "deviceId": 240481716},
        ]
    },
    {
        "OGOJA": [
            {"deviceSn": "2309194019", "deviceId": 240321506},  # not in current API
            {
                "deviceSn": "2408202575",
                "deviceId": 240897791,
            },
            {"deviceSn": "2405052283", "deviceId": 240844835},
            {"deviceSn": "2309188295", "deviceId": 240295039},
            {"deviceSn": "2309188310", "deviceId": 240294993},
            {"deviceSn": "2309188199", "deviceId": 240321874},
        ]
    },
    {
        "LAGOS": [
            {
                "deviceSn": "2401110046",
                "deviceId": 240033551,
            },
            {"deviceSn": "2306296095", "deviceId": 240033630},
            {"deviceSn": "2306290070", "deviceId": 240033712},
        ]
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


def get_weather_data(date_str, devices):
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

                    if d["name"] == "Environment Temp":
                        info["temp_c"] = utils.str_to_float_or_zero(d["value"])
                    if d["name"] == "Module Temp":
                        info["panel_temp"] = utils.str_to_float_or_zero(d["value"])
                    if d["name"] == "Environment Humidity":
                        info["humidity"] = utils.str_to_float_or_zero(d["value"])
                    if d["name"] == "Daily Rainfall":
                        info["rainfall"] = utils.str_to_float_or_zero(d["value"])

                    if d["name"] == "Irradiance":
                        info["irr"] = str(d["value"])
                    if d["name"] == "Daily Irradiance":
                        info["daily_irr"] = str(d["value"])

                if first:
                    data.append(info)
                else:
                    index = bisect.bisect_left(epoch_values, info["epoch"])
                    # Insert the new item at the correct index
                    data.insert(index, info)
                    epoch_values = [item["epoch"] for item in data]
        else:
            logging.info("??????????????????????????????????????????????????")
    if len(data) == 0:
        return None
    df = pd.DataFrame(data)
    
    #df["ts"] = df["ts"].dt.tz_localize(None)
    # # If the datetime index has timezone information
    # if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
    #     df.index = df.index.tz_localize(None)
    return df