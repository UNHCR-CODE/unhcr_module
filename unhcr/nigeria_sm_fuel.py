"""
Overview
    This script (full_test.py) is a test script that integrates data from the Leonics API into UNHCR's MySQL and Prospect databases. 
    It authenticates with the Leonics API, retrieves data, and updates the databases based on conditional flags. 
    The script includes some commented-out code suggesting past or future integration with an Oracle database and AWS S3. 
    It appears designed for testing the integration process.

Key Components
    solarman_api_historical(site="OGOJA", year=2024, month=12, day=21, days=1): 
        Retrieves historical data from the Solarman API for the given site and period. 
        It returns a list of dictionaries containing the retrieved data. 
        If the data is not found, it returns an empty list. 

    gen_file_from_csv(fn, dtStart, data, append=None): 
        Generates a CSV file from the given data. 
        It writes the data to the file and returns the file name and start date.

    extract_csv_data(site, fn, fn1, from_dt=None): 
        Extracts and processes data from a CSV file. 
        It reads the file, processes the data, and returns the processed data. 
        If the data is not found, it returns an empty list.
    
    concat_csv_files(dpath,fn, label): 
        Concatenates multiple CSV files downloaded from Galooli Pro View into a single file. 
        It reads the CSV files, processes the data, and returns the concatenated data. 
        If no data files are found, it returns False, otherwise True.

"""

import bisect
import csv
import decimal
import glob
import json
import logging
import math
import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytz
import requests
from unhcr import constants as const
from unhcr import utils

if const.LOCAL:  # testing with local python files
    const, utils, *rest = const.import_local_libs(
        mods=[["constants", "const"], ["utils", "utils"]]
    )

tz = "GMT"

# inverter data list of dictionaries of deviceSn and deviceId to be extracted
INVERTERS = [
    {
        "site": "ABUJA",
        "ABUJA": [
            {"deviceSn": "2309200154", "deviceId": 240030917},
            {"deviceSn": "2309184208", "deviceId": 240031597},
            {"deviceSn": "2309200109", "deviceId": 240030990},
            {"deviceSn": "2309198007", "deviceId": 240031585},
            {"deviceSn": "2309194017", "deviceId": 240031431},
            {"deviceSn": "2306296090", "deviceId": 240030831},
            {"deviceSn": "2309182126", "deviceId": 240031338},
        ],
        "table": "fuel_kwh_abuja",
        "fn": "ABUJA_OFFICE_DG1_and_DG2_TANK.csv",
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
    },
    {
        "site": "OGOJA",
        "OGOJA": [
            {"deviceSn": "2408202575", "deviceId": 240897791},
            {"deviceSn": "2405052283", "deviceId": 240844835},
            {"deviceSn": "2309188295", "deviceId": 240295039},
            {"deviceSn": "2309188310", "deviceId": 240294993},
            {"deviceSn": "2309188199", "deviceId": 240321874},
        ],
        "table": "fuel_kwh_ogoja_office",
        "fn": "OGOJA_OFFICE_DG1_and_DG2_TANK.csv",
    },
]

# BULK TANK EVENTS DATA
BULK = [
    {
        "site": "ABUJA",
        "table": "bulk_tank_events_abuja",
        "fn": "Bulk Tank Events Abuja.csv",
        "label": "BIOHENRY - UNHCR ABUJA OFFICE BULK TANK",
    },
    {
        "site": "OGOJA_GH",
        "table": "bulk_tank_events_ogoja_gh",
        "fn": "Bulk Tank Events Ogoja gh.csv",
        "label": "BIOHENRY - UNHCR OGOJA GUEST HOUSE BULK TANK",
    },
    {
        "site": "OGOJA",
        "table": "bulk_tank_events_ogoja",
        "fn": "Bulk Tank Events Ogoja.csv",
        "label": "BIOHENHRY - UNHCR OGOJA OFFICE BULK TANK",
    },
    {
        "site": "TAKUM",
        "table": "bulk_tank_events_takum",
        "fn": "Bulk Tank Events Takum.csv",
        "label": "BIOHENRY - UNHCR TAKUM OFFICE BULK TANK",
    },
]


def solarman_api_historical(site="OGOJA", year=2024, month=12, day=21, days=1):
    """
    Retrieves historical data from the Solarman API for the given site and period.

    Args:
        site (str): The site name. Defaults to 'OGOJA'.
        year (int): The year. Defaults to 2024.
        month (int): The month. Defaults to 12.
        day (int): The day. Defaults to 21.
        days (int): The number of days. Defaults to 1.

    Returns:
        list: A list of dictionaries with the following keys:
            - epoch: the epoch time
            - gen_pwr_w: the generator power in watts
            - load_pwr_w: the load power in watts
            - batt_chg_ttl_kwh: the total battery charge in kWh
            - batt_pwr_w: the battery power in watts
            - load_p1_w: the load power L1 in watts
            - load_p2_w: the load power L2 in watts
            - load_p3_w: the load power L3 in watts
            - gen_run_hrs: the generator run time in hours
            - batt_soc: the battery state of charge
            - batt_status: the battery status
            - deviceId: the device ID
    """
    mm = str(month).zfill(2)
    dd = str(day).zfill(2)
    yr = str(year)
    sm_dt = datetime(year, month, day)
    url = f"{const.SM_URL}/device/v1.0/historical?language=en"
    res = []
    for ii in range(0, days):
        data = []
        first = True
        for item in INVERTERS:
            if site not in item:
                continue
            sm_devices = item[site]
            x = -1
            divisor = len(sm_devices)
            for device in sm_devices:
                x += 1
                if x > 0:
                    epoch_values = [item[0] for item in data]
                    first = False

                last_epoch = None
                logging.info(f'Device Serial Number: {device["deviceSn"]} Device ID: {device["deviceId"]}')

                payload = json.dumps(
                    {
                        "deviceSn": device["deviceSn"],
                        "deviceId": device["deviceId"],
                        "startTime": yr + "-" + mm + "-" + dd,
                        "endTime": "2997-10-26",
                        "timeType": 1,
                    }
                )
                headers = {
                    "Content-Type": "application/json",
                    "User-Agent": "UNHCR_STEVE",
                    "Authorization": f"Bearer {const.SM_BIZ_ACCESS_TOKEN}",
                }

                response = requests.request("POST", url, headers=headers, data=payload)
                if response.status_code != 200:
                    logging.error(f"BAD API RESPONSE ERROR: {response.status_code}   {response.text}")
                    exit()

                j = json.loads(response.text)
                for item in j["paramDataList"]:
                    e = round(int(item["collectTime"]) / 300) * 300
                    e += 60 * 60  # add 1 hour for Africa
                    if e == last_epoch:
                        continue
                    last_epoch = e
                    info = {
                        "deviceId": str(j["deviceId"]),
                        "org_epoch": item["collectTime"],
                        "epoch": e,  # item["collectTime"]
                    }
                    for d in item["dataList"]:
                        if d["name"] == "System Time":
                            dt = datetime.fromtimestamp(
                                e, UTC
                            )  # depriciated datetime.utcfromtimestamp(e).strftime('%Y-%m-%d %H:%M')
                            info["ts"] = dt

                        if d["name"] == "Load  Power L1":
                            info["load_p1_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "Load  Power L2":
                            info["load_p2_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "Load  Power L3":
                            info["load_p3_w"] = utils.str_to_float_or_zero(d["value"])
                        # TODO: this disappeared from 1 of the inverters
                        # if d["name"] == "Battery Status":
                        #     info["batt_status"] = str(d["value"])
                        if d["name"] == "Battery Power":
                            info["batt_pwr_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "SoC":
                            info["batt_soc"] = str(d["value"])
                        if d["name"] == "Total Charging Energy":
                            info["batt_chg_ttl_kwh"] = utils.str_to_float_or_zero(
                                d["value"]
                            )

                        if d["name"] == "Gen Daily Run Time":
                            info["gen_run_hrs"] = str(d["value"])
                        if d["name"] == "Total Consumption Power":
                            info["load_pwr_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "Generator Active Power":
                            info["gen_pwr_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "Daily Production Generator":
                            info["gen_produce_kwh"] = utils.str_to_float_or_zero(
                                d["value"]
                            )

                        if d["name"] == "Total Solar Power":
                            info["solar_ttl_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "Cumulative Production (Active)":
                            info["prod_cumulative_kwh"] = utils.str_to_float_or_zero(
                                d["value"]
                            )
                        if d["name"] == "Daily Production (Active)":
                            info["prod_daily_kwh"] = utils.str_to_float_or_zero(
                                d["value"]
                            )
                        if d["name"] == "DC Voltage PV1":
                            info["pv1_v"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Voltage PV2":
                            info["pv2_v"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Voltage PV3":
                            info["pv3_v"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Voltage PV4":
                            info["pv4_v"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Voltage PV5":
                            info["pv5_v"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Voltage PV6":
                            info["pv6_v"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Voltage PV7":
                            info["pv7_v"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Voltage PV8":
                            info["pv8_v"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Power PV1":
                            info["pv1_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Power PV2":
                            info["pv2_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Power PV3":
                            info["pv3_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Power PV4":
                            info["pv4_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Power PV5":
                            info["pv5_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Power PV6":
                            info["pv6_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Power PV7":
                            info["pv7_w"] = utils.str_to_float_or_zero(d["value"])
                        if d["name"] == "DC Power PV8":
                            info["pv8_w"] = utils.str_to_float_or_zero(d["value"])
                    if first:
                        data.append(
                            [
                                int(info["epoch"]),
                                utils.str_to_float_or_zero(info["gen_pwr_w"]),
                                1,
                            ]
                        )
                    else:
                        match = next(
                            (
                                (index, it)
                                for index, it in enumerate(data)
                                if it[0] == e
                            ),
                            None,
                        )
                        if match:
                            index, matching_item = match
                            if utils.str_to_float_or_zero(info["gen_pwr_w"]) != 0:
                                pass
                            data[index][1] += utils.str_to_float_or_zero(
                                info["gen_pwr_w"]
                            )
                            data[index][2] += 1

                            # data[index]["gen_produce_kwh"] += utils.str_to_float_or_zero(info["gen_produce_kwh"])
                            # data[index]["gen_pwr_w"] += utils.str_to_float_or_zero(info["gen_pwr_w"])
                            # data[index]["load_pwr_w"] += utils.str_to_float_or_zero(info["load_pwr_w"])
                            # data[index]["batt_chg_ttl_kwh"] += utils.str_to_float_or_zero(info["batt_chg_ttl_kwh"])
                            # data[index]["batt_pwr_w"] += utils.str_to_float_or_zero(info["batt_pwr_w"])
                            # data[index]["load_p1_w"] += utils.str_to_float_or_zero(info["load_p1_w"])
                            # data[index]["load_p2_w"] += utils.str_to_float_or_zero(info["load_p2_w"])
                            # data[index]["load_p3_w"] += utils.str_to_float_or_zero(info["load_p3_w"])
                            # data[index]["gen_run_hrs"] += '_' + str(info["gen_run_hrs"])
                            # data[index]["batt_soc"] += '_' + str(info["batt_soc"])
                            # data[index]["batt_status"] += '_' + str(info["batt_status"])
                            # data[index]["deviceId"] += '_' + str(info["deviceId"])
                        else:
                            index = bisect.bisect_left(epoch_values, info["epoch"])
                            # Insert the new item at the correct index
                            data.insert(
                                index,
                                [
                                    int(info["epoch"]),
                                    utils.str_to_float_or_zero(info["gen_pwr_w"]),
                                    1,
                                ],
                            )
                            epoch_values = [item[0] for item in data]

        for d in data:
            d[1] /= 12 * 1000
            # correct for missing inverter data
            d[1] = d[1] * divisor / d[2]
            res.append(d)
        sm_dt = sm_dt + timedelta(days=1)
        logging.info(f"Date: {sm_dt}")
        mm = str(sm_dt.month).zfill(2)
        dd = str(sm_dt.day).zfill(2)
        yr = str(sm_dt.year)

    return res


def gen_file_from_csv(dtStart, data, append=None):
    """
    Processes CSV data to generate a file with processed fuel and timestamp data.

    Args:
        fn (str): The filename to write the processed data to.
        dtStart (datetime): The start datetime for the data processing.
        data (list): A list of strings representing CSV data lines.
        append (bool, optional): If True, appends to the existing file. Defaults to None.

    Returns:
        list: A list containing a status code, filename, start datetime, and lines read from the file.
              Returns [-1, fn, dtStart, lines] if the file already exists and is not appended.
              Returns [0, fn, dtStart, liters] if no liters data is generated.

    The function reads CSV data, processes each line to calculate fuel levels and timestamps,
    and writes the processed data to a file. It handles duplicate timestamps and missing data
    gaps by adjusting timestamps and filling in missing entries. The function supports both
    creating a new file or appending to an existing one.
    """

    start = True
    key = None
    liters = []
    t_delta_minute = timedelta(minutes=1)
    l1 = l2 = hr1 = hr2 = dl1 = dl2 = 0
    # ttl1=ttl2=0
    tzz = pytz.timezone(tz)
    xxx = -1

    for val in data:
        xxx += 1
        if val is None:
            continue
        if val.startswith("Unit"):
            continue

        val = val.replace('"', "")
        z = val.split(",")
        z = z[1:]
        if key is None:
            key = z[0]
        ll1 = decimal.Decimal(z[3])
        ll2 = decimal.Decimal(z[4])
        h1 = decimal.Decimal(z[5])
        h2 = decimal.Decimal(z[6])
        d = z[1]
        dt = d[0:4] + "-" + d[5:7] + "-" + d[8:10] + "T" + d[11:16] + ":00"
        # 15 minute change
        epoch = utils.ts2Epoch(dt)
        utc_datetime = datetime.utcfromtimestamp(epoch)
        # Set the timezone to UTC
        formatted_datetime = utc_datetime.strftime("%Y-%m-%dT%H:%M:%S") + ":00"
        formatted_datetime_end = (utc_datetime + timedelta(minutes=1)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        ) + ":00"

        if start:
            start = False
            l1 = ll1
            l2 = ll2
            dl1 = ll1
            dl2 = ll2
            hr1 = h1
            hr2 = h2
            lastTs = utc_datetime - t_delta_minute

        if len(liters) % 30 == 0:
            logging.debug("x", len(liters))
        tdelta = utc_datetime - lastTs
        if l1 - ll1 < 0:
            l1 = ll1
        if l2 - ll2 < 0:
            l2 = ll2
        epoch = str(epoch).split(".")[0]
        if tdelta == t_delta_minute:
            lastData = {
                "key": key + epoch,
                "start": formatted_datetime,
                "end": formatted_datetime_end,
                "epoch": epoch,
                "l1": ll1,
                "l2": ll2,
                "dl1": l1 - ll1,
                "dl2": l2 - ll2,
                "hr1": h1,
                "hr2": h2,
                "dhr1": hr1 - h1,
                "dhr2": hr2 - h2,
            }
            liters.append(lastData)
        elif tdelta == timedelta(minutes=0):  # duplicate timestamp
            continue
        else:  # more than 2 minute gap, use last data with adjusted ts
            z = 0
            while z < 60:
                z += 1
                missingTs = lastTs + t_delta_minute
                formatted_missingTs = missingTs.strftime("%Y-%m-%dT%H:%M:%S") + ":00"
                formatted_missingTs_end = (missingTs + timedelta(minutes=1)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ) + ":00"

                missingEpoch = str(int(liters[len(liters) - 1]["epoch"]) + 60).split(
                    "."
                )[0]
                if int(missingEpoch) <= int(epoch):
                    lastData = {
                        "key": key + missingEpoch,
                        "start": formatted_missingTs,
                        "end": formatted_missingTs_end,
                        "epoch": missingEpoch,
                        "l1": ll1,
                        "l2": ll2,
                        "dl1": l1 - ll1,
                        "dl2": l2 - ll2,
                        "hr1": h1,
                        "hr2": h2,
                        "dhr1": hr1 - h1,
                        "dhr2": hr2 - h2,
                    }
                    liters.append(lastData)
                    lastTs = missingTs
                    l1 = ll1
                    l2 = ll2
                    continue

                missingData = lastData = {
                    "key": key + missingEpoch,
                    "start": formatted_missingTs,
                    "end": formatted_missingTs_end,
                    "epoch": missingEpoch,
                    "l1": ll1,
                    "l2": ll2,
                    "dl1": l1 - ll1,
                    "dl2": l2 - ll2,
                    "hr1": h1,
                    "hr2": h2,
                    "dhr1": hr1 - h1,
                    "dhr2": hr2 - h2,
                }

                lastTs = missingTs
                lastData = missingData
                break
        lastTs = utc_datetime
        l1 = ll1
        l2 = ll2
    if len(liters) == 0:
        logging.debug("gen_file_from_csv No data found")

    return liters


def extract_csv_data(site, fn, fn1, from_dt=None):
    # default processing date
    dt = datetime.utcnow() - timedelta(days=1)
    yr = dt.year
    mnth = dt.month
    day = dt.day
    days = 1
    append = False

    if os.path.exists(fn) and not os.path.exists(fn1):
        logging.debug("File exists, delete it to get new data %s" % fn)
        with open(fn, "r") as file:
            # Read the lines from the file and remove newline characters
            liters = [line.strip() for line in file.readlines()]
            x, fnnn, dtStart, lines = gen_file_from_csv(fn1, "2024-10-01", liters)
    else:
        with open(fn1, "r", encoding="utf-8", errors="replace") as file:
            df1 = pd.read_csv(file)
        df = pd.read_csv(fn)
        start = df.iloc[-1].to_dict()["ts"]
        start1 = df1.iloc[-1].to_dict()["start"]
        st_ts = datetime.strptime(start, "%Y-%m-%d %H:%M")
        st_ts1 = datetime.strptime(start1, "%Y-%m-%dT%H:%M:%S:%f")
        if st_ts > st_ts1:
            append = True
            filtered_df = df[pd.to_datetime(df["ts"]) > st_ts1]
            df_data = (
                filtered_df.astype(str)
                .apply(lambda row: ",".join(row), axis=1)
                .tolist()
            )
            x, fnnn, dtStart, lines = gen_file_from_csv(
                fn1, "2024-10-01", df_data, append=append
            )
            yr = st_ts1.year
            mnth = st_ts1.month
            day = st_ts1.day
            days = math.ceil((st_ts - st_ts1).total_seconds() / (24 * 60 * 60))
        elif from_dt is not None:
            st_ts1 = datetime.strptime(from_dt, "%Y-%m-%d")
            yr = st_ts1.year
            mnth = st_ts1.month
            day = st_ts1.day
            days = math.ceil((st_ts - st_ts1).total_seconds() / (24 * 60 * 60))
        else:
            return None, None, None
    with open(fn1, "r", encoding="utf-8", errors="replace") as file:
        df1 = pd.read_csv(file, usecols=["epoch", "deltal1", "deltal2"])

    # Extract and filter epochs from data
    data = solarman_api_historical(site=site, year=yr, month=mnth, day=day, days=days)
    return df, df1, data


def extract_csv_data_new(site, df, from_dt=None):
    # default processing date
    dt = datetime.utcnow() - timedelta(days=1)
    yr = dt.year
    mnth = dt.month
    day = dt.day
    days = 1
    append = False

    st_ts = df["Time"].max()
    logging.debug(f"Type of st_ts: {type(st_ts)}")
    st_ts = datetime.strptime(st_ts, "%Y-%m-%dT%H:%M:%S")  # Convert to datetime
    st_ts = st_ts.replace(second=0, microsecond=0)
    st_ts1 = datetime.strptime(from_dt, "%Y-%m-%d %H:%M")
    if st_ts > st_ts1:
        append = True
        filtered_df = df[pd.to_datetime(df["Time"]) > st_ts1]
        df_data = (
            filtered_df.astype(str).apply(lambda row: ",".join(row), axis=1).tolist()
        )
        liters = gen_file_from_csv("2024-10-01", df_data, append=append)
        yr = st_ts1.year
        mnth = st_ts1.month
        day = st_ts1.day
        days = math.ceil((st_ts - st_ts1).total_seconds() / (24 * 60 * 60))
    elif from_dt is not None:
        st_ts1 = datetime.strptime(from_dt, "%Y-%m-%d %H:%M")
        yr = st_ts1.year
        mnth = st_ts1.month
        day = st_ts1.day
        days = math.ceil((st_ts - st_ts1).total_seconds() / (24 * 60 * 60))
    else:
        return None, None, None

    # Extract and filter epochs from data
    data = solarman_api_historical(site=site, year=yr, month=mnth, day=day, days=days)
    return liters, data


def concat_csv_files(dpath, fn, label):
    """
    Concatenate multiple CSV files downloaded from Galooli Pro View into a single file.

    Parameters
    ----------
    dpath : str
        The directory path where the downloaded CSV files are located
    fn : str
        The filename for the concatenated CSV file
    label : str
        The label that identifies the correct CSV files to concatenate
    """

    files = glob.glob(
        dpath + "Bulk Tank Events (downloaded from Galooli Pro View)*.csv"
    )
    dataframes = []
    for file in files:
        logging.debug(file)
        df = pd.read_csv(file)
        if not label in df.values[0][0]:
            logging.debug('Wrong label',df.values[0][0],label)
            continue
        if label not in df.iloc[-1, 0]:
            df = df.iloc[:-1]
        dataframes.append(df)
    # Concatenate all DataFrames into a single DataFrame
    if len(dataframes) == 0:
        logging.debug("No data files found")
        return False
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    concatenated_df["Time"] = pd.to_datetime(concatenated_df["Time"], dayfirst=True)
    concatenated_df = concatenated_df.sort_values(by="Time")
    concatenated_df.drop_duplicates(inplace=True)
    # Save the concatenated DataFrame to a new CSV file
    concatenated_df.to_csv(dpath + fn, index=False)
    return True
