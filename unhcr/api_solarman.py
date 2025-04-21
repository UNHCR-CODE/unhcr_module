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
from datetime import datetime, UTC, timedelta, timezone
import json
import logging
import time
import pandas as pd
import re
import requests
from sqlalchemy import inspect, text, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from unhcr import app_utils
from unhcr import constants as const
from unhcr import utils
from unhcr import db
from unhcr import err_handler
from unhcr import models

mods=[
    ["app_utils", "app_utils"],
    ["constants", "const"],
    ["utils", "utils"],
    ["db", "db"],
    ["err_handler", "err_handler"],
    ["models", "models"],
]

res = app_utils.app_init(mods=mods, log_file="unhcr.api_solarman.log", version="0.4.8", level="INFO", override=False)
logger = res[0]
if const.LOCAL:  # testing with local python files
    logger, app_utils, const, utils, db, err_handler, models = res

# Solarman API credentials (replace with your actual credentials)
APP_ID = const.SM_APP_ID
APP_SECRET = const.SM_APP_SECRET
BIZ_ACCESS_TOKEN = const.SM_BIZ_ACCESS_TOKEN
BASE_URL = const.SM_URL
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

def db_get_sm_weather_max_epoch(db_eng, device_sn):
    """
    Retrieves the latest timestamp from the database. If the database is empty or an error occurs,
    returns None and the error.
    Args:
        device_id (int): The device ID to query the database for.
        engine (sqlalchemy.engine.Engine): The connection engine to use.
    Returns:
        tuple: A tuple containing the latest timestamp as an integer, and None if the query was successful, or an error message if it was not.
    """
    sql = f"select max(org_epoch) FROM solarman.weather where device_sn = '{device_sn}'"
    val, err = db.sql_execute(sql, db_eng)
    if err is not None:
        return None, err
    epoch = val[0][0]
    return epoch, None



def db_get_devices_site_sn_id(db_eng, dev_type='%', site_key='%'):
    sql = """
        WITH site_devices AS (
            SELECT s."name", dsh.station_id, dsh.device_sn, dsh.device_id, d.device_type 
            FROM solarman.device_site_history dsh
            JOIN solarman.stations s ON s.id = dsh.station_id
            JOIN solarman.devices d ON dsh.device_sn = d.device_sn
            WHERE dsh.end_time IS NULL 
        )
        SELECT * FROM site_devices
        WHERE  device_type ILIKE :dev_type AND name ILIKE :site_key
    """

    params = {
        "dev_type": dev_type,
        "site_key": site_key
    }
    df, err = db.sql_execute(sql, db_eng, data=params)
    if err:
        logger.error(f"db_get_devices_site_sn_id ERROR: {err}")
        return None, err
    return pd.DataFrame(df), None

def camel_to_snake(name):
    """Converts a camelCase string to snake_case."""
    name = re.sub(r"(?<=[a-z])(?=[A-Z])", "_", name)
    return name.lower()


def convert_keys_to_snake_case(data):
    """Converts keys in a list of dictionaries from camelCase to snake_case."""
    snake_case_data = []
    for item in data:
        snake_case_item = {}
        for key, value in item.items():
            snake_case_key = camel_to_snake(key)
            snake_case_item[snake_case_key] = value
        snake_case_data.append(
            {**snake_case_item, "site_id": item.get("site_id")}
        )  # make sure to keep site_id
    return snake_case_data


def round_to_nearest_5_minutes(dt):
    # Round minutes to nearest 5-minute interval
    rounded_minutes = (dt.minute // 5) * 5
    if dt.minute % 5 >= 3:
        rounded_minutes += 5  # Round up if remainder is 3 or more

    # Create a new datetime object to handle overflow safely
    new_dt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=rounded_minutes)

    # If rounding pushed time to next hour or day, handle it with timedelta
    if new_dt.hour != dt.hour or new_dt.day != dt.day:
        return new_dt

    return dt.replace(minute=rounded_minutes, second=0, microsecond=0)


def db_all_site_ids(db_eng):
    with Session(db_eng) as session:
        # Construct the SELECT statement
        stmt = select(models.Station.id)

        # Execute the query and fetch the results
        results = session.execute(stmt).scalars().all()

        return results


def db_get_inverter_sns(db_eng):
    with Session(db_eng) as session:
        # Construct the SELECT statement
        stmt = select(models.Device.device_sn).where(
            models.Device.device_type == "INVERTER"
        )

        # Execute the query and fetch the results
        results = session.execute(stmt).scalars().all()

        return results


def db_insert_devices(db_eng, records=None):
    """
    Inserts records into the devices table in the database.

    Parameters
    ----------
    db_eng : Engine
        The SQLAlchemy engine to use for the database connection.
    records : list of dict
        The list of records to insert into the table.

    Returns
    -------
    None
    """

    with Session(db_eng) as session:
        # Process each item
        for item in records:
            record = models.Device(
                device_sn=item["device_sn"],
                device_id=item["device_id"],
                device_type=item["device_type"],
                connect_status=item["connect_status"],
                collection_time=int(item["collection_time"]),
            )

            # data = session.query(StationData).all()

            # Add and commit record
            session.merge(record)  # Uses upsert behavior
            # x = str(session.query(StationData).filter(StationData.station_id == record.station_id).statement)
            session.commit()

    logger.info("Data inserted successfully!")


def api_get_devices(site_id, db_eng=None):
    url = BASE_URL + "/station/v1.0/device?language=en"

    payload = json.dumps(
        {
            "stationId": site_id,
        }
    )
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "UNHCR_STEVE",
        "Authorization": f"Bearer {BIZ_ACCESS_TOKEN}",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    res = response.json()
    if "success" not in res or res["success"] != True:
        return None, "API call not successful"
    data = res["deviceListItems"]
    data = convert_keys_to_snake_case(data)
    data = [{**item, "site_id": site_id} for item in data]
    err = None
    if db_eng:
        ######res, err = err_handler.error_wrapper(lambda: insert_station_data_daily(db_eng, data))
        db_insert_devices(db_eng, data)
    pass
    if err:
        return None, err
    return data, None


def api_get_weather_data(date_str, devices):
    """
    Retrieves weather data for specified devices and date.

    This function sends a POST request to the historical weather data API for each
    device in the provided list. It processes the response to extract relevant weather
    parameters and compiles them into a DataFrame.

    Parameters:
    date_str (str): The start date for data retrieval in 'YYYY-MM-DD' format.
    devices (list): A list of dictionaries, each containing 'station_id', 'deviceSn' and 'deviceId' keys.

    Returns:
    pd.DataFrame: A DataFrame containing the weather data, or None if no data is retrieved.
    """

    url = HISTORICAL_URL
    data = []
    first = True
    x = -1
    for device in devices.itertuples(index=False):
        x += 1
        if x > 0:
            epoch_values = [item["epoch"] for item in data]
            first = False

        last_epoch = None
        logging.info(f"Device: {device}")

        payload = json.dumps(
            {
                "station_id": device.station_id,
                "deviceSn": device.device_sn,
                "deviceId": device.device_id,
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
                    "station_id": device.station_id,
                    "device_sn": str(j["deviceSn"]),
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
                        )
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


def db_update_weather(df, epoch, engine):
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
        df["station_id"] = df["station_id"].astype("int32")
        # df["device_sn"] = df["device_sn"]
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
                INSERT INTO solarman.weather (station_id, device_sn, device_id, org_epoch, epoch, ts, temp_c, panel_temp, humidity, rainfall, irr, daily_irr)
                SELECT station_id, device_sn, device_id, org_epoch, epoch, ts, temp_c, panel_temp, humidity, rainfall, irr, daily_irr FROM solarman.temp_weather
                ON CONFLICT (device_sn, ts) DO UPDATE 
                SET device_id = EXCLUDED.device_id,
                    org_epoch = EXCLUDED.org_epoch,
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
        return None, f"db_update_weather ERROR: {e}"


def get_station_daily_data(
    id, start_date="2025-03-01", end_date="2025-03-31", type=2, db_eng=None
):
    url = HISTORICAL_URL.replace("/device/", "/station/").replace(
        "/historical", "/history"
    )

    payload = json.dumps(
        {
            "stationId": id,
            "startTime": start_date,  # "2025-03-01",
            "endTime": end_date,  # "2025-03-26",
            "timeType": type,  # 2
        }
    )
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "UNHCR_STEVE",
        "Authorization": f"Bearer {BIZ_ACCESS_TOKEN}",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    res = response.json()
    if "success" not in res or res["success"] != True:
        return None, "API call not successful"
    data = res["stationDataItems"]
    data = [{**item, "site": id} for item in data]
    err = None
    if db_eng:
        ######res, err = err_handler.error_wrapper(lambda: insert_station_data_daily(db_eng, data))
        insert_station_data_daily(db_eng, data)
    pass
    if err:
        return None, err
    return data, None


def insert_station_data_daily(db_eng, records=None):
    """
    Inserts records into the station_data_daily table in the database.

    Parameters
    ----------
    db_eng : Engine
        The SQLAlchemy engine to use for the database connection.
    records : list of dict
        The list of records to insert into the table.

    Returns
    -------
    None
    """

    Session = sessionmaker(bind=db_eng)
    session = Session()

    # Process each item
    for item in records:
        date_time = datetime(item["year"], item["month"], item["day"])

        record = models.StationData(
            station_id=int(item["site"]),
            ts=date_time,
            year=item["year"],
            month=item["month"],
            day=item["day"],
            generation_power=item.get("generationPower"),
            use_power=item.get("usePower"),
            grid_power=item.get("gridPower"),
            purchase_power=item.get("purchasePower"),
            wire_power=item.get("wirePower"),
            charge_power=item.get("chargePower"),
            discharge_power=item.get("dischargePower"),
            battery_power=item.get("batteryPower"),
            battery_soc=item.get("batterySoc"),
            irradiate_intensity=item.get("irradiateIntensity"),
            generation_value=item.get("generationValue"),
            generation_ratio=item.get("generationRatio"),
            grid_ratio=item.get("gridRatio"),
            charge_ratio=item.get("chargeRatio"),
            use_value=item.get("useValue"),
            use_ratio=item.get("useRatio"),
            buy_ratio=item.get("buyRatio"),
            use_discharge_ratio=item.get("useDischargeRatio"),
            grid_value=item.get("gridValue"),
            buy_value=item.get("buyValue"),
            charge_value=item.get("chargeValue"),
            discharge_value=item.get("dischargeValue"),
            full_power_hours=item.get("fullPowerHours"),
            irradiate=item.get("irradiate"),
            theoretical_generation=item.get("theoreticalGeneration"),
            pr=item.get("pr"),
            cpr=item.get("cpr"),
        )

        # data = session.query(StationData).all()

        # Add and commit record
        session.merge(record)  # Uses upsert behavior
        # x = str(session.query(StationData).filter(StationData.station_id == record.station_id).statement)
        session.commit()

    logger.info("Data inserted successfully!")


def insert_inverter_data(db_eng=None, json_data={}):
    if db_eng is None:
        db_eng = db.set_local_defaultdb_engine()
    z = ''
    inverter_data_table = inspect(models.InverterData).local_table
    # Define the expected keys -- the ones coming from API so we can map them to DB cols
    DB_KEYS = [col for col in inverter_data_table.columns.keys() if col not in ['ts', 'created', 'updated']]
    EXPECTED_KEYS = {
        "SN1": "device_sn",
        "INV_MOD1": "inverter_type",
        "Pi_LV1": "output_power_level",
        "Pr1": "rated_power",
        "P_INF": "parallel_information",
        "Dev_Ty1": "device_type",
        "SYSTIM1": "system_time",
        "PTCv1": "protocol_version",
        "MAIN": "main_data",
        "HMI": "hmi",
        "LBVN": "lithium_battery_version_number",
        "CBAVM": "control_board_activator_version_number",
        "CBAMSV": "control_board_assisted_microcontroller_version_number",
        "A_B_F_V": "arc_board_firmware_version",
        "DV1": "dc_voltage_pv1",
        "DV2": "dc_voltage_pv2",
        "DV3": "dc_voltage_pv3",
        "DV4": "dc_voltage_pv4",
        "DC1": "dc_current_pv1",
        "DC2": "dc_current_pv2",
        "DC3": "dc_current_pv3",
        "DC4": "dc_current_pv4",
        "DP1": "dc_power_pv1",
        "DP2": "dc_power_pv2",
        "DP3": "dc_power_pv3",
        "DP4": "dc_power_pv4",
        "P_T_A": "total_production_active",
        "AV1": "ac_voltage_r_u_a",
        "AV2": "ac_voltage_s_v_b",
        "AV3": "ac_voltage_t_w_c",
        "AC1": "ac_current_r_u_a",
        "AC2": "ac_current_s_v_b",
        "AC3": "ac_current_t_w_c",
        "A_Fo1": "ac_output_frequency_r",
        "Et_ge0": "cumulative_production_active",
        "Etdy_ge1": "daily_production_active",
        "INV_O_P_L1": "inverter_output_power_l1",
        "INV_O_P_L2": "inverter_output_power_l2",
        "INV_O_P_L3": "inverter_output_power_l3",
        "INV_O_P_T": "total_inverter_output_power",
        "S_P_T": "total_solar_power",
        "G_V_L1": "grid_voltage_l1",
        "G_C_L1": "grid_current_l1",
        "G_P_L1": "grid_power_l1",
        "G_V_L2": "grid_voltage_l2",
        "G_C_L2": "grid_current_l2",
        "G_P_L2": "grid_power_l2",
        "G_V_L3": "grid_voltage_l3",
        "G_C_L3": "grid_current_l3",
        "G_P_L3": "grid_power_l3",
        "ST_PG1": "grid_status",
        "CT1_P_E": "external_ct1_power",
        "CT2_P_E": "external_ct2_power",
        "CT3_P_E": "external_ct3_power",
        "CT_T_E": "total_external_ct_power",
        "PG_F1": "grid_frequency",
        "PG_Pt1": "total_grid_power",
        "G16": "total_grid_reactive_power",
        "E_B_D": "daily_energy_buy",
        "E_S_D": "daily_energy_sell",
        "E_B_TO": "total_energy_buy",
        "E_S_TO": "total_energy_sell",
        "GS_A": "internal_l1_power",
        "GS_B": "internal_l2_power",
        "GS_C": "internal_l3_power",
        "GS_T": "internal_power",
        "A_RP_INV": "inverter_a_phase_reactive_power",
        "B_RP_INV": "inverter_b_phase_reactive_power",
        "C_RP_INV": "inverter_c_phase_reactive_power",
        "MPPT_N": "mppt_number_of_routes_and_phases",
        "C_V_L1": "load_voltage_l1",
        "C_V_L2": "load_voltage_l2",
        "C_V_L3": "load_voltage_l3",
        "C_P_L1": "load_power_l1",
        "C_P_L2": "load_power_l2",
        "C_P_L3": "load_power_l3",
        "E_Puse_t1": "total_consumption_power",
        "E_Suse_t1": "total_consumption_apparent_power",
        "Etdy_use1": "daily_consumption",
        "E_C_T": "total_consumption",
        "L_F": "load_frequency",
        "LPP_A": "load_phase_power_a",
        "LPP_B": "load_phase_power_b",
        "LPP_C": "load_phase_power_c",
        "B_ST1": "battery_status",
        "B_V1": "battery_voltage",
        "B_P_1": "battery_power1",
        "BATC1": "battery_current1",
        "B_C2": "battery_current2",
        "B_P1": "battery_power",
        "B_left_cap1": "soc",
        "t_cg_n1": "total_charging_energy",
        "t_dcg_n1": "total_discharging_energy",
        "Etdy_cg1": "daily_charging_energy",
        "Etdy_dcg1": "daily_discharging_energy",
        "BRC": "battery_rated_capacity",
        "B_TYP1": "battery_type",
        "Batt_ME1": "battery_mode",
        "BAT_FAC": "battery_factory",
        "B_1S": "battery_1_status",
        "B_CT": "battery_total_current",
        "B_2S": "battery_2_status",
        "BMS_B_V1": "bms_voltage",
        "BMS_B_C1": "bms_current",
        "BMST": "bms_temperature",
        "BMS_C_V": "bms_charge_voltage",
        "BMS_D_V": "bms_discharge_voltage",
        "BMS_C_C_L": "charge_current_limit",
        "BMS_D_C_L": "discharge_current_limit",
        "BMS_SOC": "bms_soc",
        "BMS_CC1": "bms_charging_max_current",
        "BMS_DC1": "bms_discharging_max_current",
        "Li_bf": "li_bat_flag",
        "B_T1": "temperature_battery",
        "AC_T": "ac_temperature",
        "yr1": "year",
        "mon1": "month",
        "tdy1": "day",
        "hou1": "hour",
        "min1": "minute",
        "sec1": "second",
        "Inver_Ara": "inverter_algebra",
        "Inver_Sd": "inverter_series_distinction",
        "GS_A1": "gs_a1",
        "GS_B1": "gs_b1",
        "GS_C1": "gs_c1",
        "GS_T1": "gs_t1",
        "GRID_RELAY_ST1": "grid_relay_status",
        "I_P_G_S": "inverter_power_generation_status",
        "GEN_P_L1": "gen_power_l1",
        "GEN_P_L2": "gen_power_l2",
        "GEN_P_L3": "gen_power_l3",
        "GEN_V_L1": "gen_voltage_l1",
        "GEN_V_L2": "gen_voltage_l2",
        "GEN_V_L3": "gen_voltage_l3",
        "R_T_D": "gen_daily_run_time",
        "EG_P_CT1": "generator_active_power",
        "GEN_P_T": "total_gen_power",
        "GEN_P_D": "daily_production_generator",
        "GEN_P_TO": "total_production_generator",
    }

    # Initialize a dictionary to hold the mapped data
    inverter_instances = []
    # Iterate over each dictionary in 'dataList'
    for item in json_data:
        mapped_data = {}
        for items in item["dataList"]:
            if items["key"] in EXPECTED_KEYS:
                # Map the key to the corresponding column name
                if "value" in items:
                    mapped_data[EXPECTED_KEYS[items["key"]]] = items["value"]

        # Check for missing keys
        missing_keys = [key for key in DB_KEYS if key not in mapped_data]
        # there are typically 31 keys missng, because we do not store those in the DB
        # if missing_keys:
        #     raise ValueError(f"Missing keys in JSON data: {', '.join(missing_keys)}")

        # Prepare the model with the expected values using the key mapping
        dt = datetime.fromtimestamp(int(item["collectTime"]), tz=timezone.utc)
        data = models.InverterData(ts=round_to_nearest_5_minutes(dt), **mapped_data)
        z = mapped_data["device_sn"]
        if data.system_time:
            if data.system_time.startswith("00-00-00"):
                data.system_time = None
            else:
                data.system_time = ("20" + data.system_time)[:16]
        inverter_instances.append(data)

    def to_plain_dict(obj, exclude_fields=None):
        """Convert SQLAlchemy ORM object to plain dict, excluding listed fields."""
        exclude_fields = set(exclude_fields or [])
        return {
            c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs
            if c.key not in exclude_fields
        }

    valid_inverters = [
        obj for obj in inverter_instances
        if obj.ts is not None #and obj.id is not None  # etc.
    ]

    rows = [to_plain_dict(obj, exclude_fields=['created', 'updated']) for obj in valid_inverters]
    
    def dedupe_by_keys(rows, keys):
        seen = set()
        dups = 0
        deduped = []
        for row in rows:
            identifier = tuple(row[k] for k in keys)
            if identifier not in seen:
                seen.add(identifier)
                deduped.append(row)
            else:
                dups += 1
            last = row
        return deduped

    rows = dedupe_by_keys(rows, keys=['ts', 'device_sn'])

    # Clean up internal SQLAlchemy attributes (like _sa_instance_state)
    for row in rows:
        row.pop('_sa_instance_state', None)

    # Build the INSERT statement
    update_columns = [
        c.name for c in inverter_data_table.columns
        if c.name not in ('ts', 'device_sn', 'created', 'updated')  # Do not try to update PKs
    ]

    stmt = insert(inverter_data_table).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=['ts', 'device_sn'],
        set_={col: getattr(stmt.excluded, col) for col in update_columns}
    )
    # No update
    # stmt = stmt.on_conflict_do_nothing(index_elements=['ts', 'device_sn'])

    if rows:
        try:
            with db_eng.begin() as conn:
                res = conn.execute(stmt)
                return res.rowcount, None
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            return None, f"Error inserting data: {e}"


def get_inverter_data(
    sn=2309200154, 
    start_date=datetime.today().date(), 
    type=1,
    days=3, 
    db_eng=None
):
    """
    Fetches inverter data for a specified device serial number over a range of days.

    This function retrieves historical data from the Solarman API for a given inverter 
    device, starting from a specified date. The data is retrieved for a specified number 
    of days, or until the most recent data in the database if `days` is None. The function 
    also attempts to insert the retrieved data into a database if a database engine is provided.

    Args:
        sn (int): The serial number of the inverter device.
        start_date (date): The starting date from which to fetch data.
        type (int): The type of data to fetch, determining the granularity of time data.
        days (int, optional): The number of days worth of data to fetch. Defaults to 3. 
                              If None, the function calculates the number of days from 
                              the last recorded date in the database.
        db_eng: A SQLAlchemy database engine instance for database operations.

    Returns:
        tuple: A tuple containing the data fetched (or None if unsuccessful), and an error message 
               if applicable. If successful, the error message is None.
    """

    url = HISTORICAL_URL

    if days is None:
        res, err = db.sql_execute(
            f" SELECT max(ts) FROM solarman.inverter_data WHERE device_sn = '{sn}';",
            db_eng
        )
        if err is not None:
            logger.warning(f"get_inverter_data max data ERROR: {err}")
            days = 4
        else:
            end_date = (res[0][0]).date()
            days = (start_date - end_date).days + 1
    data = []
    for i in range(days):
        payload = json.dumps(
            {
                "deviceSn": sn,  # 2309200154,
                "startTime": start_date.isoformat(),  # "2024-12-30",
                "endTime": (start_date + timedelta(days=1)).isoformat(),  # "2024-12-31",
                "timeType": type,  # 1
            }
        )
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "UNHCR_STEVE",
            "Authorization": f"Bearer {BIZ_ACCESS_TOKEN}",
        }

        z = time.time()
        response = requests.request("POST", url, headers=headers, data=payload)

        res = response.json()
        if "success" not in res or res["success"] != True:
            return None, f'API call not successful {response.text}, date: {start_date}, # days: {i+1}'
        data.append(res["paramDataList"])
        err = None
        if db_eng:
            res, err = err_handler.error_wrapper(lambda: insert_inverter_data(db_eng, res["paramDataList"]))
        if err:
            return None, f'Insert inverter data ERROR: {err} , date: {start_date}, # days: {i+1}'
        logger.info(f"SN: {sn} |  date: {start_date} | # rows: {res[0] if res else 0}")
        start_date -= timedelta(days=1)


    return data, None


def transform_station_data(station):
    """Convert JSON keys to match DB column names and handle timestamps."""
    return {
        "id": station["id"],
        "name": station["name"],
        "location_lat": station.get("locationLat"),
        "location_lng": station.get("locationLng"),
        "location_address": station.get("locationAddress"),
        "region_nation_id": station.get("regionNationId"),
        "region_level1": station.get("regionLevel1"),
        "region_level2": station.get("regionLevel2"),
        "region_level3": station.get("regionLevel3"),
        "region_level4": station.get("regionLevel4"),
        "region_level5": station.get("regionLevel5"),
        "region_timezone": station.get("regionTimezone"),
        "type": station.get("type"),
        "grid_interconnection_type": station.get("gridInterconnectionType"),
        "installed_capacity": station.get("installedCapacity"),
        "start_operating_time": (
            datetime.utcfromtimestamp(station["startOperatingTime"])
            if station.get("startOperatingTime")
            else None
        ),
        "station_image": station.get("stationImage"),
        "created_date": (
            datetime.utcfromtimestamp(station["createdDate"])
            if station.get("createdDate")
            else None
        ),
        "battery_soc": station.get("batterySoc"),
        "network_status": station.get("networkStatus"),
        "generation_power": station.get("generationPower"),
        "last_update_time": (
            datetime.utcfromtimestamp(station["lastUpdateTime"])
            if station.get("lastUpdateTime")
            else None
        ),
        "contact_phone": station.get("contactPhone"),
        "owner_name": station.get("ownerName"),
    }


def upsert_stations(stations_data, db_eng=None):
    if db_eng is None:
        db_eng = db.set_local_defaultdb_engine()
    transformed_data = [transform_station_data(st) for st in stations_data]

    stmt = insert(models.Station).values(transformed_data)

    update_columns = {
        col.name: getattr(stmt.excluded, col.name)
        for col in models.Station.__table__.columns
        # if col.name not in ("id",)  # Exclude primary key
    }

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=["name"], set_=update_columns  # Unique constraint field
    )
    # Session = sessionmaker(bind=db_eng)
    session = Session(db_eng)
    session.execute(upsert_stmt)
    session.commit()
