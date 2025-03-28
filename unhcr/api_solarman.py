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
import pandas as pd
import requests
from sqlalchemy import TIMESTAMP, BigInteger, Column, ForeignKey, Index, Integer, Float, String, DateTime, JSON, Numeric, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker, declarative_base

from unhcr import constants as const
from unhcr import utils
from unhcr import db
from unhcr import err_handler

# local testing ===================================
if const.LOCAL:  # testing with local python files
    const, utils, db, err_handler = const.import_local_libs(
        mods=[
            ["constants", "const"],
            ["utils", "utils"],
            ["db", "db"],
            ["err_handler", "err_handler"],
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


def round_to_nearest_5_minutes(dt):
    # Calculate the number of minutes to round to
    rounded_minutes = (dt.minute // 5) * 5
    if dt.minute % 5 >= 3:
        rounded_minutes += 5  # Round up if the remainder is 3 or more

    # If rounding minutes causes it to move to the next hour, handle the overflow
    new_hour = dt.hour
    new_day = dt.day
    if rounded_minutes >= 60:
        rounded_minutes = 0
        new_hour += 1

        # Handle hour overflow to the next day
        if new_hour >= 24:
            new_hour = 0
            new_day += 1
            # You can handle month and year overflow if needed

    # Return a new datetime object with the rounded time, keeping the date in mind
    return dt.replace(minute=rounded_minutes, second=0, microsecond=0, hour=new_hour, day=new_day)


Base = declarative_base()

class Station(Base):
    __tablename__ = 'stations'
    __table_args__ = (
        Index('idx_stations_name', 'name'),
        UniqueConstraint('name', name='uq_station_name'),
        {'schema': 'solarman'}
    )
    id = Column(BigInteger, primary_key=True, autoincrement=False)
    name = Column(String(255), nullable=False)
    location_lat = Column(Float, nullable=True)
    location_lng = Column(Float, nullable=True)
    location_address = Column(String(255), nullable=True)
    region_nation_id = Column(Integer, nullable=True)
    region_level1 = Column(Integer, nullable=True)
    region_level2 = Column(Integer, nullable=True)
    region_level3 = Column(Integer, nullable=True)
    region_level4 = Column(Integer, nullable=True)
    region_level5 = Column(Integer, nullable=True)
    region_timezone = Column(String(50), nullable=True)
    type = Column(String(50), nullable=True)
    grid_interconnection_type = Column(String(50), nullable=True)
    installed_capacity = Column(Float, nullable=True)
    start_operating_time = Column(TIMESTAMP, nullable=True)
    station_image = Column(String(255), nullable=True)
    created_date = Column(TIMESTAMP, nullable=True)
    battery_soc = Column(Float, nullable=True)
    network_status = Column(String(50), nullable=True)
    generation_power = Column(Float, nullable=True)
    last_update_time = Column(TIMESTAMP, nullable=True)
    contact_phone = Column(String(50), nullable=True)
    owner_name = Column(String(255), nullable=True)

class StationData(Base):
    __tablename__ = "station_data_daily"
    __table_args__ = (
        Index('idx_station_data_id', 'station_id'),  # Index on 'sn' column
        Index('idx_station_data_ts', 'ts'),  # Index on 'collect_time' column
        {"schema": "solarman"}  # Schema argument should be a dictionary, placed last
    )
    station_id = Column(BigInteger,  primary_key=True, autoincrement=False, nullable=False)
    ts = Column(DateTime, primary_key=True)

    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)

    generation_power = Column(Float)
    use_power = Column(Float)
    grid_power = Column(Float)
    purchase_power = Column(Float)
    wire_power = Column(Float)
    charge_power = Column(Float)
    discharge_power = Column(Float)
    battery_power = Column(Float)
    battery_soc = Column(Float)
    irradiate_intensity = Column(Float)
    generation_value = Column(Float)
    generation_ratio = Column(Float)
    grid_ratio = Column(Float)
    charge_ratio = Column(Float)
    use_value = Column(Float)
    use_ratio = Column(Float)
    buy_ratio = Column(Float)
    use_discharge_ratio = Column(Float)
    grid_value = Column(Float)
    buy_value = Column(Float)
    charge_value = Column(Float)
    discharge_value = Column(Float)
    full_power_hours = Column(Float)
    irradiate = Column(Float)
    theoretical_generation = Column(Float)
    pr = Column(Float)
    cpr = Column(Float)

class Device(Base):
    __tablename__ = "devices"
    __table_args__ = (
        Index('idx_devices_device_sn', 'device_sn'),
        Index('idx_devices_device_id', 'device_id'),
        {"schema": "solarman"}
    )

    station_id = Column(BigInteger, ForeignKey("solarman.stations.id"), primary_key=True, nullable=False)
    device_sn = Column(String(25), nullable=False, primary_key=True)  # Composite primary key with station_id
    device_id = Column(BigInteger, nullable=False, primary_key=True)
    device_type = Column(String(50), nullable=False)
    connect_status = Column(Integer, nullable=False)
    collection_time = Column(BigInteger, nullable=False)  # Epoch time stored as BigInteger

class InverterData(Base):
    __tablename__ = 'inverter_data'
    __table_args__ = (
        Index('idx_inverter_data_device_sn', 'device_sn'),
        Index('idx_inverter_data_device_id', 'device_id'),
        Index('idx_inverter_data_ts', 'ts'),
        {"schema": "solarman"}  # Schema argument should be a dictionary, placed last
    )

    station_id = Column(BigInteger, ForeignKey("solarman.stations.id"), primary_key=True, nullable=False)
    device_sn = Column(String(25), primary_key=True)
    device_id = Column(BigInteger, primary_key=True)
    ts = Column(DateTime, primary_key=True)
    inverter_type = Column(String(255))
    output_power_level = Column(String(255))
    rated_power = Column(Numeric)
    parallel_information = Column(String(255))
    device_type = Column(JSON)
    system_time = Column(DateTime)
    protocol_version = Column(String(255))
    main_data = Column(String(255))
    hmi = Column(String(255))
    lithium_battery_version_number = Column(String(255))
    control_board_activator_version_number = Column(String(255))
    control_board_assisted_microcontroller_version_number = Column(String(255))
    arc_board_firmware_version = Column(String(255))
    dc_voltage_pv1 = Column(Float)
    dc_voltage_pv2 = Column(Float)
    dc_voltage_pv3 = Column(Float)
    dc_voltage_pv4 = Column(Float)
    dc_current_pv1 = Column(Float)
    dc_current_pv2 = Column(Float)
    dc_current_pv3 = Column(Float)
    dc_current_pv4 = Column(Float)
    dc_power_pv1 = Column(Float)
    dc_power_pv2 = Column(Float)
    dc_power_pv3 = Column(Float)
    dc_power_pv4 = Column(Float)
    total_production_active = Column(Float)
    ac_voltage_r_u_a = Column(Float)
    ac_voltage_s_v_b = Column(Float)
    ac_voltage_t_w_c = Column(Float)
    ac_current_r_u_a = Column(Float)
    ac_current_s_v_b = Column(Float)
    ac_current_t_w_c = Column(Float)
    ac_output_frequency_r = Column(Float)
    cumulative_production_active = Column(Float)
    daily_production_active = Column(Float)
    inverter_output_power_l1 = Column(Float)
    inverter_output_power_l2 = Column(Float)
    inverter_output_power_l3 = Column(Float)
    total_inverter_output_power = Column(Float)
    total_solar_power = Column(Float)
    grid_voltage_l1 = Column(Float)
    grid_current_l1 = Column(Float)
    grid_power_l1 = Column(Float)
    grid_voltage_l2 = Column(Float)
    grid_current_l2 = Column(Float)
    grid_power_l2 = Column(Float)
    grid_voltage_l3 = Column(Float)
    grid_current_l3 = Column(Float)
    grid_power_l3 = Column(Float)
    grid_status = Column(String(255))
    external_ct1_power = Column(Float)
    external_ct2_power = Column(Float)
    external_ct3_power = Column(Float)
    total_external_ct_power = Column(Float)
    grid_frequency = Column(Float)
    total_grid_power = Column(Float)
    total_grid_reactive_power = Column(Float)
    a_phase_reactive_power_of_power_grid = Column(Float)
    b_phase_reactive_power_of_power_grid = Column(Float)
    c_phase_reactive_power_of_power_grid = Column(Float)
    daily_energy_buy = Column(Float)
    daily_energy_sell = Column(Float)
    total_energy_buy = Column(Float)
    total_energy_sell = Column(Float)
    internal_l1_power = Column(Float)
    internal_l2_power = Column(Float)
    internal_l3_power = Column(Float)
    internal_power = Column(Float)
    inverter_a_phase_reactive_power = Column(Float)
    inverted_b_phase_reactive_power = Column(Float)
    inverted_c_phase_reactive_power = Column(Float)
    mppt_number_of_routes_and_phases = Column(String(255))
    load_voltage_l1 = Column(Float)
    load_voltage_l2 = Column(Float)
    load_voltage_l3 = Column(Float)
    load_power_l1 = Column(Float)
    load_power_l2 = Column(Float)
    load_power_l3 = Column(Float)
    total_consumption_power = Column(Float)
    total_consumption_apparent_power = Column(Float)
    daily_consumption = Column(Float)
    total_consumption = Column(Float)
    load_frequency = Column(Float)
    load_phase_power_a = Column(Float)
    load_phase_power_b = Column(Float)
    load_phase_power_c = Column(Float)
    battery_status = Column(String(255))
    battery_voltage = Column(Float)
    battery_power1 = Column(Float)
    battery_current1 = Column(Float)
    battery_current2 = Column(Float)
    battery_power = Column(Float)
    soc = Column(Float)
    total_charging_energy = Column(Float)
    total_discharging_energy = Column(Float)
    daily_charging_energy = Column(Float)
    daily_discharging_energy = Column(Float)
    battery_rated_capacity = Column(Float)
    battery_type = Column(String(255))
    battery_mode = Column(JSON)
    battery_factory = Column(String(255))
    battery_1_status = Column(String(255))
    battery_total_current = Column(Float)
    battery_2_status = Column(String(255))
    bms_voltage = Column(Float)
    bms_current = Column(Float)
    bms_temperature = Column(Float)
    bms_charge_voltage = Column(Float)
    bms_discharge_voltage = Column(Float)
    charge_current_limit = Column(Float)
    discharge_current_limit = Column(Float)
    bms_soc = Column(Float)
    bms_charging_max_current = Column(Float)
    bms_discharge_max_current = Column(Float)
    li_bat_flag = Column(String(255))
    temperature_battery = Column(Float)
    ac_temperature = Column(Float)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    hour = Column(Integer)
    minute = Column(Integer)
    second = Column(Integer)
    inverter_algebra = Column(String(255))
    inverter_series_distinction = Column(String(255))
    gs_a1 = Column(Float)
    gs_b1 = Column(Float)
    gs_c1 = Column(Float)
    gs_t1 = Column(Float)
    grid_relay_status = Column(String(255))
    inverter_power_generation_status = Column(String(255))
    gen_power_l1 = Column(Float)
    gen_power_l2 = Column(Float)
    gen_power_l3 = Column(Float)
    gen_voltage_l1 = Column(Float)
    gen_voltage_l2 = Column(Float)
    gen_voltage_l3 = Column(Float)
    gen_daily_run_time = Column(Float)
    generator_active_power = Column(Float)
    total_gen_power = Column(Float)
    daily_production_generator = Column(Float)
    total_production_generator = Column(Float)

class TempWeather(Base):
    __tablename__ = 'temp_weather'
    __table_args__ = {'schema': 'solarman'}  # Specify the schema
    device_id = Column(Integer, nullable=False, primary_key=True)
    device_sn = Column(String(25), nullable=False, primary_key=True)
    org_epoch = Column(Integer, nullable=True)
    epoch = Column(Integer, nullable=True)
    ts = Column(DateTime(timezone=True), nullable=False, primary_key=True)
    temp_c = Column(Float, nullable=True)
    panel_temp = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    rainfall = Column(Float, nullable=True)
    irr = Column(Float, nullable=True)
    daily_irr = Column(Float, nullable=True)

class Weather(Base):
    __tablename__ = 'weather'
    __table_args__ = (
        Index('idx_weather_device_id', 'device_id'),
        Index('idx_weather_device_sn', 'device_sn'),
        Index('idx_weather_ts', 'ts', postgresql_using='btree', postgresql_ops={'ts': 'desc'}),
        {'schema': 'solarman'}  # Schema should come last
    )
    station_id = Column(BigInteger, ForeignKey("solarman.stations.id"), primary_key=True, nullable=False)
    device_sn = Column(String(25), nullable=False, primary_key=True)
    device_id = Column(Integer, nullable=False, primary_key=True)
    org_epoch = Column(Integer, nullable=False)
    epoch = Column(Integer, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, primary_key=True)
    temp_c = Column(Float, nullable=True)
    panel_temp = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    rainfall = Column(Float, nullable=True)
    irr = Column(Float, nullable=True)
    daily_irr = Column(Float, nullable=True)



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



def get_station_daily_data(id, start_date='2025-03-01', end_date='2025-03-31', type=2, eng=None):
    url = HISTORICAL_URL.replace("/device/", '/station/').replace("/historical", "/history")

    payload = json.dumps({
    "stationId": id,
    "startTime": start_date, #"2025-03-01",
    "endTime": end_date, #"2025-03-26",
    "timeType": type, #2
    })
    headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'UNHCR_STEVE',
    'Authorization': f'Bearer {BIZ_ACCESS_TOKEN}',
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    res = response.json()
    if 'success' not in res or res['success'] != True:
        return None, 'API call not successful'
    data = res['stationDataItems']
    data = [{**item, "site": id} for item in data]
    err = None
    if eng:
        ######res, err = err_handler.error_wrapper(lambda: insert_station_data_daily(eng, data))
        insert_station_data_daily(eng, data)
    pass
    if err:
        return None, err
    return data, None


def insert_station_data_daily(eng, records=None):
    """
    Inserts records into the station_data_daily table in the database.

    Parameters
    ----------
    eng : Engine
        The SQLAlchemy engine to use for the database connection.
    records : list of dict
        The list of records to insert into the table.

    Returns
    -------
    None
    """
    
    Session = sessionmaker(bind=eng)
    session = Session()

    # Process each item
    for item in records:
        date_time = datetime(item["year"], item["month"], item["day"])

        record = StationData(
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
        
        #data = session.query(StationData).all()

        # Add and commit record
        session.merge(record)  # Uses upsert behavior
        #x = str(session.query(StationData).filter(StationData.station_id == record.station_id).statement)
        session.commit()

    print("Data inserted successfully!")


def insert_inverter_data(eng=None, json_data={}):
    if eng is None:
        eng = db.set_azure_defaultdb_engine()

    # Define the expected keys
    EXPECTED_KEYS = {
        'SN1': 'sn',
        'INV_MOD1': 'inverter_type',
        'Pi_LV1': 'output_power_level',
        'Pr1': 'rated_power',
        'P_INF': 'parallel_information',
        'Dev_Ty1': 'device_type',
        'SYSTIM1': 'system_time',
        'PTCv1': 'protocol_version',
        'MAIN': 'main_data',
        'HMI': 'hmi',
        'LBVN': 'lithium_battery_version_number',
        'CBAVM': 'control_board_activator_version_number',
        'CBAMSV': 'control_board_assisted_microcontroller_version_number',
        'A_B_F_V': 'arc_board_firmware_version',
        'DV1': 'dc_voltage_pv1',
        'DV2': 'dc_voltage_pv2',
        'DV3': 'dc_voltage_pv3',
        'DV4': 'dc_voltage_pv4',
        'DC1': 'dc_current_pv1',
        'DC2': 'dc_current_pv2',
        'DC3': 'dc_current_pv3',
        'DC4': 'dc_current_pv4',
        'DP1': 'dc_power_pv1',
        'DP2': 'dc_power_pv2',
        'DP3': 'dc_power_pv3',
        'DP4': 'dc_power_pv4',
        'P_T_A': 'total_production_active',
        'AV1': 'ac_voltage_r_u_a',
        'AV2': 'ac_voltage_s_v_b',
        'AV3': 'ac_voltage_t_w_c',
        'AC1': 'ac_current_r_u_a',
        'AC2': 'ac_current_s_v_b',
        'AC3': 'ac_current_t_w_c',
        'A_Fo1': 'ac_output_frequency_r',
        'Et_ge0': 'cumulative_production_active',
        'Etdy_ge1': 'daily_production_active',
        'INV_O_P_L1': 'inverter_output_power_l1',
        'INV_O_P_L2': 'inverter_output_power_l2',
        'INV_O_P_L3': 'inverter_output_power_l3',
        'INV_O_P_T': 'total_inverter_output_power',
        'S_P_T': 'total_solar_power',
        'G_V_L1': 'grid_voltage_l1',
        'G_C_L1': 'grid_current_l1',
        'G_P_L1': 'grid_power_l1',
        'G_V_L2': 'grid_voltage_l2',
        'G_C_L2': 'grid_current_l2',
        'G_P_L2': 'grid_power_l2',
        'G_V_L3': 'grid_voltage_l3',
        'G_C_L3': 'grid_current_l3',
        'G_P_L3': 'grid_power_l3',
        'ST_PG1': 'grid_status',
        'CT1_P_E': 'external_ct1_power',
        'CT2_P_E': 'external_ct2_power',
        'CT3_P_E': 'external_ct3_power',
        'CT_T_E': 'total_external_ct_power',
        'PG_F1': 'grid_frequency',
        'PG_Pt1': 'total_grid_power',
        'G16': 'total_grid_reactive_power',
        'E_B_D': 'daily_energy_buy',
        'E_S_D': 'daily_energy_sell',
        'E_B_TO': 'total_energy_buy',
        'E_S_TO': 'total_energy_sell',
        'GS_A': 'internal_l1_power',
        'GS_B': 'internal_l2_power',
        'GS_C': 'internal_l3_power',
        'GS_T': 'internal_power',
        'A_RP_INV': 'inverter_a_phase_reactive_power',
        'B_RP_INV': 'inverter_b_phase_reactive_power',
        'C_RP_INV': 'inverter_c_phase_reactive_power',
        'MPPT_N': 'mppt_number_of_routes_and_phases',
        'C_V_L1': 'load_voltage_l1',
        'C_V_L2': 'load_voltage_l2',
        'C_V_L3': 'load_voltage_l3',
        'C_P_L1': 'load_power_l1',
        'C_P_L2': 'load_power_l2',
        'C_P_L3': 'load_power_l3',
        'E_Puse_t1': 'total_consumption_power',
        'E_Suse_t1': 'total_consumption_apparent_power',
        'Etdy_use1': 'daily_consumption',
        'E_C_T': 'total_consumption',
        'L_F': 'load_frequency',
        'LPP_A': 'load_phase_power_a',
        'LPP_B': 'load_phase_power_b',
        'LPP_C': 'load_phase_power_c',
        'B_ST1': 'battery_status',
        'B_V1': 'battery_voltage',
        'B_P_1': 'battery_power1',
        'BATC1': 'battery_current1',
        'B_C2': 'battery_current2',
        'B_P1': 'battery_power',
        'B_left_cap1': 'soc',
        't_cg_n1': 'total_charging_energy',
        't_dcg_n1': 'total_discharging_energy',
        'Etdy_cg1': 'daily_charging_energy',
        'Etdy_dcg1': 'daily_discharging_energy',
        'BRC': 'battery_rated_capacity',
        'B_TYP1': 'battery_type',
        'Batt_ME1': 'battery_mode',
        'BAT_FAC': 'battery_factory',
        'B_1S': 'battery_1_status',
        'B_CT': 'battery_total_current',
        'B_2S': 'battery_2_status',
        'BMS_B_V1': 'bms_voltage',
        'BMS_B_C1': 'bms_current',
        'BMST': 'bms_temperature',
        'BMS_C_V': 'bms_charge_voltage',
        'BMS_D_V': 'bms_discharge_voltage',
        'BMS_C_C_L': 'charge_current_limit',
        'BMS_D_C_L': 'discharge_current_limit',
        'BMS_SOC': 'bms_soc',
        'BMS_CC1': 'bms_charging_max_current',
        'BMS_DC1': 'bms_discharging_max_current',
        'Li_bf': 'li_bat_flag',
        'B_T1': 'temperature_battery',
        'AC_T': 'ac_temperature',
        'yr1': 'year',
        'mon1': 'month',
        'tdy1': 'day',
        'hou1': 'hour',
        'min1': 'minute',
        'sec1': 'second',
        'Inver_Ara': 'inverter_algebra',
        'Inver_Sd': 'inverter_series_distinction',
        'GS_A1': 'gs_a1',
        'GS_B1': 'gs_b1',
        'GS_C1': 'gs_c1',
        'GS_T1': 'gs_t1',
        'GRID_RELAY_ST1': 'grid_relay_status',
        'I_P_G_S': 'inverter_power_generation_status',
        'GEN_P_L1': 'gen_power_l1',
        'GEN_P_L2': 'gen_power_l2',
        'GEN_P_L3': 'gen_power_l3',
        'GEN_V_L1': 'gen_voltage_l1',
        'GEN_V_L2': 'gen_voltage_l2',
        'GEN_V_L3': 'gen_voltage_l3',
        'R_T_D': 'gen_daily_run_time',
        'EG_P_CT1': 'generator_active_power',
        'GEN_P_T': 'total_gen_power',
        'GEN_P_D': 'daily_production_generator',
        'GEN_P_TO': 'total_production_generator'
    }

    # Initialize a dictionary to hold the mapped data
    inverter_instances = []
    # Iterate over each dictionary in 'dataList'
    for item in json_data:
        mapped_data = {}
        for items in item['dataList']:
            if items['key'] in EXPECTED_KEYS:
                # Map the key to the corresponding column name
                if 'value' in items:
                    mapped_data[EXPECTED_KEYS[items['key']]] = items['value']

        # Check for missing keys
        missing_keys = [key for key in EXPECTED_KEYS if key not in mapped_data]

        # if missing_keys:
        #     raise ValueError(f"Missing keys in JSON data: {', '.join(missing_keys)}")

        # Prepare the model with the expected values using the key mapping
        dt = datetime.fromtimestamp(int(item['collectTime']), tz=timezone.utc)
        data = InverterData(
            collect_time=round_to_nearest_5_minutes(dt),
            ** mapped_data
        )
        data.system_time = '20' + data.system_time
        inverter_instances.append(data)

    Session = sessionmaker(bind=eng)
    session = Session()
    # Merge all instances in the batch at once
    for inverter_instance in inverter_instances:
        session.merge(inverter_instance)
    # Commit the new record to the database
    session.commit()
    pass


def get_inverter_data(sn=2309200154, start_date=datetime.today().date(), type=1, eng=None):
    """
    Retrieves inverter data for a given device serial number and date range.

    Args:
        sn (int, optional): Device serial number. Defaults to 2309200154.
        start_date (datetime.date, optional): Start date of the date range. Defaults to today.
        type (int, optional): Time type. Defaults to 1.
        eng (sqlalchemy.engine.Engine, optional): Engine to use for inserting data into database.

    Returns:
        tuple: Data and error message. If error, data is None and error message is not None.
    """
    url = HISTORICAL_URL
    ###print(url)

    payload = json.dumps({
        "deviceSn": sn, #2309200154,
        "startTime": start_date.isoformat(), #"2024-12-30",
        "endTime": (start_date  + timedelta(days=1)).isoformat(), #"2024-12-31",
        "timeType": type #1
        })
    headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'UNHCR_STEVE',
    'Authorization': f'Bearer {BIZ_ACCESS_TOKEN}',
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    res = response.json()
    if 'success' not in res or res['success'] != True:
        return None, 'API call not successful'
    data = res['paramDataList']
    err = None
    if eng:
        res, err = err_handler.error_wrapper(lambda: insert_inverter_data(eng, data))
    pass
    if err:
        return None, err
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
        "start_operating_time": datetime.utcfromtimestamp(station["startOperatingTime"]) if station.get("startOperatingTime") else None,
        "station_image": station.get("stationImage"),
        "created_date": datetime.utcfromtimestamp(station["createdDate"]) if station.get("createdDate") else None,
        "battery_soc": station.get("batterySoc"),
        "network_status": station.get("networkStatus"),
        "generation_power": station.get("generationPower"),
        "last_update_time": datetime.utcfromtimestamp(station["lastUpdateTime"]) if station.get("lastUpdateTime") else None,
        "contact_phone": station.get("contactPhone"),
        "owner_name": station.get("ownerName"),
    }


def upsert_stations(stations_data, eng=None):
    if eng is None:
        eng = db.set_local_defaultdb_engine()
    transformed_data = [transform_station_data(st) for st in stations_data]

    stmt = insert(Station).values(transformed_data)

    update_columns = {
        col.name: getattr(stmt.excluded, col.name)
        for col in Station.__table__.columns
        #if col.name not in ("id",)  # Exclude primary key
    }

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=["name"],  # Unique constraint field
        set_=update_columns
    )
    Session = sessionmaker(bind=eng)
    session = Session()
    session.execute(upsert_stmt)
    session.commit()


