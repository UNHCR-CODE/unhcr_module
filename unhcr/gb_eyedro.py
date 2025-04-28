
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import partial
from itertools import chain
import json
import time
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
from sqlalchemy import text
import time

from unhcr import app_utils
from unhcr import constants as const
from unhcr import db
from unhcr import err_handler

run_dt = datetime.now().date()
mods = [["app_utils", "app_utils"],["constants", "const"],["db", "db"],["err_handler", "err_handler"]]
res = app_utils.app_init(mods=mods, log_file="unhcr.gb_eyedro.log", version="0.4.8", level="INFO", override=False)
logger = res[0]
if const.LOCAL:  # testing with local python files
    logger, app_utils, const, db, err_handler = res

epoch_2024 = 1704067199
epoch_2023 = 1672502399
epoch_2022 = 1640937599
epoch_2021 = 1609372799
epoch_2020 = 1577807999

def map_gb(device_data):
    if all(len(sublist) == 0 for sublist in device_data['Wh']):
        return pd.DataFrame()

    parameter_mapping = {"A": "a_p", "V": "v_p", "PF": "pf_p", "Wh": "wh_p"}
    records = {}

    for param, lists in device_data.items():
        param_prefix = parameter_mapping.get(param, param)  # Default to param name if not in mapping
        for i, dataset in enumerate(lists):
            for timestamp, value in dataset:
                ts_str = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                if ts_str not in records:
                    records[ts_str] = {"ts": ts_str, "epoch_secs": timestamp}  # Initialize timestamp row
                records[ts_str][f"{param_prefix}{i + 1}"] = value
    # Convert records dict to DataFrame
    df = pd.DataFrame.from_dict(records, orient="index")
    # !!! no need for this, but might use it later
    # # Ensure missing columns (V & PF) exist with NaN
    # expected_columns = ["ts"] + [f"{p}{i}" for p in ["a_p", "v_p", "pf_p", "wh_p"] for i in range(1, 4)]
    # for col in expected_columns:
    #     if col not in df:
    #         df[col] = None  # Or df[col] = 0 for default values

    # if amps are missing, drop the row Eyedro send wh when there are no amps
    # Define the required columns
    required_columns = ['a_p1', 'a_p2', 'a_p3']

    # Check if all required columns are in the DataFrame
    if all(col in df.columns for col in required_columns):
        # If all required columns exist, drop rows with NaN in those columns
        df = df.dropna(subset=required_columns)
    else:
        # If any required columns are missing, remove all rows
        df = df.iloc[0:0]  # This clears the DataFrame
    if not df.empty:
        df.loc[:, "api_flag"] = -666
     # flag using new API
    return df.sort_values("ts").reset_index(drop=True)


def api_get_gb_user_info():
    """
    Retrieve user info data from the GreenButton API v1.

    Parameters
    ----------
    None

    Returns
    -------
    user_info_data : dict
        The user info data retrieved from the API, or None if an error occurred.
    err : str
        An error message if an error occurred, otherwise None.
    """
    url = (
        const.GB_API_V1_API_BASE_URL
        + const.GB_API_V1_GET_DEVICE_LIST
        + const.GB_API_V1_USER_KEY
    )
    payload = {}
    headers = {}

    response, err = err_handler.error_wrapper(
        lambda: requests.request("GET", url, headers=headers, data=payload)
    )
    if err:
        return None, err
    user_info_data, err = err_handler.error_wrapper(lambda: response.json())
    if err:
        return None, err
    return user_info_data, None


def parse_user_info_as_df(user_info_data):
    """
    Parse the user info data from the GreenButton API into a pandas DataFrame.

    Parameters
    ----------
    user_info_data : dict
        The user info data retrieved from the API.

    Returns
    -------
    final_df : pandas.DataFrame
        A DataFrame containing the parsed user info data, with columns
        'site_label', 'device_serial', 'last_comm', and 'state'. If an error
        occurred, the return value will be None.
    """

    top_level = user_info_data.get("UserInfo", {}).get("DeviceSerialList", [])

    top_level_serials = []
    for sn in top_level:
        top_level_serials.append(str(sn)[:3] + "-" + str(sn)[3:])

    # Extract DeviceSerials and SiteLabels from DisplayGroupList
    device_data = []
    for site in user_info_data.get("UserInfo", {}).get("SiteList", []):
        site_label = site.get("SiteLabel")
        for group in site.get("DisplayGroupList", []):
            for device in group.get("DeviceList", []):
                sn = device.get("DeviceSerial")
                sn = str(sn)[:3] + "-" + str(sn)[3:]
                last_com = device.get("LastCommSecUtc")
                state = device.get("State")
                device_data.append((site_label, sn, last_com, state))

    # Convert device_data into a dictionary for quick lookup
    serial_info = {item[1]: item for item in device_data}

    # Step 1: Merge data while keeping all of top_level_serials
    merged_data = []
    added_serials = set()

    # Add serials from top_level_serials, along with their details if available
    for sn in top_level_serials:
        if sn in serial_info:
            merged_data.append(
                [sn] + list(serial_info[sn][0:1]) + list(serial_info[sn][2:])
            )
            added_serials.add(sn)
        else:
            merged_data.append([sn])
            added_serials.add(sn)

    # Step 2: Add any row from device_data that did not match top_level_serials
    for item in serial_info.values():
        serial_num = item[1]
        if serial_num not in added_serials:
            merged_data.append([serial_num, item[0], item[2], item[3]])

    # Step 3: Remove standalone serials if a detailed entry exists
    final_data = []
    seen_serials = set()

    for row in merged_data:
        serial_num = row[0]
        if serial_num in seen_serials:
            continue  # Skip duplicate standalone serials
        seen_serials.add(serial_num)
        if len(row) == 1 and any(
            r[0] == serial_num and len(r) > 1 for r in merged_data
        ):
            continue  # Remove standalone if detailed version exists
        final_data.append(row)

    # Optionally: Get the count of final rows
    logger.debug(f"Final row count: {len(final_data)}")
    # Convert merged data to a pandas DataFrame
    final_df = pd.DataFrame(final_data, columns=const.GB_SN_COLS)

    return final_df


def api_get_user_info_as_df():
    """
    Gets the user info data from the API, parses it into a list of tuples of
    (site_label, device_serial), and returns the list or an error string.

    Returns:
        pd.DataFrame: a DataFrame with columns 'site_label', 'device_serial',
            'last_comm_sec_utc', 'state'
        str: error string if an error occurred
    """
    # Extract top-level DeviceSerialList
    user_info_data, err = api_get_gb_user_info()
    if err or len(user_info_data["Errors"]) != 0:
        err_str = f"app_gb_serial_nums: Failed to get user info data ERROR: {err}"
        if user_info_data and "Errors" in user_info_data:
            err_str += f" {user_info_data['Errors']}"
        logger.error(err_str)
        return None, err_str
    all_serials_df, err = err_handler.error_wrapper(
        lambda: parse_user_info_as_df(user_info_data)
    )
    if err:
        err_str = f"app_gb_serial_nums: Failed to parse user info data ERROR: {err}"
        logger.error(err_str)
        return None, err_str
    return all_serials_df, None


def db_create_tables_1(serials, db_eng=db.set_local_defaultdb_engine()):
    res = True
    err = None
    conn = db_eng.raw_connection()  # Get raw psycopg2 connection
    try:
        conn = db_eng.connect()
        with conn as cur:
            for serial in serials:
                serial = serial.replace("gb_", "").lower()
                print(serial)
                sql = f"""
        ------------ !!!!!! DROP TABLE IF EXISTS eyedro.gb_{serial} CASCADE;
        CREATE TABLE IF NOT EXISTS eyedro.gb_{serial} (
        epoch_secs BIGINT NOT NULL,
        ts timestamp not NULL,
        a_p1 float8 NULL,
        a_p2 float8 NULL,
        a_p3 float8 NULL,
        v_p1 float8 NULL,
        v_p2 float8 NULL,
        v_p3 float8 NULL,
        pf_p1 float8 NULL,
        pf_p2 float8 NULL,
        pf_p3 float8 NULL,
        wh_p1 float8 NULL,
        wh_p2 float8 NULL,
        wh_p3 float8 NULL,
        CONSTRAINT gb_{serial}_pkey PRIMARY KEY (ts, epoch_secs)
    );

    SELECT create_hypertable('eyedro.gb_{serial}', 'ts', if_not_exists => TRUE, migrate_data => true);

    DROP MATERIALIZED VIEW IF EXISTS eyedro.gb_{serial}_hourly cascade;
    CREATE MATERIALIZED VIEW eyedro.gb_{serial}_hourly 
    WITH (timescaledb.continuous) AS
    SELECT 
        time_bucket('1h', ts) AS ts_hr,

        -- Power Aggregation (Ensuring No Negative Values)
        AVG(ABS(a_p1)) FILTER (WHERE a_p1 IS NOT NULL AND a_p1::TEXT <> 'NaN') AS avg_amps_p1,
        MIN(ABS(a_p1)) FILTER (WHERE a_p1 IS NOT NULL AND a_p1::TEXT <> 'NaN') AS min_amps_p1,
        MAX(ABS(a_p1)) FILTER (WHERE a_p1 IS NOT NULL AND a_p1::TEXT <> 'NaN') AS max_amps_p1,

        AVG(ABS(a_p2)) FILTER (WHERE a_p2 IS NOT NULL AND a_p2::TEXT <> 'NaN') AS avg_amps_p2,
        MIN(ABS(a_p2)) FILTER (WHERE a_p2 IS NOT NULL AND a_p2::TEXT <> 'NaN') AS min_amps_p2,
        MAX(ABS(a_p2)) FILTER (WHERE a_p2 IS NOT NULL AND a_p2::TEXT <> 'NaN') AS max_amps_p2,

        AVG(ABS(a_p3)) FILTER (WHERE a_p3 IS NOT NULL AND a_p3::TEXT <> 'NaN') AS avg_amps_p3,
        MIN(ABS(a_p3)) FILTER (WHERE a_p3 IS NOT NULL AND a_p3::TEXT <> 'NaN') AS min_amps_p3,
        MAX(ABS(a_p3)) FILTER (WHERE a_p3 IS NOT NULL AND a_p3::TEXT <> 'NaN') AS max_amps_p3,

        -- Voltage Aggregation
        AVG(ABS(v_p1)) FILTER (WHERE v_p1 IS NOT NULL AND v_p1::TEXT <> 'NaN') AS avg_volts_p1,
        MIN(ABS(v_p1)) FILTER (WHERE v_p1 IS NOT NULL AND v_p1::TEXT <> 'NaN') AS min_volts_p1,
        MAX(ABS(v_p1)) FILTER (WHERE v_p1 IS NOT NULL AND v_p1::TEXT <> 'NaN') AS max_volts_p1,

        AVG(ABS(v_p2)) FILTER (WHERE v_p2 IS NOT NULL AND v_p2::TEXT <> 'NaN') AS avg_volts_p2,
        MIN(ABS(v_p2)) FILTER (WHERE v_p2 IS NOT NULL AND v_p2::TEXT <> 'NaN') AS min_volts_p2,
        MAX(ABS(v_p2)) FILTER (WHERE v_p2 IS NOT NULL AND v_p2::TEXT <> 'NaN') AS max_volts_p2,

        AVG(ABS(v_p3)) FILTER (WHERE v_p3 IS NOT NULL AND v_p3::TEXT <> 'NaN') AS avg_volts_p3,
        MIN(ABS(v_p3)) FILTER (WHERE v_p3 IS NOT NULL AND v_p3::TEXT <> 'NaN') AS min_volts_p3,
        MAX(ABS(v_p3)) FILTER (WHERE v_p3 IS NOT NULL AND v_p3::TEXT <> 'NaN') AS max_volts_p3,

        -- Power Factor Aggregation (Weighted, Ensuring No Negative Values)
        ABS(SUM(pf_p1 * a_p1)) / NULLIF(SUM(ABS(a_p1)) FILTER (WHERE a_p1 IS NOT NULL AND a_p1::TEXT <> 'NaN'), 0) AS weighted_pf_p1,
        MIN(ABS(pf_p1)) FILTER (WHERE pf_p1 IS NOT NULL AND pf_p1::TEXT <> 'NaN') AS min_pf_p1,
        MAX(ABS(pf_p1)) FILTER (WHERE pf_p1 IS NOT NULL AND pf_p1::TEXT <> 'NaN') AS max_pf_p1,

        ABS(SUM(pf_p2 * a_p2)) / NULLIF(SUM(ABS(a_p2)) FILTER (WHERE a_p2 IS NOT NULL AND a_p2::TEXT <> 'NaN'), 0) AS weighted_pf_p2,
        MIN(ABS(pf_p2)) FILTER (WHERE pf_p2 IS NOT NULL AND pf_p2::TEXT <> 'NaN') AS min_pf_p2,
        MAX(ABS(pf_p2)) FILTER (WHERE pf_p2 IS NOT NULL AND pf_p2::TEXT <> 'NaN') AS max_pf_p2,

        ABS(SUM(pf_p3 * a_p3)) / NULLIF(SUM(ABS(a_p3)) FILTER (WHERE a_p3 IS NOT NULL AND a_p3::TEXT <> 'NaN'), 0) AS weighted_pf_p3,
        MIN(ABS(pf_p3)) FILTER (WHERE pf_p3 IS NOT NULL AND pf_p3::TEXT <> 'NaN') AS min_pf_p3,
        MAX(ABS(pf_p3)) FILTER (WHERE pf_p3 IS NOT NULL AND pf_p3::TEXT <> 'NaN') AS max_pf_p3,

        -- Energy Aggregation (Wh) Ensuring No Negative Values
        AVG(ABS(wh_p1)) FILTER (WHERE wh_p1 IS NOT NULL AND wh_p1::TEXT <> 'NaN') AS avg_wh_p1,
        MIN(ABS(wh_p1)) FILTER (WHERE wh_p1 IS NOT NULL AND wh_p1::TEXT <> 'NaN') AS min_wh_p1,
        MAX(ABS(wh_p1)) FILTER (WHERE wh_p1 IS NOT NULL AND wh_p1::TEXT <> 'NaN') AS max_wh_p1,
        SUM(ABS(wh_p1)) FILTER (WHERE wh_p1 IS NOT NULL AND wh_p1::TEXT <> 'NaN') AS ttl_wh_p1,

        AVG(ABS(wh_p2)) FILTER (WHERE wh_p2 IS NOT NULL AND wh_p2::TEXT <> 'NaN') AS avg_wh_p2,
        MIN(ABS(wh_p2)) FILTER (WHERE wh_p2 IS NOT NULL AND wh_p2::TEXT <> 'NaN') AS min_wh_p2,
        MAX(ABS(wh_p2)) FILTER (WHERE wh_p2 IS NOT NULL AND wh_p2::TEXT <> 'NaN') AS max_wh_p2,
        SUM(ABS(wh_p2)) FILTER (WHERE wh_p2 IS NOT NULL AND wh_p2::TEXT <> 'NaN') AS ttl_wh_p2,

        AVG(ABS(wh_p3)) FILTER (WHERE wh_p3 IS NOT NULL AND wh_p3::TEXT <> 'NaN') AS avg_wh_p3,
        MIN(ABS(wh_p3)) FILTER (WHERE wh_p3 IS NOT NULL AND wh_p3::TEXT <> 'NaN') AS min_wh_p3,
        MAX(ABS(wh_p3)) FILTER (WHERE wh_p3 IS NOT NULL AND wh_p3::TEXT <> 'NaN') AS max_wh_p3,
        SUM(ABS(wh_p3)) FILTER (WHERE wh_p3 IS NOT NULL AND wh_p3::TEXT <> 'NaN') AS ttl_wh_p3

    FROM eyedro.gb_{serial}
    where a_p1 is not null and a_p2 is not null and a_p3 is not null
    GROUP BY ts_hr
    WITH NO DATA;

    SELECT add_continuous_aggregate_policy(
        'eyedro.gb_{serial}_hourly',
        start_offset => INTERVAL '15 days',
        end_offset => INTERVAL '1 hour',
        schedule_interval => INTERVAL '6 hours'
        );
    """
                trans = conn.begin()
                res = conn.execute(text(sql))
                sql = f"""
                SELECT 55 FROM pg_indexes 
                WHERE schemaname = 'eyedro'
                AND tablename = 'gb_{serial}' 
                AND indexname = 'gb_{serial}_ts_idx';
                """
                res =  conn.execute(text(sql))
                cnt = res.rowcount
                res = res._allrows()
                print(f"XXXXXX', {serial}, {res}")
                if cnt != 1 or res[0][0] != 55:
                    sql = f"""
                        CREATE INDEX gb_{serial}_ts_idx ON eyedro.gb_{serial} USING btree (ts DESC);
                        """
                    res = conn.execute(text(sql))
                    cnt = res.rowcount
                    print(f"YYYYYY' {serial} {cnt}  {res}")

                trans.commit()
                trans.close()
                with db_eng.connect().execution_options(isolation_level='AUTOCOMMIT') as procedure_conn:
                    # Direct execution of stored procedure 
                    sql = f"CALL refresh_continuous_aggregate('eyedro.gb_{serial}_hourly', NULL, localtimestamp + INTERVAL '2 days')"
                    procedure_sql = text(sql)
                    # Execute in autocommit mode
                    res = procedure_conn.execute(procedure_sql)
                    cnt = res.rowcount
                    print(f"ZZZZZZZ' {serial} {cnt}")
                    pass
        return 'All good Houston', None
    except psycopg2.DatabaseError as e:
        conn.rollback()  # Rollback on failure
        logger.error(f"Database error during table creation: {e}")
        err = e
    except Exception as e:
        conn.rollback()  # Rollback on any other failure
        logger.error(f"Unexpected error: {e}")
        err = e
    finally:
        conn.close()  # ✅ Always close the connection

    return res, err

def db_create_tables_2(serials, db_eng=db.set_local_defaultdb_engine()):
    res = True
    err = None
    conn = db_eng.raw_connection()  # Get raw psycopg2 connection
    try:
        conn = db_eng.connect()

        for serial in serials:
            with conn.begin():
                serial = serial.replace("gb_", "").lower()
                print(serial)
                sql = f"""
        ------------ !!!!!! DROP TABLE IF EXISTS eyedro.gb_{serial} CASCADE;
        CREATE TABLE IF NOT EXISTS eyedro.gb_{serial} (
        epoch_secs BIGINT NOT NULL,
        ts timestamp not NULL,
        a_p1 float8 NULL,
        a_p2 float8 NULL,
        a_p3 float8 NULL,
        v_p1 float8 NULL,
        v_p2 float8 NULL,
        v_p3 float8 NULL,
        pf_p1 float8 NULL,
        pf_p2 float8 NULL,
        pf_p3 float8 NULL,
        wh_p1 float8 NULL,
        wh_p2 float8 NULL,
        wh_p3 float8 NULL,
        CONSTRAINT gb_{serial}_pkey PRIMARY KEY (ts)
    );

    SELECT create_hypertable('eyedro.gb_{serial}', 'ts', if_not_exists => TRUE, migrate_data => true);

    DROP MATERIALIZED VIEW IF EXISTS eyedro.gb_{serial}_hourly cascade;
    CREATE MATERIALIZED VIEW eyedro.gb_{serial}_hourly 
    WITH (timescaledb.continuous) AS
    SELECT 
        time_bucket('1h', ts) AS ts_hr,

        -- Power Aggregation (Ensuring No Negative Values)
        AVG(ABS(a_p1)) FILTER (WHERE a_p1 IS NOT NULL AND a_p1::TEXT <> 'NaN') AS avg_amps_p1,
        MIN(ABS(a_p1)) FILTER (WHERE a_p1 IS NOT NULL AND a_p1::TEXT <> 'NaN') AS min_amps_p1,
        MAX(ABS(a_p1)) FILTER (WHERE a_p1 IS NOT NULL AND a_p1::TEXT <> 'NaN') AS max_amps_p1,

        AVG(ABS(a_p2)) FILTER (WHERE a_p2 IS NOT NULL AND a_p2::TEXT <> 'NaN') AS avg_amps_p2,
        MIN(ABS(a_p2)) FILTER (WHERE a_p2 IS NOT NULL AND a_p2::TEXT <> 'NaN') AS min_amps_p2,
        MAX(ABS(a_p2)) FILTER (WHERE a_p2 IS NOT NULL AND a_p2::TEXT <> 'NaN') AS max_amps_p2,

        AVG(ABS(a_p3)) FILTER (WHERE a_p3 IS NOT NULL AND a_p3::TEXT <> 'NaN') AS avg_amps_p3,
        MIN(ABS(a_p3)) FILTER (WHERE a_p3 IS NOT NULL AND a_p3::TEXT <> 'NaN') AS min_amps_p3,
        MAX(ABS(a_p3)) FILTER (WHERE a_p3 IS NOT NULL AND a_p3::TEXT <> 'NaN') AS max_amps_p3,

        -- Voltage Aggregation
        AVG(ABS(v_p1)) FILTER (WHERE v_p1 IS NOT NULL AND v_p1::TEXT <> 'NaN') AS avg_volts_p1,
        MIN(ABS(v_p1)) FILTER (WHERE v_p1 IS NOT NULL AND v_p1::TEXT <> 'NaN') AS min_volts_p1,
        MAX(ABS(v_p1)) FILTER (WHERE v_p1 IS NOT NULL AND v_p1::TEXT <> 'NaN') AS max_volts_p1,

        AVG(ABS(v_p2)) FILTER (WHERE v_p2 IS NOT NULL AND v_p2::TEXT <> 'NaN') AS avg_volts_p2,
        MIN(ABS(v_p2)) FILTER (WHERE v_p2 IS NOT NULL AND v_p2::TEXT <> 'NaN') AS min_volts_p2,
        MAX(ABS(v_p2)) FILTER (WHERE v_p2 IS NOT NULL AND v_p2::TEXT <> 'NaN') AS max_volts_p2,

        AVG(ABS(v_p3)) FILTER (WHERE v_p3 IS NOT NULL AND v_p3::TEXT <> 'NaN') AS avg_volts_p3,
        MIN(ABS(v_p3)) FILTER (WHERE v_p3 IS NOT NULL AND v_p3::TEXT <> 'NaN') AS min_volts_p3,
        MAX(ABS(v_p3)) FILTER (WHERE v_p3 IS NOT NULL AND v_p3::TEXT <> 'NaN') AS max_volts_p3,

        -- Power Factor Aggregation (Weighted, Ensuring No Negative Values)
        ABS(SUM(pf_p1 * a_p1)) / NULLIF(SUM(ABS(a_p1)) FILTER (WHERE a_p1 IS NOT NULL AND a_p1::TEXT <> 'NaN'), 0) AS weighted_pf_p1,
        MIN(ABS(pf_p1)) FILTER (WHERE pf_p1 IS NOT NULL AND pf_p1::TEXT <> 'NaN') AS min_pf_p1,
        MAX(ABS(pf_p1)) FILTER (WHERE pf_p1 IS NOT NULL AND pf_p1::TEXT <> 'NaN') AS max_pf_p1,

        ABS(SUM(pf_p2 * a_p2)) / NULLIF(SUM(ABS(a_p2)) FILTER (WHERE a_p2 IS NOT NULL AND a_p2::TEXT <> 'NaN'), 0) AS weighted_pf_p2,
        MIN(ABS(pf_p2)) FILTER (WHERE pf_p2 IS NOT NULL AND pf_p2::TEXT <> 'NaN') AS min_pf_p2,
        MAX(ABS(pf_p2)) FILTER (WHERE pf_p2 IS NOT NULL AND pf_p2::TEXT <> 'NaN') AS max_pf_p2,

        ABS(SUM(pf_p3 * a_p3)) / NULLIF(SUM(ABS(a_p3)) FILTER (WHERE a_p3 IS NOT NULL AND a_p3::TEXT <> 'NaN'), 0) AS weighted_pf_p3,
        MIN(ABS(pf_p3)) FILTER (WHERE pf_p3 IS NOT NULL AND pf_p3::TEXT <> 'NaN') AS min_pf_p3,
        MAX(ABS(pf_p3)) FILTER (WHERE pf_p3 IS NOT NULL AND pf_p3::TEXT <> 'NaN') AS max_pf_p3,

        -- Energy Aggregation (Wh) Ensuring No Negative Values
        AVG(ABS(wh_p1)) FILTER (WHERE wh_p1 IS NOT NULL AND wh_p1::TEXT <> 'NaN') AS avg_wh_p1,
        MIN(ABS(wh_p1)) FILTER (WHERE wh_p1 IS NOT NULL AND wh_p1::TEXT <> 'NaN') AS min_wh_p1,
        MAX(ABS(wh_p1)) FILTER (WHERE wh_p1 IS NOT NULL AND wh_p1::TEXT <> 'NaN') AS max_wh_p1,
        SUM(ABS(wh_p1)) FILTER (WHERE wh_p1 IS NOT NULL AND wh_p1::TEXT <> 'NaN') AS ttl_wh_p1,

        AVG(ABS(wh_p2)) FILTER (WHERE wh_p2 IS NOT NULL AND wh_p2::TEXT <> 'NaN') AS avg_wh_p2,
        MIN(ABS(wh_p2)) FILTER (WHERE wh_p2 IS NOT NULL AND wh_p2::TEXT <> 'NaN') AS min_wh_p2,
        MAX(ABS(wh_p2)) FILTER (WHERE wh_p2 IS NOT NULL AND wh_p2::TEXT <> 'NaN') AS max_wh_p2,
        SUM(ABS(wh_p2)) FILTER (WHERE wh_p2 IS NOT NULL AND wh_p2::TEXT <> 'NaN') AS ttl_wh_p2,

        AVG(ABS(wh_p3)) FILTER (WHERE wh_p3 IS NOT NULL AND wh_p3::TEXT <> 'NaN') AS avg_wh_p3,
        MIN(ABS(wh_p3)) FILTER (WHERE wh_p3 IS NOT NULL AND wh_p3::TEXT <> 'NaN') AS min_wh_p3,
        MAX(ABS(wh_p3)) FILTER (WHERE wh_p3 IS NOT NULL AND wh_p3::TEXT <> 'NaN') AS max_wh_p3,
        SUM(ABS(wh_p3)) FILTER (WHERE wh_p3 IS NOT NULL AND wh_p3::TEXT <> 'NaN') AS ttl_wh_p3

    FROM eyedro.gb_{serial}
    where a_p1 is not null and a_p2 is not null and a_p3 is not null
    GROUP BY ts_hr
    WITH NO DATA;

    SELECT add_continuous_aggregate_policy(
        'eyedro.gb_{serial}_hourly',
        start_offset => INTERVAL '15 days',
        end_offset => INTERVAL '1 hour',
        schedule_interval => INTERVAL '6 hours'
        );
    """
                res = conn.execute(text(sql))
                sql = f"""
                SELECT 55 FROM pg_indexes 
                WHERE schemaname = 'eyedro'
                AND tablename = 'gb_{serial}' 
                AND indexname = 'gb_{serial}_ts_idx';
                """
                res =  conn.execute(text(sql))
                cnt = res.rowcount
                res = res._allrows()
                print(f"XXXXXX', {serial}, {res}")
                if cnt != 1 or res[0][0] != 55:
                    sql = f"""
                        CREATE INDEX gb_{serial}_ts_idx ON eyedro.gb_{serial} USING btree (ts DESC);
                        CREATE UNIQUE INDEX gb_{serial}_ts_epoch_idx ON eyedro.gb_{serial} USING btree (ts, epoch_secs);
                        """
                    res = conn.execute(text(sql))
                    cnt = res.rowcount
                    print(f"YYYYYY' {serial} {cnt}  {res}")

            with db_eng.connect().execution_options(isolation_level='AUTOCOMMIT') as procedure_conn:

                # Direct execution of stored procedure 
                sql = f"CALL refresh_continuous_aggregate('eyedro.gb_{serial}_hourly', NULL, localtimestamp + INTERVAL '2 days')"
                procedure_sql = text(sql)
                # Execute in autocommit mode
                res = procedure_conn.execute(procedure_sql)
                cnt = res.rowcount
                print(f"ZZZZZZZ' {serial} {cnt}")
                pass
            pass
        return 'All good Houston', None
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error during table creation: {e}")
        err = e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        err = e
    finally:
        conn.close()  # ✅ Always close the connection

    return res, err


def log_gb_errors(errs, logger):
    logger.debug(f"ERRORS: {errs}")
    if len(errs) > 1 and any('DeviceSerial invalid' in str(item) for item in errs[1]):
        return errs[1]
    elif len(errs) > 2 and any('Invalid DateStartSecUtc' in str(item) for item in errs[2]):
        logger.debug('Invalid DateStartSecUtc NORMAL END OF DATA')
        return errs[2]
    elif any('API' in str(item) for item in errs[0]):
        return errs[0]
    return errs


def meter_response_empty(serial, epoch=None):
    url = f'{const.GB_API_V1_API_BASE_URL}{const.GB_API_V1_GET_DATA}'
    epoch_str = f"&DateStartSecUtc={str(epoch)}" if epoch else ""
    meter_url = f"{url}{str(serial)}{epoch_str}&DateNumSteps=1440&UserKey={const.GB_API_V1_EMPTY_KEY}"

    response = requests.get(meter_url, timeout=600)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return json.loads(response.text)


def get_last_com_epoch(s_num, logger=logger):
    # Get UTC midnight for today
    midnight_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # Convert to epoch (seconds since Unix epoch)
    midnight_epoch = int(midnight_utc.timestamp())
    data = meter_response_empty(s_num)
    if data is None:
        ###logger.error(f"ZZZ {s_num} ERROR: NO DATA")
        return midnight_epoch, None
    if data['Errors']:
        err = log_gb_errors(data['Errors'], logger)
        ##logger.error(f"ZZZ {s_num}  {midnight_epoch} ERRORS: {err}")
        return midnight_epoch, data
    last_comm = data['LastCommSecUtc']
    return app_utils.get_previous_midnight_epoch(last_comm), data

def update_gb_db(serial, df, engine, msg=''):
    """
    Updates the database with new data for a given serial number using an UPSERT operation.

    This function takes a DataFrame containing device data and inserts it into a PostgreSQL table
    named "gb_<serial>". If a record with the same timestamp and epoch_secs already exists, it updates
    the existing record with new values for each parameter. The function commits the transaction after
    successfully inserting/updating the records, and logs the number of inserted and updated rows.

    Args:
        serial (str): The serial number of the device.
        df (pd.DataFrame): A DataFrame containing the device data to be inserted/updated.
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine connected to the PostgreSQL database.

    Returns:
        tuple: A tuple containing a list with the number of inserted and updated records, and an error object (if any).
               If an error occurs, the function returns (None, error).
    """

    columns_str = ", ".join(df.columns)
    # Use ON CONFLICT for UPSERT (Insert or Update)
    upsert_sql = f"""
WITH insert_attempt AS (
    INSERT INTO eyedro.gb_{serial} ({columns_str})
    VALUES %s
    ON CONFLICT (ts, epoch_secs) DO UPDATE SET
        a_p1 = EXCLUDED.a_p1,
        a_p2 = EXCLUDED.a_p2,
        a_p3 = EXCLUDED.a_p3,
        v_p1 = EXCLUDED.v_p1,
        v_p2 = EXCLUDED.v_p2,
        v_p3 = EXCLUDED.v_p3,
        pf_p1 = EXCLUDED.pf_p1,
        pf_p2 = EXCLUDED.pf_p2,
        pf_p3 = EXCLUDED.pf_p3,
        wh_p1 = EXCLUDED.wh_p1,
        wh_p2 = EXCLUDED.wh_p2,
        wh_p3 = EXCLUDED.wh_p3,
        api_flag = EXCLUDED.api_flag
    RETURNING xmax = 0 AS inserted
)
SELECT 
    COUNT(*) FILTER (WHERE inserted) AS inserted_count,
    COUNT(*) FILTER (WHERE NOT inserted) AS updated_count
FROM insert_attempt;
"""

    conn = engine.raw_connection()  # Get raw psycopg2 connection
    try:
        with conn.cursor() as cur:
            # cur.executemany(upsert_sql, df.to_records(index=False).tolist())
            execute_values(cur, upsert_sql, df.to_records(index=False).tolist(),page_size=1500)
            #xx = cur.rowcount
                    # Fetch the inserted count
            ######cur.execute("SELECT COUNT(*) FROM insert_attempt")  # Correct query to count
            # Get the inserted count from RETURNING
            #inserted_count = len(cur.fetchall()) # cur.rowcount #cur.fetchone()[0]  # Fetch single row result
            inserted_count, updated_count = cur.fetchone()  
            #print(f"Inserted: {inserted_count}, Updated: {updated_count}")

            # Use rowcount to get total affected rows (inserts + updates)
            # total_affected = len(df)

            # # Calculate updated count
            # updated_count = total_affected - inserted_count

            # Commit transaction
            conn.commit()

    except psycopg2.DatabaseError as e:
        conn.rollback()  # Rollback on failure
        logger.error(f"ZZZ {serial} update_gb_db Database error during UPSERT: {e}")
        return None, e
    except Exception as e:
        conn.rollback()  # Rollback on any other failure
        logger.error(f"ZZZ {serial} update_gb_db Unexpected error: {e}")
        return None, e
    finally:
        conn.close()
    logger.info(f'{serial} {df["ts"].min()} {df["ts"].max()} In: {inserted_count}, Up: {updated_count} ✅ {msg}')
    return [inserted_count, updated_count], None


def upsert_gb_data(s_num, engine, epoch_cutoff = epoch_2020, epoch_start = None, MAX_EMPTY=30, msg='', logger=logger):
    no_data_cnt = 0
    ttl_cnt = 0
    err_cnt = 0
    busy_cnt = 0
    ttl_inserted = 0
    ttl_updated = 0
    ttl_empty = 0
    MAX_BUSY = 7
    MAX_ERR = 15
    epoch, data = get_last_com_epoch(s_num, logger)
    if epoch < epoch_cutoff: # or s_num == '00980639':
        logger.info(f"ZZZ {s_num} last com before cutoff: {datetime.fromtimestamp(epoch, timezone.utc).isoformat()}\n{datetime.fromtimestamp(epoch_cutoff, timezone.utc).isoformat()}" )
        return s_num, ttl_cnt, err_cnt, ttl_inserted, ttl_updated, ttl_empty
    #!!!!! epoch = get_db_epoch(s_num, engine, max=True)
    #!!!!! epoch = 1740535260
    if data and "DeviceData" in data:
        df = map_gb(data["DeviceData"])
        if not df.empty:
            cnt, err = update_gb_db(s_num, df, engine, msg)
            if err:
                logger.error(f"ZZZ {s_num}  {epoch} ERRORS: {err}")
                return s_num, ttl_cnt, err_cnt, ttl_inserted, ttl_updated, ttl_empty
            ttl_inserted += cnt[0]
            ttl_updated += cnt[1]
            ttl_cnt += 1
            print(cnt, ttl_cnt, epoch, s_num)
        ####!!!! max_epoch_db = get_db_epoch(s_num, engine, max=True)
        #!!! min_epoch_db = get_db_epoch(s_num, local_engine, max=False)
    
    logger.info(f"{s_num} last com: {datetime.fromtimestamp(epoch, timezone.utc).isoformat()} cutoff: {datetime.fromtimestamp(epoch_cutoff, timezone.utc).isoformat()}" )
    if data and "Errors" in data:
        if len(data["Errors"]) == 0:
            epoch -= 86400
        else:
            logger.error(f"last_comm TRY AGAIN ZZZ {s_num}  {epoch} ERRORS: {data['Errors']}")
    elif data is None:
        logger.error(f"data None TRY AGAIN ZZZ {s_num}  {epoch}")
    ####epoch = min_epoch_db # start where we left off
    if epoch_start:
        epoch = epoch_start
        try:
            sql = f'select min(epoch_secs), min(ts) from eyedro.gb_{s_num};'
            res, err = db.sql_execute(sql, engine)
            if res[0][0] and res[0][0] < epoch_start:
                epoch = app_utils.get_previous_midnight_epoch(res[0][0])
        except: 
            pass

    while epoch >= epoch_cutoff and err_cnt < MAX_ERR and busy_cnt < MAX_BUSY:  #2024-01-01
        # if ttl_cnt > 7:
        #     #!!!!if epoch <= max_epoch_db and min_epoch_db <= epoch_cutoff:
        #     #    return ttl_cnt, err_cnt, ttl_inserted, ttl_updated
        #     return ttl_cnt, err_cnt, ttl_inserted, ttl_updated

        #     epoch = min_epoch_db
        #     ttl_cnt = -200
        data = meter_response_empty(s_num, epoch)
        ttl_cnt += 1
        # these first 3 errors will retry
        if data is None:
            no_data_cnt += 1
            err_cnt += 1
            logger.error(f"{err_cnt} ZZZ {s_num} {epoch} ERROR: NO DATA: {no_data_cnt}")
            ###epoch -= 86400
            time.sleep(.5)
            continue
        if 'Errors' in data and len(data['Errors']) != 0:
            gb_err = log_gb_errors(data['Errors'], logger)
            if 'Invalid DateStartSecUtc' in str(gb_err[1]):
                logger.info(f'Invalid DateStartSecUtc NORMAL END OF DATA  {s_num} {epoch} {ttl_cnt} {err_cnt} {ttl_inserted} {ttl_updated} {ttl_empty}')
                return s_num, ttl_cnt, err_cnt, ttl_inserted, ttl_updated, ttl_empty
            err_cnt += 1
            logger.error(f"{err_cnt} ZZZ {s_num}  {epoch} ERRORS: {gb_err}")
            if 'API Error' in gb_err[1]:
                print(f'{s_num} {epoch}  ERROR: API Error. Contact Eyedro Admin.')
                time.sleep(.5)
            if  'API busy' in gb_err[1]:
                print(f'{s_num} {epoch}  BUSY: ERROR: API Error. Contact Eyedro Admin.')
                busy_cnt += 1
                time.sleep(2)
            continue
        if 'DeviceData' not in data:
            err_cnt += 1
            no_data_cnt += 1
            logger.error(f"{err_cnt} ZZZ {s_num} {epoch} ERROR: NO DATA IN RESPONSE: {no_data_cnt}")
            time.sleep(.5)
            continue
        else:
            df = map_gb(data["DeviceData"])
            if not df.empty:
                cnt, err = update_gb_db(s_num, df, engine, msg)
                ttl_empty = 0
                err_cnt = 0
                busy_cnt = 0
                #!!! if gaps:
                #     columns = ['a_p1', 'a_p2', 'a_p3']
                #     existing_columns = [col for col in columns if col in df.columns] 
                #     if existing_columns:
                #         df_filtered = df[df[existing_columns].isna().any(axis=1)]  # Select rows with NaN in any of the existing columns
                #         df_filtered.loc[:, ~df_filtered.columns.isin(['ts', 'epoch_secs'])] = None

                #         cnt1, err1 = update_gb_db(s_num, df_filtered, engine, msg)
                #         print(epoch,datetime.fromtimestamp(epoch, timezone.utc).date(), s_num, err, err1)
                if cnt:
                    ttl_inserted += cnt[0]
                    ttl_updated += cnt[1]
            else:
                ttl_empty += 1
                if ttl_empty >= MAX_EMPTY:
                    logger.warning(f"ZZZ {s_num} {ttl_empty} data empty: {datetime.fromtimestamp(epoch, timezone.utc).isoformat()}\n{datetime.fromtimestamp(epoch_cutoff, timezone.utc).isoformat()}" )
                    break
        epoch -= 86400
        # event = keyboard.read_event(suppress=True) if keyboard.is_pressed('space') else None
        # if event and event.event_type == keyboard.KEY_DOWN:
        #     print("Key pressed! Skipping execution and continuing loop.")
        #     break

    if busy_cnt >= MAX_BUSY or err_cnt >= MAX_ERR or ttl_empty >= MAX_EMPTY:
        if busy_cnt >= MAX_BUSY:
            msg = 'BUSY'
        elif ttl_empty >= MAX_EMPTY:
            msg = 'EMPTY'
        else:
            msg = 'ERROR'
        logger.warning(f"EEEE {msg} {s_num} {epoch} ERROR: API errors too many times. exiting busy: {busy_cnt >= MAX_BUSY} {busy_cnt} errs: {err_cnt >= MAX_ERR} {err_cnt} empty: {ttl_empty >= MAX_EMPTY} {ttl_empty}")
    return s_num, ttl_cnt, err_cnt, ttl_inserted, ttl_updated, ttl_empty


def db_create_gb_gaps_table(db_eng=db.set_local_defaultdb_engine()):
    res, err = db.sql_execute(f'select epoch_secs from {const.GB_GAPS_TABLE} limit 1;', db_eng)
    if err:
        res, err = db.sql_execute(const.SQL_GB_GAPS_TABLE, db_eng)
        if err:
            print(const.SQL_GB_GAPS_TABLE)
            logger.error(err)
            return None, err
    logger.debug(res)
    return res, err


def db_get_gb_hypertables(db_eng=db.set_local_defaultdb_engine()):
    """
    Gets all hypertables in the "eyedro" schema that have a name like 'gb_%'.
    
    Parameters:
    end (datetime): The end date for the query.
    
    Returns:
    list of str: A list of hypertable names.
    """
    return db.sql_execute("""SELECT hypertable_name FROM timescaledb_information.hypertables 
    where hypertable_schema = 'eyedro' and hypertable_name like 'gb_%';""", db_eng)


def db_hyper_gb_gaps(hypertable_name, db_eng=db.set_local_defaultdb_engine()):
    """
    Collects all hypergaps in the "eyedro" schema and writes the results to a CSV file.
    
    The function first gets all hypertables in the "eyedro" schema, then runs a query on each one
    to detect gaps in the data. The results are collected in a list and then written to a CSV file.
    
    The query is a window query that calculates the difference between the current epoch and the previous
    epoch for each row in the table. If the difference is not equal to 60 (i.e., there is a gap), the row
    is included in the results.
    
    The results are written to a CSV file in the "data/gaps" directory. The file is named "eyedro_data_gaps.csv".
    """
    try:
        conn = db_eng.raw_connection()
        with conn.cursor() as cursor:
            query = f"""
            WITH ordered_epochs AS (
                SELECT epoch_secs, 
                    LAG(epoch_secs) OVER (ORDER BY epoch_secs) AS prev_epoch
                FROM eyedro.{hypertable_name}
            )
            SELECT '{hypertable_name}' AS hypertable, epoch_secs, prev_epoch, (epoch_secs - prev_epoch) AS diff_seconds
            FROM ordered_epochs
            WHERE (epoch_secs - prev_epoch) != 60;
            """

            cursor.execute(query)
            rows = cursor.fetchall()
            print(f"Processed hypertable {hypertable_name}, gaps found: {len(rows)}")
        conn.close()
        return rows

    except psycopg2.OperationalError as e:
        err = e
        print(f"Database connection error: {e}")
    except psycopg2.DatabaseError as e:
        err = e
        print(f"Database query error: {e}")
    except Exception as e:
        err = e
        print(f"Unexpected error: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

    return []


def hyper_gb_gaps_concur(ht_names, chunks=10, src='local', db_eng=db.set_local_defaultdb_engine()):
    def process_chunk(chunk, param1):
        return [db_hyper_gb_gaps(item[0], db_eng=param1) for item in chunk]  # Apply function to each item

    # ht_names = []
    # for item in res:
    #     ht_names.append(item[0][0])
    
    num_parts = chunks
    chunks = [list(chunk) for chunk in np.array_split(ht_names, num_parts)]  # Ensure list format

    # cutoff = datetime.now(timezone.utc) - timedelta(days=12)
    # epoch_cutoff = get_previous_midnight_epoch(int(cutoff.timestamp()))
    # local_engine = db.set_local_defaultdb_engine()

    with ThreadPoolExecutor(max_workers=num_parts) as executor:
        results = list(executor.map(partial(process_chunk, param1=db_eng), chunks))

    return results
    # Flatten results
    return [item for sublist in results for item in sublist]


def db_get_all_gb_gaps(src='local'):
    """
    Retrieves and processes data gaps for all "gb" hypertables.

    This function connects to a local or remote database, retrieves hypertable names,
    and uses concurrent execution to find data gaps in each hypertable. The results
    are compiled into a DataFrame and saved to a CSV file. A summary of the results
    is printed to the console.

    Args:
        src (str): Specifies the data source. Use 'local' for the local database 
                   and any other value for the remote database.

    Returns:
        None
    """

    ht_names, err = db_get_gb_hypertables()
    pass
    res = hyper_gb_gaps_concur(ht_names=ht_names)
    flattened_data = list(chain.from_iterable(chain.from_iterable(res)))
    #Step 3: Convert results to DataFrame
    df = pd.DataFrame(flattened_data, columns=["hypertable", "epoch_secs", "prev_epoch", "diff_seconds"])
    df['src'] = src

    #Save results to CSV
    df.to_csv(const.GB_GAPS_CSV, index=False)

    #Print summary
    print(df.head())
    print(f"✅ Data gaps found in {len(df)} rows. Results saved to 'eyedro_data_gaps.csv'.")



""" TimescaleDB Notes
what does this do ?    SELECT add_continuous_aggregate_policy(
        'eyedro.gb_{serial}_hourly',
        start_offset => INTERVAL '10 days',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '6 hours'
        );


ChatGPT said:
The SQL statement you provided uses the add_continuous_aggregate_policy function from TimescaleDB, which is a PostgreSQL extension for time-series data. This function adds a continuous aggregate policy to a hypertable, which allows TimescaleDB to automatically refresh materialized views (continuous aggregates) at scheduled intervals.

Here’s a breakdown of what each part of the query does:

sql
Copy
Edit
SELECT add_continuous_aggregate_policy(
    'eyedro.gb_{serial}_hourly',            -- Name of the continuous aggregate view
    start_offset => INTERVAL '10 days',    -- Start of the time window for the aggregate
    end_offset => INTERVAL '1 day',        -- End of the time window for the aggregate
    schedule_interval => INTERVAL '6 hours' -- Interval at which the continuous aggregate should refresh
);
Explanation of the Parameters:
'eyedro.gb_{serial}_hourly':

This is the name of the continuous aggregate view that you want to create (or apply the policy to). It seems to follow a pattern, where {serial} would be replaced with an actual value (e.g., a device serial number). This likely refers to a specific hypertable for hourly aggregates based on serial numbers.

start_offset => INTERVAL '10 days':

The start_offset defines the earliest time for which data will be included in the continuous aggregate. In this case, it's set to 10 days before the current time (i.e., it will start aggregating data from 10 days ago).

end_offset => INTERVAL '1 day':

The end_offset specifies how far in the future the aggregation should go. In this case, it’s set to 1 day into the future. This means the continuous aggregate will include data up to 1 day from the current time, i.e., it will have a rolling window of 9 days (10 days - 1 day).

schedule_interval => INTERVAL '6 hours':

This defines how often TimescaleDB should refresh the continuous aggregate. Here, it's set to every 6 hours. This means the aggregate view will be updated every 6 hours to include the latest data and maintain freshness.

What This Does:
The add_continuous_aggregate_policy function will create a policy to automatically refresh the continuous aggregate (materialized view) on the eyedro.gb_{serial}_hourly hypertable.

The continuous aggregate will use a rolling window: it will refresh every 6 hours and always include data from 10 days ago to 1 day in the future.

This allows you to have up-to-date aggregates without manually refreshing the view. It's useful for time-series data, where you want efficient querying of pre-aggregated data over large periods (e.g., hourly data for devices or sensors).

In Summary:
This policy will keep the materialized view eyedro.gb_{serial}_hourly updated automatically every 6 hours with a time window of 10 days back to 1 day ahead, which helps you efficiently manage and query large time-series datasets.

Let me know if you'd like more details on how continuous aggregates work in TimescaleDB!







You said:
does it recreate the entire materialized view every 6 hours
ChatGPT said:
No, the add_continuous_aggregate_policy in TimescaleDB does not recreate the entire materialized view every 6 hours. Instead, it incrementally refreshes the materialized view (continuous aggregate) by only updating the data that has changed or new data that has been added within the defined time window.

How it works:
Incremental Refresh: TimescaleDB uses a mechanism to update only the portion of the continuous aggregate that has changed since the last refresh. This means only the new or updated data within the specified time window (e.g., the last 6 hours in your example) is processed and added to the materialized view. This approach avoids recreating the entire materialized view, making the refresh process more efficient.

Automatic Refresh: The policy you created with schedule_interval => INTERVAL '6 hours' will trigger an automatic refresh every 6 hours. However, as mentioned, only the new data (or data that has been updated) will be processed and aggregated, rather than rebuilding the entire materialized view.

Materialized View: A materialized view in TimescaleDB stores precomputed query results, and when you set up a continuous aggregate, you're creating a special type of materialized view that TimescaleDB will maintain and refresh automatically based on the time window you specify.

Efficient Updates: The refresh process involves appending new data and potentially adjusting previously aggregated data if necessary (e.g., if records that affect the previous time window have been updated).

Example:
If you have data aggregated in 1-hour intervals for the past 10 days and the policy is set to refresh every 6 hours:

After 6 hours, TimescaleDB will only aggregate and append the new data for the past 6 hours to the existing materialized view, rather than recalculating the entire 10-day range.

The next refresh will update the data for the following 6-hour window, ensuring the materialized view remains up-to-date without needing to rebuild all the data.

Benefits:
Performance: The incremental refresh is highly efficient and ensures that you only work with a subset of data, avoiding the need to rebuild the entire materialized view each time.

Reduced Load: Since only new data is processed, the refresh operation consumes fewer resources, especially for large datasets.

Conclusion:
The continuous aggregate policy will not recreate the entire materialized view every 6 hours. Instead, it refreshes the view incrementally, ensuring only the relevant data is updated. This makes continuous aggregates very efficient for time-series data in TimescaleDB.

Let me know if you need more clarification!







You said:
would 30 days of 1 minute data take too long ?
ChatGPT said:
Whether 30 days of 1-minute data would take too long to process in TimescaleDB depends on several factors, such as:

Size of the Data:

If you have a large number of devices or sensors (i.e., a lot of rows per minute), the total amount of data can grow significantly. For example:

1 minute of data for a single sensor would result in 1440 rows per day.

For 30 days, this would be 43,200 rows per sensor.

If you have many sensors, this could result in millions or billions of rows.

Hardware Resources:

The performance of TimescaleDB largely depends on the hardware you're running it on. If you're on high-performance hardware (fast CPU, SSD storage, and sufficient RAM), TimescaleDB is generally very efficient at handling large datasets.

For typical time-series data, TimescaleDB can handle millions of rows per minute if it's configured correctly.

Indexes and Partitioning:

TimescaleDB automatically partitions your data into chunks based on time. This helps with efficient querying and insertion of data. The add_continuous_aggregate_policy function works best when your data is well-indexed and properly partitioned (which TimescaleDB handles automatically).

If you're aggregating data by minute, TimescaleDB can efficiently perform these aggregations on partitioned chunks.

Continuous Aggregate Policies:

With continuous aggregates, the view is not recalculated from scratch every time. It is updated incrementally, which is highly efficient. However, the first time the continuous aggregate is created (or if you force a full refresh), it will need to compute over all the data for the time window you're aggregating (e.g., the past 30 days of 1-minute data).

Depending on the size of the dataset and the complexity of the aggregation, this could take some time initially, but subsequent updates (when new data arrives) will be much faster due to the incremental nature of continuous aggregates.

Query Complexity:

If you plan on performing complex queries or aggregations (e.g., calculating averages, sums, etc.), the query performance can degrade as data grows unless it’s pre-aggregated with continuous aggregates.

Best Practices:
Batch Processing: If you're concerned about the initial load or query time for large datasets, you could consider processing the data in smaller batches instead of all at once.

Proper Indexing: Ensure your hypertable is well-indexed, especially on columns like timestamps and serial numbers (or whatever partitioning key you're using).

Monitoring Performance: Monitor your TimescaleDB instance’s performance while processing large datasets. You can use tools like pg_stat_activity, EXPLAIN ANALYZE, or TimescaleDB's built-in monitoring tools to understand how queries are performing and whether any bottlenecks exist.

Will 30 Days of 1-Minute Data Take Too Long?
Initial Data Load: The initial process of aggregating 30 days of 1-minute data could take some time, but as long as your hardware is reasonable and TimescaleDB is properly set up, it should be manageable. However, if you have millions of devices or sensors, the first refresh could take a while.

Subsequent Updates: After the initial aggregation, subsequent refreshes will be much faster (since it’s only processing new data, not all of the 30 days of data).

Conclusion:
For 30 days of 1-minute data, TimescaleDB should be able to handle it fairly well if you're using continuous aggregates and have proper partitioning and indexing. The initial refresh might take some time depending on your data volume, but once it's set up, subsequent updates will be very fast due to the incremental refresh. However, if you have a huge number of devices or high-frequency data (e.g., thousands of devices generating data every minute), you should monitor performance and possibly tweak the configuration.

Let me know if you need help optimizing for your use case!







You said:
500 sensors
ChatGPT said:
For 500 sensors, each producing data every minute over 30 days, the data volume can grow quite large, but TimescaleDB should still handle it well with the right configuration. Let's break it down:

Estimating the Data Volume:
Each sensor generates 1 row per minute.

For 30 days of data:
1
 row per minute
×
60
 minutes per hour
×
24
 hours per day
×
30
 days
=
43
,
200
 rows per sensor
1 row per minute×60 minutes per hour×24 hours per day×30 days=43,200 rows per sensor.

For 500 sensors:

43
,
200
 rows per sensor
×
500
 sensors
=
21
,
600
,
000
 rows
43,200 rows per sensor×500 sensors=21,600,000 rows
So, you'll have approximately 21.6 million rows of data after 30 days.

TimescaleDB Performance Considerations:
Partitioning: TimescaleDB automatically partitions your data based on time. For a setup with 500 sensors, TimescaleDB will create chunks for each time interval (e.g., daily, hourly) and each sensor's data will be stored in its own partition. This partitioning helps with efficient querying and aggregation.

Continuous Aggregates: Once you create continuous aggregates, they will be incrementally updated every 6 hours (or based on the interval you set). The first time the continuous aggregate policy runs, it will compute over all 30 days of data, which might take a while depending on your hardware. However, this is a one-time computation. After that, subsequent updates will be very fast because only new data (or data that was updated) will be processed.

Indexes: To optimize performance, make sure that your hypertable has appropriate indexes on important columns, such as:

timestamp or time column (for efficient time-based querying).

sensor_id or serial_number (if you're partitioning by device).

TimescaleDB automatically indexes the time column, but you may want to create additional indexes if needed based on your query patterns.

Hardware: The performance will depend on your server's hardware. A system with sufficient RAM, SSD storage, and a fast CPU will handle 21.6 million rows much better than one with limited resources. TimescaleDB is designed to scale well with large datasets, but for very large volumes, more powerful hardware can improve performance.

Optimizing Performance:
TimescaleDB Compression: If you don't need to query historical data very frequently, you could enable compression on older data to save disk space and improve query performance.

For example, data older than 7 days could be compressed to speed up queries and reduce storage.

Indexing: Ensure that the time and sensor_id columns are indexed. TimescaleDB automatically creates indexes on the time column, but indexing the sensor_id (if you're using it for grouping or partitioning) can speed up queries.

Memory and Disk Considerations: With 21.6 million rows over 30 days, this is manageable for most modern servers. Ensure your database has enough RAM for caching and SSD storage for fast access to disk.

Query Tuning: Use EXPLAIN ANALYZE to check how your queries are performing and ensure they are optimized. Consider limiting the time range for queries to only the data you need (e.g., querying only the most recent few days).

Continuous Aggregates and Refreshing:
Initial Refresh: The first time you run the continuous aggregate policy, it will compute aggregates for the full 30 days, which might take some time, especially with a large number of sensors. The time it takes depends on your hardware and the complexity of the aggregation.

Incremental Updates: After the initial aggregation, continuous aggregates will update incrementally every 6 hours. This should be relatively fast, as only the new data will be processed.

Conclusion:
For 500 sensors generating 1-minute data over 30 days, TimescaleDB should be able to handle the resulting 21.6 million rows efficiently, especially if you're using continuous aggregates for pre-aggregation. The first refresh might take some time, but subsequent updates will be much faster. Just ensure your hardware is adequate, and you're following best practices with partitioning, indexing, and query optimization.

Let me know if you need further details or help with any specific setup!







You said:
so if I have 2 years of data I need to create the materialized view with data and then the aggr will keep it updated ?
ChatGPT said:
Yes, exactly! If you have 2 years of data, you’ll need to create the continuous aggregate (materialized view) first, which will initially process and aggregate the entire dataset. After that, the continuous aggregate policy will take over and incrementally refresh the view, ensuring it's always up to date.

Step-by-Step Process for 2 Years of Data:
1. Create the Continuous Aggregate
Before setting up an automatic refresh policy, you need to create a continuous aggregate view based on your raw data.

For example, if you have raw data stored in a hypertable called sensor_data, you can create an hourly aggregate like this:

sql
Copy
Edit
CREATE MATERIALIZED VIEW sensor_data_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    sensor_id,
    avg(value) AS avg_value,
    max(value) AS max_value,
    min(value) AS min_value
FROM sensor_data
GROUP BY bucket, sensor_id;
This will create a continuous aggregate view that precomputes hourly averages, max, and min values for each sensor_id.

The first time you run this, it will compute aggregates for all 2 years of existing data, which might take a while depending on your dataset size.

2. Set Up an Automatic Refresh Policy
Once the continuous aggregate is created, you need to set up an automatic refresh policy so that the aggregate remains updated as new data arrives.

sql
Copy
Edit
SELECT add_continuous_aggregate_policy(
    'sensor_data_hourly',
    start_offset => INTERVAL '2 years',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '6 hours'
);
start_offset => INTERVAL '2 years' → This tells TimescaleDB to maintain data going back 2 years in the materialized view.

end_offset => INTERVAL '1 hour' → This ensures that recent data is aggregated up to 1 hour before the present time (since very recent data might still be changing).

schedule_interval => INTERVAL '6 hours' → This refreshes the aggregate every 6 hours, making sure new data is incorporated efficiently.

How This Works Over Time
Initial Processing: The first time you create the continuous aggregate, it will compute the hourly aggregates for all 2 years of data, which might take a while (depending on your hardware and database size).

Incremental Updates: After the initial aggregation, only new or modified data will be processed during each refresh cycle (every 6 hours). This makes future updates much faster.

Historical Data Retention: The view will always contain the last 2 years of aggregated data, rolling forward as time progresses.

Key Considerations
Initial Processing Time: The first materialization for 2 years might take a long time if you have billions of rows. You might want to test with smaller time ranges first (e.g., 1 month) before scaling up.

Query Performance: Once the continuous aggregate is built, queries on it will be much faster than querying raw data.

Compression: If older raw data isn't needed for frequent queries, you can enable compression in TimescaleDB to save space:

sql
Copy
Edit
ALTER TABLE sensor_data SET (timescaledb.compress);
SELECT add_compression_policy('sensor_data', INTERVAL '6 months');
This will automatically compress raw data older than 6 months, reducing storage and improving query performance.

Final Answer:
Yes! If you have 2 years of raw data, you: ✅ Create the continuous aggregate view (which will process all past data once).
✅ Add an automatic refresh policy to keep the materialized view up to date.
✅ Benefit from fast queries on pre-aggregated data, with only incremental updates happening in the future.

Let me know if you need help with anything specific! 🚀







You said:
if I compress the data how will that affect queries ?
ChatGPT said:
How Compression in TimescaleDB Affects Queries
When you compress data in TimescaleDB, it reduces storage usage and improves query performance for historical data, but it also introduces some trade-offs in terms of query behavior. Let’s break it down:

1️⃣ What Happens When You Compress Data?
TimescaleDB reorganizes the raw data into a highly compressed format, using columnar storage instead of row-based storage.

Compressed data is stored in large, optimized chunks that require less disk space and improve I/O efficiency.

Querying compressed data is generally faster for historical analysis but slower for real-time queries that require frequent updates or row-by-row lookups.

2️⃣ How Queries Are Affected by Compression
✅ Queries That Perform Well on Compressed Data
Compression improves performance when:
✔️ You aggregate over long time ranges (e.g., AVG(), SUM(), COUNT(), MAX(), MIN())
✔️ You query historical data (older than the compression threshold)
✔️ You use column-based filtering (e.g., WHERE time > '2024-01-01' AND device_serial = '123')
✔️ You perform bulk reads (e.g., reports, dashboards, batch processing)

Example: A query like this will be faster on compressed data because it benefits from columnar storage and bulk I/O:

sql
Copy
Edit
SELECT sensor_id, AVG(temperature)
FROM sensor_data
WHERE time BETWEEN '2024-01-01' AND '2024-03-01'
GROUP BY sensor_id;
❌ Queries That Are Slower on Compressed Data
Compression reduces performance when:
❌ You frequently UPDATE or DELETE rows (compressed data is read-only)
❌ You need random access to individual rows
❌ You perform point lookups on a single row (e.g., SELECT * FROM sensor_data WHERE id = 12345;)
❌ You do real-time queries on the most recent data (because compression is applied to older chunks)

Example: A query like this will be slower because it requires extracting a single row from a compressed chunk:

sql
Copy
Edit
SELECT * FROM sensor_data
WHERE sensor_id = '12345' AND time = '2024-01-15 12:00:00';
3️⃣ How to Use Compression Effectively
🔹 Keep Recent Data Uncompressed
Since compression works best for historical data, it's common to keep recent data uncompressed while compressing older data. You can set up a compression policy like this:

sql
Copy
Edit
SELECT add_compression_policy('sensor_data', INTERVAL '6 months');
🔹 This will only compress data older than 6 months, keeping recent data fast and editable.

🔹 Automatically Decompress If Needed
If you need to modify old compressed data, you can decompress it:

sql
Copy
Edit
SELECT decompress_chunk('_timescaledb_internal._hyper_2_50_chunk');
🔹 However, this is not efficient for frequent modifications, so it's best for rare cases.

🔹 Continuous Aggregates Still Work with Compression
If you're using continuous aggregates, they will still work fine even if the raw data is compressed. Since continuous aggregates are incrementally updated, they will only process new (uncompressed) data, so performance remains optimal.

Example:

sql
Copy
Edit
SELECT * FROM sensor_data_hourly WHERE time > now() - INTERVAL '1 year';
✔️ The query will be fast because it uses pre-aggregated data
✔️ The underlying raw data is compressed, but queries to the materialized view remain efficient

4️⃣ Should You Compress Your Data?
✅ Compression is a Good Idea If:
✔️ You have millions or billions of rows
✔️ You primarily read/analyze historical data
✔️ You don’t need to update or delete old records
✔️ You use continuous aggregates for fast queries

❌ Avoid Compression If:
❌ You need to frequently modify or delete historical data
❌ You often query individual rows instead of aggregated data
❌ Your queries are mostly real-time, low-latency lookups

5️⃣ Final Answer:
Compression makes queries faster for historical aggregations and analytics.

Compression makes queries slower if you frequently update, delete, or query individual rows.

Best practice: Keep recent data uncompressed and only compress older data for storage efficiency.

Let me know if you need help setting it up! 🚀
"""

