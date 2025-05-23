import calendar
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import itertools
import json
import logging
import numpy as np
import os
import pandas as pd
from psycopg2.extras import execute_values, execute_batch
from psycopg2 import DatabaseError
import requests
from sqlalchemy import text
import sys
import threading
import time

from unhcr import api_solarman
from unhcr import app_utils
from unhcr import constants as const
from unhcr import db
from unhcr import err_handler

mods=[
    ["api_solarman", "api_solarman"],
    ["app_utils", "app_utils"],
    ["constants", "const"],
    ["db", "db"],
    ["err_handler", "err_handler"],
]

res =app_utils.app_init(mods=mods,  log_file="unhcr.app_nigeria_sm_db_api.log", version= '0.4.8', level="INFO", override=True, quiet=False)
logger = res[0]
# local testing ===================================
if const.LOCAL:  # testing with local python files
    logger, api_solarman, app_utils, const, db, err_handler = res


#!!!!!!!!!!!!!!!!!!!!
db_eng = db.set_local_defaultdb_engine()
#!!!!!!!!!!!!!!!!!!!!
#models.Base.metadata.create_all(db_eng)
# pass


site_id_list =api_solarman.db_all_site_ids(db_eng)
ar = []
res_list = []
for site in site_id_list:
    print(site)
    res, err = api_solarman.api_get_devices(site, deviceType="INVERTER")
    if err:
        logger.error(err)
        exit(9)
    res_list.extend(res)

# Extract device_sn and create DataFrame
df_inverter_api = pd.DataFrame([item["device_sn"] for item in res_list], columns=["device_sn"])
print(df_inverter_api)
pass


df_inverter_db, err = api_solarman.db_get_devices_site_sn_id(db_eng, dev_type="INVERTER") #, site_key="'%'")
if err:
    logger.error(err)
    exit(9)
print(df_inverter_db)
pass

only_in_api = df_inverter_api[~df_inverter_api['device_sn'].isin(df_inverter_db['device_sn'])]
only_in_db = df_inverter_db[~df_inverter_db['device_sn'].isin(df_inverter_api['device_sn'])]

print("Device SNs only in API:")
print(only_in_api)

print("\nDevice SNs only in DB:")
print(only_in_db)

# Optional: combine differences
diff_df = pd.concat([only_in_api.assign(source='API'), only_in_db.assign(source='DB')])
print("\nAll differences:")
print(diff_df)
pass


#!!!!!! create and execute migration TODO: only execute if DB needs upgrade -- generalize schema and env.py & alembic.ini
# res, err = models.create_solarman_migration('Change device history key and remove id', db_eng)
# logger.info('Added created and updated columns to solarman schema tables')
# pass


#!!!!!!!!! STATION DAILY DATA
# # Get current date
# current_date = datetime.today()
# # Number of months to go back
# num_months = 1  # Change as needed
# for i in range(num_months):
#     first_day = current_date.replace(day=1)  # 1st day of the month
#     last_day = first_day.replace(day=calendar.monthrange(first_day.year, first_day.month)[1])  # Last day
#     start_dt = first_day.date().isoformat()
#     end_dt = last_day.date().isoformat()
#     print(f"Start: {start_dt}, End: {end_dt}")

#     # Move back one month
#     current_date = first_day - relativedelta(days=1)  # Go to last day of previous month

#     for site in api_solarman.SITE_ID:
#         key = next(iter(site))
#         print(key, site[key])
#         res = api_solarman.get_station_daily_data(id=site[key], start_date=start_dt, end_date=end_dt, type=2, db_eng=db_eng)
#         pass
# pass
#!!!!!!!!! STATION DAILY DATA

#!!!! initial load of device history TODO: make it general and prod ready
# sites = api_solarman.db_all_site_ids(db_eng)
# for site in sites:
#     res, err = models.db_update_device_history(site, db_eng)
#     pass




#!!!!!! Update Db with INVERTER DATA
# Fetch inverter serial numbers
inverters_sn = api_solarman.db_get_inverter_sns(db_eng)

# Get the initial date from the database
res, err = db.sql_execute(
    """
    WITH x AS (
        SELECT device_sn, MIN(ts) ts 
        FROM solarman.inverter_data 
        GROUP BY 1
    ) 
    SELECT MAX(ts)::date FROM x;
    """,
    db_eng
)

if err:
    logger.error(err)
    exit(9)

start_dt = res[0][0]
num_threads = len(inverters_sn)  # Number of parallel threads (adjust as needed)
#num_threads = 1  # For testing, set to 1 to avoid parallel processing

# Split inverter list into chunks for parallel processing
chunks = [list(chunk) for chunk in np.array_split(inverters_sn, num_threads)]  

# Thread-safe counters
  # Tracks processed chunks
  # Tracks processed inverters within chunks
counter_lock = threading.Lock()
api_lock = threading.Lock()
def process_chunk(chunk, date, engine):
    """Fetch inverter data for a chunk of inverters in parallel."""
    with counter_lock:
        chunk_count = next(global_counter)  # Thread-safe chunk count
    chunk_size = len(chunk)
    print(f"Processing chunk #{chunk_count} on {date} with {chunk_size} inverters")

    results = []
    item_lock = threading.Lock()
    item_counter = itertools.count(0)
    for sn in chunk:
        with item_lock:
            item_count = next(item_counter)  # Thread-safe item count

        #print(f"Processing chunk #{chunk_count} | SN {sn} | Item #{item_count}")


        start_time = time.time()
        # this blocks till the API call returns. The solarman API server seems to have changed, it used to be async
        # I used to be able to calls this in parallel, but now it slows down.
        with api_lock:
            res, err = api_solarman.get_inverter_data(sn=sn, start_date=date, type=1)
        if err:
            logger.error(f"Error fetching data for SN {sn}: {err}")
        else:
            res, err = err_handler.error_wrapper(lambda: api_solarman.insert_inverter_data(db_eng, res))
            if err:
                logger.error(f"Error fetching data for SN {sn}: {err}")
        end_time = time.time()
        print(f"SN: {sn} | Execution time: {((end_time - start_time)):.2f} secs")

        results.append((sn, res, err))

    return results

# **MAIN PROCESS LOOP**
###!!! if getting new data   start_dt = datetime.today().date()
start_dt = datetime.strptime('2025-03-22', "%Y-%m-%d").date()
start_dt = datetime.today().date() + timedelta(days=1)
x = 3
timing = time.time()
while x > 0:
    global_counter = itertools.count(1)

    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(
            executor.map(lambda chunk: process_chunk(chunk, start_dt, db_eng), chunks)
        )

    print(f"Completed processing for {start_dt}. Moving to next day.")
    # Compute elapsed time
    et = time.time()
    elapsed = et-timing
    timing = et
    print(f"Elapsed time: {elapsed:.2f} seconds")
    x -= 1
    start_dt -= timedelta(days=1)
    if x <= 0 or start_dt < datetime.strptime('2024-10-01', "%Y-%m-%d").date():
        break
    #time.sleep(1)

# Flatten results
final_output = [item for sublist in results for item in sublist]
pass












inverters_sn = api_solarman.db_get_inverter_sns(db_eng)

start_dt = datetime.today().date()
res, err = db.sql_execute(f"with x as (select device_sn, min(ts) ts FROM solarman.inverter_data group by 1)select max(ts)::date from x;", db_eng)
if err:
    logger.error(err)
    exit(9)
start_dt = res[0][0]
while 1 == 1:
    for sn in inverters_sn:
        print("SN: ", sn, start_dt)
        start_time = time.time()
        res, err = api_solarman.get_inverter_data(sn=sn, start_date=start_dt, type=1, db_eng=db_eng)
        end_time = time.time()
        print(f'Execution time: {((end_time - start_time)/60):.2f} minutes')
        pass
    start_dt = start_dt - timedelta(days=1)
pass
# api_solarman.insert_inverter_data(data, db_eng=None)
# pass




#!!! this came from Istvans excel 2024-02-27 "New Top 20 - Copy.xlsx" 
# Actually the tables that I could get data for (add eyedro.gb_ for AZURE defaultdb table name)
# Here is the list of errors:
"""
    2025-02-27 10:14:12,349 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 00980829 ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:14:28,330 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 0098084C ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:14:46,387 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 0098085D ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:14:48,485 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 0098086A ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:14:48,798 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 0098086C ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:14:49,104 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 0098086D ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:15:14,057 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 009808DF ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:15:24,188 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 00980953 ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:15:35,050 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 009809B7 ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:16:13,562 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 00980A03 ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:16:45,553 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 00980A9A ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:16:55,433 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 00980AAF ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:16:55,763 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 00980ABA ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:17:07,954 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 00980B12 ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:17:20,336 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 00980B2E ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
2025-02-27 10:18:23,851 - ERROR - 2025-02-27T00:00:00+00:00 ZZZ 00980B93 ERRORS: [[1210, 'API Error. Contact Eyedro Admin.'], [899, 'DeviceSerial invalid, or device owner has not claimed device.']]
"""

genset_gbs_org = ['00980864',	'0098086a',	'0098086c',	'0098086d',	'009807c4',	'009807d3',	'009807d8',	'00980827',	'00980828',	'00980829',	'0098082e',	'0098082f',	'0098082d',	'00980830',	'00980848',	'00980849',	'0098084c',	'0098084f',	'00980857',	'0098085c',	'0098085b',	'0098085d',	'00980aa1',	'00980890',	'0098087b',	'00980891',	'0098087c',	'009808c7',	'009808b5',	'009808c6',	'00980893',	'00980892',	'009808e8',	'009808dc',	'009808df',	'009808db',	'00980907',	'00980914',	'00980953',	'009808f8',	'009808f1',	'009809ad',	'00980954',	'009809b5',	'009809b7',	'009809c3',	'009809c4',	'009809e5',	'009809b8',	'009809ea',	'009809f3',	'009809f4',	'009809e6',	'009809e9',	'009809fd',	'00980a02',	'00980a03',	'00980a01',	'00980a06',	'00980a07',	'00980a14',	'00980a05',	'00980a17',	'00980a27',	'00980a20',	'00980a21',	'00980a24',	'00980a50',	'00980a40',	'00980a52',	'00980a74',	'00980aa0',	'00980a9a',	'00980aa2',	'00980a53',	'00980aa5',	'00980abb',	'00980aaf',	'00980aba',	'00980ac9',	'00980acb',	'00980b11',	'00980af4',	'00980b12',	'00980b1c',	'00980b1e',	'00980b2e',	'00980b2a',	'00980b29',	'00980b2f',	'00980b36',	'00980b37',	'00980b58',	'00980b35',	'00980b6b',	'00980b5e',	'00980b6e',	'00980b6c',	'00980b74',	'00980b76',	'00980b6f',	'00980b75',	'00980b80',	'00980b81',	'00980b77',	'00980b89',	'00980b8a',	'00980b8b',	'00980b87',	'00980b91',	'00980b96',	'00980b93',	'00980b90',	'00980b8c',	'00980da4',	'00980b97',	'00980da6',	'00980dbb',	'00980dd3',	'00980da7',	'00980dcb',	'00980dcd',	'00980dd6',	'00980de3',	'00980dd4',	'00980de8',	'00980e05',	'00980dfc',	'00980e09',	'00980e08',	'00980e10',	'00980e0e',	'00980e13',	'00980e1f',	'00980e22',	'00980e27',	'b120045e',	'00980e29',	'00980e28',	'b120045f',	'00980e2a',	'b1200464',	'b1200465',	'b1200631',	'b1200461',	'009809f5',	'00980a2a',	'00980a29',	'00980b13',	'00980de5',	'00980b6a',	'00980b98',	'00980b84',	'009808c8',	'00980ddb',	'009809cb',	'00980e1c',	'00980b33',	'0098097b',	'00980850',	'00980889',	'00980826',	'00980845',	'0098084e',	'00980a19',	'00980aa3',	'00980ac1',	'00980b1a',	'00980b34',	'00980b70',	'00980b7a',	'00980b86',	'00980da0',	'00980dd5',	'00980df1',	'00980e18',	'b1200462']

genset_gbs = [
'009807c4',
'009807d3',
'009807d8',
'00980826',
'00980827',
'00980828',
'00980829',
'0098082d',
'0098082e',
'0098082f',
'00980830',
'00980845',
'00980848',
'00980849',
'0098084c',
'0098084e',
'0098084f',
'00980850',
'00980857',
'0098085b',
'0098085c',
'0098085d',
'00980864',
'0098086a',
'0098086c',
'0098086d',
'0098087b',
'0098087c',
'00980889',
'00980890',
'00980891',
'00980892',
'00980893',
'009808c6',
'009808c7',
'009808c8',
'009808b5',
'009808db',
'009808dc',
'009808df',
'009808e8',
'009808f1',
'009808f8',
'00980907',
'00980914',
'00980953',
'00980954',
'0098097b',
'009809ad',
'009809b5',
'009809b7',
'009809b8',
'009809c3',
'009809c4',
'009809cb',
'009809e5',
'009809e6',
'009809e9',
'009809ea',
'009809f3',
'009809f4',
'009809f5',
'009809fd',
'00980a01',
'00980a02',
'00980a03',
'00980a05',
'00980a06',
'00980a07',
'00980a14',
'00980a17',
'00980a19',
'00980a20',
'00980a21',
'00980a24',
'00980a27',
'00980a29',
'00980a2a',
'00980a40',
'00980a50',
'00980a52',
'00980a53',
'00980a74',
'00980a9a',
'00980aa0',
'00980aa1',
'00980aa2',
'00980aa3',
'00980aa5',
'00980abb',
'00980aba',
'00980aaf',
'00980ac1',
'00980ac9',
'00980acb',
'00980af4',
'00980b11',
'00980b12',
'00980b13',
'00980b1a',
'00980b1c',
'00980b1e',
'00980b29',
'00980b2a',
'00980b2e',
'00980b2f',
'00980b33',
'00980b34',
'00980b35',
'00980b36',
'00980b37',
'00980b58',
'00980b5e',
'00980b6a',
'00980b6b',
'00980b6c',
'00980b6d',
'00980b6e',
'00980b6f',
'00980b70',
'00980b74',
'00980b75',
'00980b76',
'00980b77',
'00980b7a',
'00980b80',
'00980b81',
'00980b84',
'00980b86',
'00980b87',
'00980b89',
'00980b8a',
'00980b8b',
'00980b8c',
'00980b90',
'00980b91',
'00980b93',
'00980b96',
'00980b97',
'00980b98',
'00980da0',
'00980da4',
'00980da6',
'00980da7',
'00980dbb',
'00980dcb',
'00980dcd',
'00980dd3',
'00980dd4',
'00980dd5',
'00980dd6',
'00980de3',
'00980de5',
'00980de8',
'00980df1',
'00980dfc',
'00980e05',
'00980e08',
'00980e09',
'00980e0e',
'00980e10',
'00980e13',
'00980e18',
'00980e1c',
'00980e1f',
'00980e22',
'00980e27',
'00980e28',
'00980e29',
'00980e2a',
'b120045e',
'b120045f',
'b1200461',
'b1200462',
'b1200464',
'b1200465',
'b1200631'
]


engines = db.set_db_engines()

def phase_imbalance(engine, gbs):
    path = r'E:\UNHCR\OneDrive - UNHCR\Energy Team\Concept development\AZURE DATA\phase_imbalance\gensets'
    dt = datetime.now().date().isoformat()

    try:
        conn = engine.raw_connection()  # Get raw psycopg2 connection
        with conn.cursor() as cur:
            for serial in gbs:
                try:
                    fn = f'{path}\\{serial.upper()}_phase_imbalance_{dt}.csv'
                    sql = f"""
                    WITH wh AS (
                        SELECT 
                            DATE_TRUNC('hour', ts) AS ts_hour,
                            DATE(ts) as dt,
                            EXTRACT(HOUR from ts) as hr,
                            ROUND(SUM(wh_p1)::NUMERIC, 1) AS wh_p1, 
                            ROUND(SUM(wh_p2)::NUMERIC, 1) AS wh_p2, 
                            ROUND(SUM(wh_p3)::NUMERIC, 1) AS wh_p3
                        FROM eyedro.gb_{serial.upper()}
                        GROUP BY 1,2,3
                    )
                    SELECT 
                        '{serial.lower()}' serial,
                        ts_hour, 
                        dt,
                        hr, 
                        ROUND(((GREATEST(wh_p1, wh_p2, wh_p3) - LEAST(wh_p1, wh_p2, wh_p3)) / NULLIF(GREATEST(wh_p1, wh_p2, wh_p3), 0)), 1) AS phase_imbalance,
                        wh_p1, 
                        wh_p2, 
                        wh_p3
                    FROM wh
                    WHERE wh_p1 + wh_p2 + wh_p3 != 0
                    ORDER BY 2
                    limit 500; -- this is a bit more than 14 days
                    """

                    cur.execute(sql)
                    res = cur.fetchall()

                    # Convert to DataFrame only if there is data
                    if res:
                        df_result = pd.DataFrame(res, columns=[desc[0] for desc in cur.description])
                        df_result.to_csv(fn, index=False)  # ✅ Save to CSV
                        logger.info(f"Saved CSV for {serial}: {fn}")
                    else:
                        logger.info(f"No data for {serial}, skipping CSV creation.")

                except DatabaseError as e:
                    logger.error(f"Database error for {serial}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error for {serial}: {e}")

    except DatabaseError as e:
        logger.error(f"Database connection error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if conn:
            conn.close()  # ✅ Always close the connection

# phase_imbalance(engines[1], genset_gbs)
# pass

def create_tables(serials, engine=engines[1]):
    conn = engine.raw_connection()  # Get raw psycopg2 connection
    try:
        with conn.cursor() as cur:
            for serial in serials:
                sql = f"""
        --DROP TABLE IF EXISTS eyedro.gb_{serial} CASCADE;
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

    DROP MATERIALIZED VIEW if exists eyedro.gb_{serial}_daily cascade;
    CREATE MATERIALIZED VIEW if not exists eyedro.gb_{serial}_daily
    WITH (timescaledb.continuous) AS
    SELECT 
        time_bucket('1 day', ts) AS day,
    -- Aggregate power (average, min, max)
    AVG(a_p1) AS avg_amps_p1, 
    MIN(a_p1) AS min_amps_p1, 
    MAX(a_p1) AS max_amps_p1,
    AVG(a_p2) AS avg_amps_p2, 
    MIN(a_p2) AS min_amps_p2, 
    MAX(a_p2) AS max_amps_p2,
    AVG(a_p3) AS avg_amps_p3, 
    MIN(a_p3) AS min_amps_p3, 
    MAX(a_p3) AS max_amps_p3,
    -- Aggregate voltage (average, min, max)
    AVG(v_p1) AS avg_voltage_p1, 
    MIN(v_p1) AS min_voltage_p1, 
    MAX(v_p1) AS max_voltage_p1,
    AVG(v_p2) AS avg_voltage_p2, 
    MIN(v_p2) AS min_voltage_p2, 
    MAX(v_p2) AS max_voltage_p2,
    AVG(v_p3) AS avg_voltage_p3, 
    MIN(v_p3) AS min_voltage_p3, 
    MAX(v_p3) AS max_voltage_p3,
    -- Aggregate power factor (weighted average, min, max)
    -- Otherwise, periods with low power consumption will affect the true weighted average.
    SUM(pf_p1 * wh_p1) / NULLIF(SUM(wh_p1), 0) AS avg_pf_p1,
    MIN(pf_p1) AS min_pf_p1, 
    MAX(pf_p1) AS max_pf_p1,
    SUM(pf_p2 * wh_p2) / NULLIF(SUM(wh_p2), 0) AS avg_pf_p2,
    MIN(pf_p2) AS min_pf_p2, 
    MAX(pf_p2) AS max_pf_p2,
    SUM(pf_p3 * wh_p3) / NULLIF(SUM(wh_p3), 0) AS avg_pf_p3,
    MIN(pf_p3) AS min_pf_p3, 
    MAX(pf_p3) AS max_pf_p3,
    -- Aggregate energy (Wh) with SUM (energy accumulates)
    SUM(wh_p1) AS total_wh_p1, 
    SUM(wh_p2) AS total_wh_p2, 
    SUM(wh_p3) AS total_wh_p3

    FROM eyedro.gb_{serial}
    GROUP BY day
    WITH NO DATA;

    SELECT add_continuous_aggregate_policy(
        'eyedro.gb_{serial}_daily',
        start_offset => INTERVAL '7 days',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day'
    );
    
    CREATE MATERIALIZED VIEW if not exists eyedro.gb_{serial}_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1h', ts) AS ts_hour,
    -- Aggregate power (average, min, max)
    AVG(a_p1) AS avg_amps_p1, 
    MIN(a_p1) AS min_amps_p1, 
    MAX(a_p1) AS max_amps_p1,
    AVG(a_p2) AS avg_amps_p2, 
    MIN(a_p2) AS min_amps_p2, 
    MAX(a_p2) AS max_amps_p2,
    AVG(a_p3) AS avg_amps_p3, 
    MIN(a_p3) AS min_amps_p3, 
    MAX(a_p3) AS max_amps_p3,
    -- Aggregate voltage (average, min, max)
    AVG(v_p1) AS avg_voltage_p1, 
    MIN(v_p1) AS min_voltage_p1, 
    MAX(v_p1) AS max_voltage_p1,
    AVG(v_p2) AS avg_voltage_p2, 
    MIN(v_p2) AS min_voltage_p2, 
    MAX(v_p2) AS max_voltage_p2,
    AVG(v_p3) AS avg_voltage_p3, 
    MIN(v_p3) AS min_voltage_p3, 
    MAX(v_p3) AS max_voltage_p3,
    -- Aggregate power factor (weighted average, min, max)
    -- Otherwise, periods with low power consumption will affect the true weighted average.
    SUM(pf_p1 * wh_p1) / NULLIF(SUM(wh_p1), 0) AS avg_pf_p1,
    MIN(pf_p1) AS min_pf_p1, 
    MAX(pf_p1) AS max_pf_p1,
    SUM(pf_p2 * wh_p2) / NULLIF(SUM(wh_p2), 0) AS avg_pf_p2,
    MIN(pf_p2) AS min_pf_p2, 
    MAX(pf_p2) AS max_pf_p2,
    SUM(pf_p3 * wh_p3) / NULLIF(SUM(wh_p3), 0) AS avg_pf_p3,
    MIN(pf_p3) AS min_pf_p3, 
    MAX(pf_p3) AS max_pf_p3,
    -- Aggregate energy (Wh) with SUM (energy accumulates)
    SUM(wh_p1) AS total_wh_p1, 
    SUM(wh_p2) AS total_wh_p2, 
    SUM(wh_p3) AS total_wh_p3
FROM eyedro.gb_{serial}
GROUP BY 1
WITH NO DATA;

-- Apply the policy to refresh the last 7 days every 1 day
SELECT add_continuous_aggregate_policy('eyedro.gb_{serial}_hourly',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

    """
                cur.execute(sql)
                conn.commit()  # ✅ Commit only if successful
                print('XXXXXX', serial)
    except DatabaseError as e:
        conn.rollback()  # Rollback on failure
        logger.error(f"Database error during table creation: {e}")
    except Exception as e:
        conn.rollback()  # Rollback on any other failure
        logger.error(f"Unexpected error: {e}")
    finally:
        conn.close()  # ✅ Always close the connection

genset_gbs = ['00980b6d']
#create_tables(genset_gbs)
#pass


def meter_response(serial, epoch):
    try:
        EYEDRO_KEY_GET_DATA_EMPTY = const.GB_API_V1_EMPTY_KEY
        print("EyeDro Endpoint and Key Set", serial, epoch)
        meter_url = "https://api.eyedro.com/Unhcr/DeviceData?DeviceSerial=" + str(serial) + "&DateStartSecUtc=" + str(epoch) + f"&DateNumSteps=1440&UserKey={EYEDRO_KEY_GET_DATA_EMPTY}"
        response = requests.get(meter_url, timeout=600)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return json.loads(response.text)
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"meter_response:ZZZ {serial} {epoch} HTTP error occurred ERROR: {http_err}")
        return None
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"meter_response: ZZZ {serial} {epoch} Connection error occurred ERROR: {conn_err}")
        return None
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"meter_response: ZZZ {serial} {epoch} Timeout error occurred ERROR: {timeout_err}")
        return None
    except json.JSONDecodeError as json_err:
        logger.error(f"meter_response: ZZZ {serial} {epoch} JSON decoding error occurred ERROR: {json_err}")
        return None
    except Exception as e:
        logger.error(f"meter_response: ZZZ {serial} {epoch} An unexpected error occurred: {e}")
        return None

# x = meter_response("00980AA1", 1725147900)
# print(x)


def update_gb_db(serial, data, engine):
    # Rename "A" to "a_p1", "a_p2", "a_p3"
    parameter_mapping = {"A": "a_p", "V": "v_p", "PF": "pf_p", "Wh": "wh_p"}

    # Extract device data
    device_data = data["DeviceData"]
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
    df = df.fillna(0)
    # !!! no need for this, but might use it later
    # # Ensure missing columns (V & PF) exist with NaN
    # expected_columns = ["ts"] + [f"{p}{i}" for p in ["a_p", "v_p", "pf_p", "wh_p"] for i in range(1, 4)]
    # for col in expected_columns:
    #     if col not in df:
    #         df[col] = None  # Or df[col] = 0 for default values

    # Sort by timestamp
    df = df.sort_values("ts").reset_index(drop=True)

    columns_str = ", ".join(df.columns)
    # Use ON CONFLICT for UPSERT (Insert or Update)
    upsert_sql = f"""
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
            wh_p3 = EXCLUDED.wh_p3
            RETURNING (xmax = 0) AS inserted;
    """

    conn = engine.raw_connection()  # Get raw psycopg2 connection
    try:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, df.to_records(index=False).tolist())
            res = cur.fetchall() # Commit after execution
            conn.commit()
            inserted_count = sum(row[0] for row in res)
            updated_count = len(res) - inserted_count
    except DatabaseError as e:
        conn.rollback()  # Rollback on failure
        logger.error(f"ZZZ {serial} update_gb_db Database error during UPSERT: {e}")
        return None, e
    except Exception as e:
        conn.rollback()  # Rollback on any other failure
        logger.error(f"ZZZ {serial} update_gb_db Unexpected error: {e}")
        return None, e
    finally:
        conn.close()

    logger.info(f"Inserted: {inserted_count}, Updated: {updated_count} ✅")
    return [inserted_count, updated_count], None

serials_all = ['009807C4',	'009807D3',	'009807D8',	'00980826',	'00980827',	'00980828',	'00980829',	'0098082D',	'0098082E',	'0098082F',	'00980830',	'00980845',	'00980848',	'00980849',	'0098084C',	'0098084E',	'0098084F',	'00980850',	'00980857',	'0098085B',	'0098085C',	'0098085D',	'00980864',	'0098086A',	'0098086C',	'0098086D',	'0098087B',	'0098087C',	'00980889',	'00980890',	'00980891',	'00980892',	'00980893',	'009808B5',	'009808C6',	'009808C7',	'009808C8',	'009808DB',	'009808DC',	'009808DF',	'009808E8',	'009808F1',	'009808F8',	'00980907',	'00980914',	'00980953',	'00980954',	'0098097B',	'009809AD',	'009809B5',	'009809B7',	'009809B8',	'009809C3',	'009809C4',	'009809CB',	'009809E5',	'009809E6',	'009809E9',	'009809EA',	'009809F3',	'009809F4',	'009809F5',	'009809FD',	'00980A01',	'00980A02',	'00980A03',	'00980A05',	'00980A06',	'00980A07',	'00980A14',	'00980A17',	'00980A19',	'00980A20',	'00980A21',	'00980A24',	'00980A27',	'00980A29',	'00980A2A',	'00980A40',	'00980A50',	'00980A52',	'00980A53',	'00980A74',	'00980A9A',	'00980AA0',	'00980AA2',	'00980AA3',	'00980AA5',	'00980AAF',	'00980ABA',	'00980ABB',	'00980AC1',	'00980AC9',	'00980ACB',	'00980AF4',	'00980B11',	'00980B12',	'00980B13',	'00980B1A',	'00980B1C',	'00980B1E',	'00980B29',	'00980B2A',	'00980B2E',	'00980B2F',	'00980B33',	'00980B34',	'00980B35',	'00980B36',	'00980B37',	'00980B58',	'00980B5E',	'00980B6A',	'00980B6B',	'00980B6C',	'00980B6E',	'00980B6F',	'00980B70',	'00980B74',	'00980B75',	'00980B76',	'00980B77',	'00980B7A',	'00980B80',	'00980B81',	'00980B84',	'00980B86',	'00980B87',	'00980B89',	'00980B8A',	'00980B8B',	'00980B8C',	'00980B90',	'00980B91',	'00980B93',	'00980B96',	'00980B97',	'00980B98',	'00980DA0',	'00980DA4',	'00980DA6',	'00980DA7',	'00980DBB',	'00980DCB',	'00980DCD',	'00980DD3',	'00980DD4',	'00980DD5',	'00980DD6',	'00980DDB',	'00980DE3',	'00980DE5',	'00980DE8',	'00980DF1',	'00980DFC',	'00980E05',	'00980E08',	'00980E09',	'00980E0E',	'00980E10',	'00980E13',	'00980E18',	'00980E1C',	'00980E1F',	'00980E22',	'00980E27',	'00980E28',	'00980E29',	'00980E2A',	'B120045E',	'B120045F',	'B1200461',	'B1200462',	'B1200464',	'B1200465',	'B1200631']

complete = []
if not os.path.exists('complete.txt'):
    open('complete.txt', "w").close()
if not os.path.exists('complete_app.txt'):
    open('complete_app.txt', "w").close()

with open("complete.txt", "r") as f:
    content = f.read().strip()
    complete = list(map(str, content.split(","))) if content else []

if not complete: 
    complete = ['00980AA1','009807C4','00980892','00980AF4','00980953']

serials =  list(set(genset_gbs) - set(complete) - set(['00980864',	'0098086a',	'0098086c',	'0098086d',	'009807c4',	'009807d3',	'009807d8',	'00980827',	'00980828',	'00980829',	'0098082e',	'0098082f',	'0098082d',	'00980830',	'00980848',	'00980849',	'0098084c',	'0098084f',	'00980857',	'0098085c',	'0098085b',	'0098085d',	'00980aa1',	'00980890',	'0098087b',	'00980891',	'0098087c',	'009808c7',	'009808b5',	'009808c6',	'00980893',	'00980892',	'009808e8',	'009808dc',	'009808df',	'009808db',	'00980907',	'00980914',	'00980953',	'009808f8',	'009808f1']))

xx = 0
for serial in np.sort(genset_gbs):
    serial = serial.upper()
    xx += 1
    # Get midnight timestamp (00:00:00) for today in UTC
    midnight_today_utc = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    epoch_midnight_today = int(midnight_today_utc.timestamp())
    epoch, err = db.get_gb_epoch(serial, engines[1], max=False)
    if epoch:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc) + timedelta(days=1) # Convert epoch to datetime
        midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)  # Set time to midnight
        epoch = int(midnight.timestamp()) 
    if not epoch:
        epoch = epoch_midnight_today
    no_data_cnt = 0
    while epoch > 1704067199:   # Dec 31 2023
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
        data = meter_response(serial, epoch)
        if data is None:
            no_data_cnt += 1
            logger.error(f"{dt} ZZZ {serial} ERROR: NO DATA: {no_data_cnt}")
            complete.append(serial)
            break
        if data['Errors']:
            logger.error(f"{dt} ZZZ {serial} ERRORS: {data['Errors']}")
            if any('API' in str(item) for item in data['Errors'][0]):
                complete.append(serial)
                break
            elif any('DeviceSerial invalid' in str(item) for item in data['Errors'][1]):
                complete.append(serial)
                # with open("complete.txt", "w") as f:
                #     f.write(",".join(map(str, complete)))
                break
            elif len(data['Errors']) > 2 and any('Invalid DateStartSecUtc' in str(item) for item in data['Errors'][2]):
                logger.info('Invalid DateStartSecUtc NORMAL END OF DATA')
                complete.append(serial)
                with open("complete.txt", "w") as f:
                    f.write(",".join(map(str, complete)))
                break
            else:
                logger.error(f"{dt} ZZZ {serial} ERRORS: uncaught error")
                complete.append(serial)
                break

        res, err = update_gb_db(serial.lower(), data, engines[1])
        if err:
            logger.error(f"ZZZ {serial} gb_1min update_gb_db ERROR: {err}")
            break

        logger.info(f"gb_1min GB: {xx} {serial} Date: {dt} Rows: {res}  {epoch}")
        # previous day
        epoch -= 86400
with open("complete_app.txt", "w") as f:
    f.write(",".join(map(str, complete)))
pass

"""


# Get list of tables in schema
cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'eyedro'")
tables = [row[0] for row in cur.fetchall()]

# Build the query
query_parts = []
for table in tables:
    query_parts.append(f"(SELECT ts, epoch_secs, '{table}' AS source_table, * FROM {table})")

query = " FULL OUTER JOIN ".join(query_parts)

final_query = f"SELECT * FROM {query};"
print(final_query)  # Print the generated query

# Execute the query
cur.execute(final_query)
results = cur.fetchall()

# Print first few rows
for row in results[:5]:
    print(row)

# Close connection
cur.close()
conn.close()

    
    
"""
