
from datetime import UTC, datetime, timedelta, timezone
import json
import logging
import os
from numpy import sort
import pandas as pd
from psycopg2.extras import execute_values, execute_batch
from psycopg2 import DatabaseError
import requests
from sqlalchemy import text
import sys

from unhcr import constants as const
from unhcr import db
from unhcr import utils

# local testing ===================================
if const.LOCAL:  # testing with local python files
    const, db, utils, *rest = const.import_local_libs(
        mods=[
            ["constants", "const"],
            ["db", "db"],
            ["utils", "utils"],
        ]
    )

utils.log_setup(level="INFO", log_file="unhcr.gb_1min.log", override=True)
logging.info(
    f"{sys.argv[0]} Process ID: {os.getpid()}   Log Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}"
)

if not utils.is_version_greater_or_equal('0.4.7'):
    logging.error(
        "This version of the script requires at least version 0.4.6 of the unhcr module."
    )
    exit(47)

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

genset_gbs = ['00980864',	'0098086a',	'0098086c',	'0098086d',	'009807c4',	'009807d3',	'009807d8',	'00980827',	'00980828',	'00980829',	'0098082e',	'0098082f',	'0098082d',	'00980830',	'00980848',	'00980849',	'0098084c',	'0098084f',	'00980857',	'0098085c',	'0098085b',	'0098085d',	'00980aa1',	'00980890',	'0098087b',	'00980891',	'0098087c',	'009808c7',	'009808b5',	'009808c6',	'00980893',	'00980892',	'009808e8',	'009808dc',	'009808df',	'009808db',	'00980907',	'00980914',	'00980953',	'009808f8',	'009808f1',	'009809ad',	'00980954',	'009809b5',	'009809b7',	'009809c3',	'009809c4',	'009809e5',	'009809b8',	'009809ea',	'009809f3',	'009809f4',	'009809e6',	'009809e9',	'009809fd',	'00980a02',	'00980a03',	'00980a01',	'00980a06',	'00980a07',	'00980a14',	'00980a05',	'00980a17',	'00980a27',	'00980a20',	'00980a21',	'00980a24',	'00980a50',	'00980a40',	'00980a52',	'00980a74',	'00980aa0',	'00980a9a',	'00980aa2',	'00980a53',	'00980aa5',	'00980abb',	'00980aaf',	'00980aba',	'00980ac9',	'00980acb',	'00980b11',	'00980af4',	'00980b12',	'00980b1c',	'00980b1e',	'00980b2e',	'00980b2a',	'00980b29',	'00980b2f',	'00980b36',	'00980b37',	'00980b58',	'00980b35',	'00980b6b',	'00980b5e',	'00980b6e',	'00980b6c',	'00980b74',	'00980b76',	'00980b6f',	'00980b75',	'00980b80',	'00980b81',	'00980b77',	'00980b89',	'00980b8a',	'00980b8b',	'00980b87',	'00980b91',	'00980b96',	'00980b93',	'00980b90',	'00980b8c',	'00980da4',	'00980b97',	'00980da6',	'00980dbb',	'00980dd3',	'00980da7',	'00980dcb',	'00980dcd',	'00980dd6',	'00980de3',	'00980dd4',	'00980de8',	'00980e05',	'00980dfc',	'00980e09',	'00980e08',	'00980e10',	'00980e0e',	'00980e13',	'00980e1f',	'00980e22',	'00980e27',	'b120045e',	'00980e29',	'00980e28',	'b120045f',	'00980e2a',	'b1200464',	'b1200465',	'b1200631',	'b1200461',	'009809f5',	'00980a2a',	'00980a29',	'00980b13',	'00980de5',	'00980b6a',	'00980b98',	'00980b84',	'009808c8',	'00980ddb',	'009809cb',	'00980e1c',	'00980b33',	'0098097b',	'00980850',	'00980889',	'00980826',	'00980845',	'0098084e',	'00980a19',	'00980aa3',	'00980ac1',	'00980b1a',	'00980b34',	'00980b70',	'00980b7a',	'00980b86',	'00980da0',	'00980dd5',	'00980df1',	'00980e18',	'b1200462']

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
                        logging.info(f"Saved CSV for {serial}: {fn}")
                    else:
                        logging.info(f"No data for {serial}, skipping CSV creation.")

                except DatabaseError as e:
                    logging.error(f"Database error for {serial}: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error for {serial}: {e}")

    except DatabaseError as e:
        logging.error(f"Database connection error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if conn:
            conn.close()  # ✅ Always close the connection

# phase_imbalance(engines[1], genset_gbs)
# pass

def create_tables(serial, engine=engines[1]):
    conn = engine.raw_connection()  # Get raw psycopg2 connection
    try:
        with conn.cursor() as cur:
            for serial in serials:
                sql = f"""
        DROP TABLE IF EXISTS eyedro.gb_{serial} CASCADE;
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

    CREATE MATERIALIZED VIEW eyedro.gb_{serial}_daily
    WITH (timescaledb.continuous) AS
    SELECT 
        time_bucket('1 day', ts) AS day,
        AVG(a_p1) AS avg_a_p1,
        AVG(a_p2) AS avg_a_p2,
        AVG(a_p3) AS avg_a_p3,
        AVG(v_p1) AS avg_v_p1,
        AVG(v_p2) AS avg_v_p2,
        AVG(v_p3) AS avg_v_p3,
        AVG(pf_p1) AS avg_pf_p1,
        AVG(pf_p2) AS avg_pf_p2,
        AVG(pf_p3) AS avg_pf_p3,
        SUM(wh_p1) AS sum_wh_p1,
        SUM(wh_p2) AS sum_wh_p2,
        SUM(wh_p3) AS sum_wh_p3
    FROM eyedro.gb_{serial}
    GROUP BY day
    WITH NO DATA;

    SELECT add_continuous_aggregate_policy(
        'eyedro.gb_{serial}_daily',
        start_offset => INTERVAL '7 days',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day'
    );
    """
                cur.execute(sql)
                conn.commit()  # ✅ Commit only if successful
                print('XXXXXX', serial)
    except DatabaseError as e:
        conn.rollback()  # Rollback on failure
        logging.error(f"Database error during table creation: {e}")
    except Exception as e:
        conn.rollback()  # Rollback on any other failure
        logging.error(f"Unexpected error: {e}")
    finally:
        conn.close()  # ✅ Always close the connection





def meter_response(serial, epoch):
    try:
        EYEDRO_KEY_GET_DATA_EMPTY = "URcLQ4MNDKgCPOacW8PB4jbTxBdEXvk3sajrD7SU"
        print("EyeDro Endpoint and Key Set", serial, epoch)
        meter_url = "https://api.eyedro.com/Unhcr/DeviceData?DeviceSerial=" + str(serial) + "&DateStartSecUtc=" + str(epoch) + f"&DateNumSteps=1440&UserKey={EYEDRO_KEY_GET_DATA_EMPTY}"
        response = requests.get(meter_url, timeout=600)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return json.loads(response.text)
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"meter_response:ZZZ {serial} {epoch} HTTP error occurred ERROR: {http_err}")
        return None
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"meter_response: ZZZ {serial} {epoch} Connection error occurred ERROR: {conn_err}")
        return None
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"meter_response: ZZZ {serial} {epoch} Timeout error occurred ERROR: {timeout_err}")
        return None
    except json.JSONDecodeError as json_err:
        logging.error(f"meter_response: ZZZ {serial} {epoch} JSON decoding error occurred ERROR: {json_err}")
        return None
    except Exception as e:
        logging.error(f"meter_response: ZZZ {serial} {epoch} An unexpected error occurred: {e}")
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
        logging.error(f"ZZZ {serial} update_gb_db Database error during UPSERT: {e}")
        return None, e
    except Exception as e:
        conn.rollback()  # Rollback on any other failure
        logging.error(f"ZZZ {serial} update_gb_db Unexpected error: {e}")
        return None, e
    finally:
        conn.close()

    logging.info(f"Inserted: {inserted_count}, Updated: {updated_count} ✅")
    return [inserted_count, updated_count], None

serials_all = ['009807C4',	'009807D3',	'009807D8',	'00980826',	'00980827',	'00980828',	'00980829',	'0098082D',	'0098082E',	'0098082F',	'00980830',	'00980845',	'00980848',	'00980849',	'0098084C',	'0098084E',	'0098084F',	'00980850',	'00980857',	'0098085B',	'0098085C',	'0098085D',	'00980864',	'0098086A',	'0098086C',	'0098086D',	'0098087B',	'0098087C',	'00980889',	'00980890',	'00980891',	'00980892',	'00980893',	'009808B5',	'009808C6',	'009808C7',	'009808C8',	'009808DB',	'009808DC',	'009808DF',	'009808E8',	'009808F1',	'009808F8',	'00980907',	'00980914',	'00980953',	'00980954',	'0098097B',	'009809AD',	'009809B5',	'009809B7',	'009809B8',	'009809C3',	'009809C4',	'009809CB',	'009809E5',	'009809E6',	'009809E9',	'009809EA',	'009809F3',	'009809F4',	'009809F5',	'009809FD',	'00980A01',	'00980A02',	'00980A03',	'00980A05',	'00980A06',	'00980A07',	'00980A14',	'00980A17',	'00980A19',	'00980A20',	'00980A21',	'00980A24',	'00980A27',	'00980A29',	'00980A2A',	'00980A40',	'00980A50',	'00980A52',	'00980A53',	'00980A74',	'00980A9A',	'00980AA0',	'00980AA2',	'00980AA3',	'00980AA5',	'00980AAF',	'00980ABA',	'00980ABB',	'00980AC1',	'00980AC9',	'00980ACB',	'00980AF4',	'00980B11',	'00980B12',	'00980B13',	'00980B1A',	'00980B1C',	'00980B1E',	'00980B29',	'00980B2A',	'00980B2E',	'00980B2F',	'00980B33',	'00980B34',	'00980B35',	'00980B36',	'00980B37',	'00980B58',	'00980B5E',	'00980B6A',	'00980B6B',	'00980B6C',	'00980B6E',	'00980B6F',	'00980B70',	'00980B74',	'00980B75',	'00980B76',	'00980B77',	'00980B7A',	'00980B80',	'00980B81',	'00980B84',	'00980B86',	'00980B87',	'00980B89',	'00980B8A',	'00980B8B',	'00980B8C',	'00980B90',	'00980B91',	'00980B93',	'00980B96',	'00980B97',	'00980B98',	'00980DA0',	'00980DA4',	'00980DA6',	'00980DA7',	'00980DBB',	'00980DCB',	'00980DCD',	'00980DD3',	'00980DD4',	'00980DD5',	'00980DD6',	'00980DDB',	'00980DE3',	'00980DE5',	'00980DE8',	'00980DF1',	'00980DFC',	'00980E05',	'00980E08',	'00980E09',	'00980E0E',	'00980E10',	'00980E13',	'00980E18',	'00980E1C',	'00980E1F',	'00980E22',	'00980E27',	'00980E28',	'00980E29',	'00980E2A',	'B120045E',	'B120045F',	'B1200461',	'B1200462',	'B1200464',	'B1200465',	'B1200631']

complete = []
if not os.path.exists('complete.txt'):
    open('complete.txt', "w").close()

with open("complete.txt", "r") as f:
    content = f.read().strip()
    complete = list(map(str, content.split(","))) if content else []

if not complete: 
    complete = ['00980AA1','009807C4','00980892','00980AF4','00980953']

serials = serials = list(set(serials_all) - set(complete))


for serial in sort(serials):
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
    while epoch > 1735732800:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
        data = meter_response(serial, epoch)
        if data is None:
            no_data_cnt += 1
            print(dt, "NO DATA", no_data_cnt)
            epoch -= 86400
            continue
        if data['Errors']:
            logging.error(f"{dt} ZZZ {serial} ERRORS: {data['Errors']}")
            if any('API' in str(item) for item in data['Errors'][0]):
                complete.append(serial)
                break
            elif any('DeviceSerial invalid' in str(item) for item in data['Errors'][1]):
                complete.append(serial)
                # with open("complete.txt", "w") as f:
                #     f.write(",".join(map(str, complete)))
                break
            elif len(data['Errors']) > 1 and any('Invalid DateStartSecUtc' in str(item) for item in data['Errors'][2]):
                logging.info('Invalid DateStartSecUtc NORMAL END OF DATA')
                # complete.append(serial)
                # with open("complete.txt", "w") as f:
                #     f.write(",".join(map(str, complete)))
                complete.append(serial)
                break
            else:
                logging.error(f"{dt} ZZZ {serial} ERRORS: uncaught error")
                complete.append(serial)
                break

        res, err = update_gb_db(serial, data, engines[1])
        if err:
            logging.error(f"ZZZ {serial} gb_1min update_gb_db ERROR: {err}")
            break

        logging.info(f"gb_1min GB: {xx} {serial} Date: {dt} Rows: {res}  {epoch}")
        # previous day
        epoch -= 86400
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
