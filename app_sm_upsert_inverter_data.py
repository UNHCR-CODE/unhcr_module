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

db_eng = db.set_local_defaultdb_engine()
# Fetch inverter serial numbers
inverters_sn = api_solarman.db_get_inverter_sns(db_eng)

num_threads = len(inverters_sn)  # Number of parallel threads (adjust as needed)
#num_threads = 1  # For testing, set to 1 to avoid parallel processing

# Split inverter list into chunks for parallel processing
chunks = [list(chunk) for chunk in np.array_split(inverters_sn, num_threads)]  

# Thread-safe counter and API blocker
counter_lock = threading.Lock()
api_lock = threading.Lock()
def process_chunk(chunk, date, engine):
    """Fetch inverter data for a chunk of inverters in parallel."""
    with counter_lock:
        chunk_count = next(global_counter)  # Thread-safe chunk count
    chunk_size = len(chunk)
    logger.info(f"Processing chunk #{chunk_count} on {date} with {chunk_size} inverters")

    results = []
    for sn in chunk:
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
        logger.info(f"SN: {sn} | Execution time: {((end_time - start_time)):.2f} secs")

        results.append((sn, res, err))

    return results

# **MAIN PROCESS LOOP**
# start_dt = datetime.strptime('2025-03-22', "%Y-%m-%d").date() # set a specific date
start_dt = datetime.today().date() #!!!!!!+ timedelta(days=1)
days = 3
timing = time.time()
while days > 0:
    days -= 1
    global_counter = itertools.count(1)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(
            executor.map(lambda chunk: process_chunk(chunk, start_dt, db_eng), chunks)
        )

    logger.info(f"Completed processing for {start_dt}. Moving to next day.")
    # Compute elapsed time
    et = time.time()
    elapsed = et-timing
    timing = et
    logger.info(f"Elapsed time: {elapsed:.2f} seconds")
    start_dt -= timedelta(days=1)
    if start_dt < datetime.strptime('2024-10-01', "%Y-%m-%d").date():
        break

# Flatten results
final_output = [item for sublist in results for item in sublist]
