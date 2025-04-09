from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import partial
import itertools
import numpy as np
import os
import pandas as pd
import threading

# OPTIONAL: set your own environment
##ef = const.load_env(r'E:\_UNHCR\CODE\unhcr_module\.env')
## print(ef)
# OPTIONAL: set your own environment

from unhcr import app_utils
from unhcr import constants as const
from unhcr import db
from unhcr import gb_eyedro

mods=[
    ["app_utils", "app_utils"],
    ["constants", "const"],
    ["db", "db"],
    ["gb_eyedro", "gb_eyedro"],
]

res = app_utils.app_init(mods, log_file="unhcr.app_gb_1min_data.log", version="0.4.7", 
                         level="INFO", override=True, quiet=False)
logger = res[0]
if const.LOCAL:
    logger,app_utils, const, db, gb_eyedro = res


#!!!!!! upload new gb data to db concurrent -- update all_api_gbs using unhcr_module\gb_serial_nums.py
global_counter = itertools.count(0)
counter_lock = threading.Lock()

def process_chunk_new(chunk, param1, param2, param3, param4):
    with counter_lock:
        chunk_count = next(param4)  # Thread-safe chunk count
    chunk_size = len(chunk)
    print(f"Processing chunk #{chunk_count} with {chunk_size} items")

    results = []
    item_count = itertools.count(0)  # Reset counter for each chunk
    item_lock = threading.Lock()

    for item in chunk:
        with item_lock:
            current_item_count = next(item_count)  # Now it's per chunk

        print(f"Processing chunk #{chunk_count} item #{current_item_count}")
        msg = f'{chunk_count}:{current_item_count} of {chunk_size}'
        result = gb_eyedro.upsert_gb_data(s_num=item, engine=param1, epoch_cutoff=param2, MAX_EMPTY=param3, msg=msg, logger=logger)  # Apply function to each item
        #########result = add_constraints(eng=param1, serial=item)
        results.append(result)

    return results

def process_chunk_historical(chunk, param1, param2, param3, param4):
    with counter_lock:
        chunk_count = next(param4)  # Thread-safe chunk count

    chunk_size = len(chunk)
    print(f"Processing chunk #{chunk_count} with {chunk_size} items")
    results = []
    item_count = itertools.count(0)  # Reset counter for each chunk
    item_lock = threading.Lock()

    for item in chunk:
        with item_lock:
            current_item_count = next(item_count)  # Now it's per chunk
        print(f"Processing chunk #{chunk_count} item #{current_item_count}")
        msg = f'{chunk_count}:{current_item_count} of {chunk_size}'
        result = gb_eyedro.upsert_gb_data(s_num=item, engine=param1, epoch_start=param2, MAX_EMPTY=param3, msg=msg, logger=logger)  # Apply function to each item
        results.append(result)
    return results

run_dt = datetime.now().date()
#ILTERED_GB_SN_PATH=const.add_csv_dt(const.ALL_API_GBS_CSV_PATH, (run_dt - timedelta(days=1)).isoformat()) #const.add_csv_dt(const.ALL_API_GBS_CSV_PATH, run_dt.isoformat())
FILTERED_GB_SN_PATH=const.add_csv_dt(const.ALL_API_GBS_CSV_PATH, (run_dt).isoformat()) #const.add_csv_dt(const.ALL_API_GBS_CSV_PATH, run_dt.isoformat())

if os.path.exists(FILTERED_GB_SN_PATH):
    filtered_gb_sn_df = pd.read_csv(FILTERED_GB_SN_PATH)
else:
    df, err = gb_eyedro.api_get_user_info_as_df()
    if err:
        logger.error(f"api_get_user_info_as_df ERROR: {err}")
        exit(1)

    filtered_gb_sn_df = df[df['gb_serial'].str.startswith('B') | df['gb_serial'].str.startswith('009')]['gb_serial'].drop_duplicates()

sn_array = sorted(filtered_gb_sn_df.str.replace('-', '').tolist())

#sn_array = ['B120045E']

num_parts = 10
chunks = [list(chunk) for chunk in np.array_split(sn_array, num_parts)]  # Ensure list format

days = 5
dt_start = datetime.now(timezone.utc)
cutoff = datetime.now(timezone.utc) - timedelta(days=days)
epoch_cutoff = app_utils.get_previous_midnight_epoch(int(cutoff.timestamp()))
eng = db.set_local_defaultdb_engine()
MAX_EMPTY = days


with ThreadPoolExecutor(max_workers=num_parts) as executor:
    item_counter = itertools.count(0)
    results = list(executor.map(partial(process_chunk_new, param1=eng, param2=epoch_cutoff, param3=MAX_EMPTY, param4=global_counter), chunks))

# epoch_start = gb_eyedro.epoch_2024 + 86400
# with ThreadPoolExecutor(max_workers=num_parts) as executor:
#     item_counter = itertools.count(0)
#     results = list(executor.map(partial(process_chunk_historical, param1=eng, param2=epoch_start, param3=MAX_EMPTY, param4=global_counter), chunks))


# Flatten results
final_output = [item for sublist in results for item in sublist]

# Compute elapsed time
elapsed = datetime.now(timezone.utc) - dt_start

# Format as hh:mm:ss
formatted_elapsed = str(elapsed).split('.')[0] 
print('total time:', formatted_elapsed)
pass
#!!!!!! upload new gb data to db concurrent
