from datetime import datetime, timedelta, timezone
import logging
import os
import sys

import pandas as pd
from sqlalchemy import text
from unhcr import constants as const
from unhcr import db
from unhcr import utils
from unhcr import api_solarman

# local testing ===================================
if const.LOCAL:  # testing with local python files
    const, db, utils, api_solarman, *rest = const.import_local_libs(
        mods=[
            ["constants", "const"],
            ["db", "db"],
            ["utils", "utils"],
            ["api_solarman", "api_solarman"],
        ]
    )

utils.log_setup(level="INFO", log_file="unhcr.sm_weather.log", override=True)
logging.info(
    f"{sys.argv[0]} Process ID: {os.getpid()}   Log Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}"
)

if not utils.is_version_greater_or_equal('0.4.6'):
    logging.error(
        "This version of the script requires at least version 0.4.6 of the unhcr module."
    )
    exit(46)

engines = [db.set_local_defaultdb_engine()]
if not const.is_running_on_azure():
    engines.append(db.set_azure_defaultdb_engine())

for engine in engines:
    epochs = []
    for site in api_solarman.SITE_ID:
        key = next(iter(site))
        device = api_solarman.WEATHER[key]
        epoch, err = db.get_sm_weather_max_epoch(device['deviceId'], engine)
        if err:
            logging.error(err)
            continue
        epochs.append(epoch)
    if not epochs:
        continue
    epoch = min(epochs)
    for site in api_solarman.SITE_ID:
        key = next(iter(site))
        device = api_solarman.WEATHER[key]

        date_obj = datetime.fromtimestamp(epoch, tz=timezone.utc).date()
        date_str = date_obj.strftime("%Y-%m-%d")

        while date_obj <= datetime.now().date():
            df = api_solarman.get_weather_data(date_str, devices = [device])
            if df is None:
                print(date_str, "NO DATA")
                date_obj = date_obj + timedelta(days=1)
                date_str = date_obj.strftime("%Y-%m-%d")
                continue
            res, err = api_solarman.update_weather_db(df, epoch, engine)
            if err:
                logging.error(f"sm_weather update_weather_db (next site) ERROR: {err}")
                break

            logging.info(f"sm_weather SITE: {site} Date: {date_str} Rows: {len(df)}")
            date_obj = date_obj + timedelta(days=1)
            date_str = date_obj.strftime("%Y-%m-%d")

        # input_file, output_file = "SOLARMAN/OGOJA/data/5_mins/ABUJA_W*.csv", "SOLARMAN/OGOJA/data/5_mins/merged_abuja_weather_sorted.csv"
        # utils.concat_csv_files(input_file, output_file, append=False)