from datetime import datetime, timedelta, timezone
import logging

from unhcr import app_init
from unhcr import constants as const
from unhcr import db
from unhcr import api_solarman

mods = [
    ["constants", "const"],
    ["db", "db"],
    ["api_solarman", "api_solarman"],
]

res = app_init.init(mods, "unhcr.sm_weather.log", "0.4.6", level="INFO", override=True)
if const.LOCAL:
    const, db, api_solarman = res

engines = db.set_db_engines()
if const.is_running_on_azure():
    engines = [engines[1]]

for engine in engines:
    epochs = []
    for site in api_solarman.SITE_ID:
        key = next(iter(site))
        device = api_solarman.WEATHER[key]
        epoch, err = db.get_sm_weather_max_epoch(device["deviceId"], engine)
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
            df = api_solarman.get_weather_data(date_str, devices=[device])
            if df is None:
                print(date_str, "NO DATA")
                date_obj = date_obj + timedelta(days=1)
                date_str = date_obj.strftime("%Y-%m-%d")
                continue
            res, err = api_solarman.update_weather_db(df, epoch, engine)
            if err:
                logging.error(f"sm_weather update_weather_db (next site) ERROR: {err}")
                break

            logging.info(f"sm_weather SITE: {site} Date: {date_str} Rows: {res}")
            date_obj = date_obj + timedelta(days=1)
            date_str = date_obj.strftime("%Y-%m-%d")

        # input_file, output_file = "SOLARMAN/OGOJA/data/5_mins/ABUJA_W*.csv", "SOLARMAN/OGOJA/data/5_mins/merged_abuja_weather_sorted.csv"
        # utils.concat_csv_files(input_file, output_file, append=False)
