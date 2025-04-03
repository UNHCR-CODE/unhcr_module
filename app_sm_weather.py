from datetime import datetime, timedelta, timezone
import logging
import time

from unhcr import app_utils
from unhcr import constants as const
from unhcr import db
from unhcr import api_solarman

mods = [
    ["app_utils", "app_utils"],
    ["constants", "const"],
    ["db", "db"],
    ["api_solarman", "api_solarman"],
]

res = app_utils.app_init(mods, "unhcr.sm_weather.log", "0.4.6", level="INFO", override=True)
if const.LOCAL:
    logger,app_utils, const, db, api_solarman = res
else:
    logger = res

if const.is_running_on_azure():
    engines = [db.set_local_defaultdb_engine()]
else:
    engines = db.set_db_engines()

for eng in engines:
    devices = []
    epochs = []
    for site in api_solarman.SITE_ID:
        key = next(iter(site))
        device = api_solarman.WEATHER[key]
        devices.append(device)
        epoch, err = db.get_sm_weather_max_epoch(device["deviceId"], eng)
        if err:
            logger.error(err)
            continue
        epochs.append(epoch)
    if not epochs:
        continue
    epoch = min(epochs)
    epoch = int(time.time()) - (5 * 86400)
    epoch = epoch - (epoch % 86400)
    while epoch < int(time.time()):
        date_str = datetime.fromtimestamp(epoch, tz=timezone.utc).date().strftime("%Y-%m-%d")
        df_devices =api_solarman.db_get_devices_site_sn_id(eng, dev_type="WEATHER_STATION")
        df = api_solarman.api_get_weather_data(date_str, df_devices)
        pass
        res, err = api_solarman.update_weather_db(df, epoch, eng)
        if err:
            logger.error(f"sm_weather update_weather_db (next site) ERROR: {err}")
            break
        pass
        epoch = epoch + 86400

    # pass

    # for site in api_solarman.SITE_ID:
    #     key = next(iter(site))
    #     device = api_solarman.WEATHER[key]

    #     date_obj = datetime.fromtimestamp(epoch, tz=timezone.utc).date()
    #     date_str = date_obj.strftime("%Y-%m-%d")

    #     while date_obj <= datetime.now().date():
    #         df = api_solarman.get_weather_data(date_str, devices=[device])
    #         if df is None:
    #             print(date_str, "NO DATA")
    #             date_obj = date_obj + timedelta(days=1)
    #             date_str = date_obj.strftime("%Y-%m-%d")
    #             continue
    #         res, err = api_solarman.update_weather_db(df, epoch, eng)
    #         if err:
    #             logger.error(f"sm_weather update_weather_db (next site) ERROR: {err}")
    #             break

    #         logger.info(f"sm_weather SITE: {site} Date: {date_str} Rows: {res}")
    #         date_obj = date_obj + timedelta(days=1)
    #         date_str = date_obj.strftime("%Y-%m-%d")

    #     # input_file, output_file = "SOLARMAN/OGOJA/data/5_mins/ABUJA_W*.csv", "SOLARMAN/OGOJA/data/5_mins/merged_abuja_weather_sorted.csv"
    #     # utils.concat_csv_files(input_file, output_file, append=False)
