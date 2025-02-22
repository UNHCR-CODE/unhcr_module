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


logging.info(
    f"{sys.argv[0]} Process ID: {os.getpid()}   Log Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}"
)

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
    if len(epochs) == 0:
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

            df.to_sql("temp_weather", engine, schema="solarman", if_exists="replace", index=False)

            with engine.begin() as conn:
                conn.execute(text("""
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
                """))
                ######conn.execute(text("DROP TABLE solarman.temp_weather;"))
                conn.commit()

            logging.info(date_str)
            date_obj = date_obj + timedelta(days=1)
            date_str = date_obj.strftime("%Y-%m-%d")

        # input_file, output_file = "SOLARMAN/OGOJA/data/5_mins/ABUJA_W*.csv", "SOLARMAN/OGOJA/data/5_mins/merged_abuja_weather_sorted.csv"
        # utils.concat_csv_files(input_file, output_file, append=False)