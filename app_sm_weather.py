from datetime import datetime, timezone
import time

from unhcr import app_utils
from unhcr import constants as const
from unhcr import db
from unhcr import api_solarman
from unhcr import err_handler

mods = [
    ["app_utils", "app_utils"],
    ["constants", "const"],
    ["db", "db"],
    ["api_solarman", "api_solarman"],
    ["err_handler", "err_handler"],
]

res = app_utils.app_init(
    mods=mods,
    log_file="unhcr.sm_weather.log",
    version="0.4.6",
    level="INFO",
    override=True,
    quiet=False,
)
logger = res[0]
if const.LOCAL:
    logger, app_utils, const, db, api_solarman, err_handler = res

eng = db.set_local_defaultdb_engine()
epochs = []



df_weather_devices, err = api_solarman.db_get_devices_site_sn_id(eng, dev_type="WEATHER_STATION")
if err:
    logger.error(f'sm_weather db_get_devices_site_sn_id ERROR: {err}')
    exit(1)
devices = df_weather_devices["device_sn"].tolist()
for deviceSn in devices:
    epoch, err = api_solarman.db_get_sm_weather_max_epoch(eng, deviceSn)
    if err:
        logger.error(err)
        continue
    epochs.append(epoch)
if not epochs:
    logger.warning(f'No epochs for weather devices found to process {df_weather_devices["device_sn"]}')
    exit(1)
epoch = min(epochs)
epoch_now = int(time.time())
epoch = epoch_now - (5 * 86400)
epoch = epoch - (epoch % 86400)
MAX_LOOPS = 1000
while epoch < epoch_now:
    date_str = (
        datetime.fromtimestamp(epoch, tz=timezone.utc).date().strftime("%Y-%m-%d")
    )
    df_data, err = err_handler.error_wrapper(lambda: api_solarman.api_get_weather_data(date_str, df_weather_devices))
    if err:
        logger.error(f"sm_weather db_update_weather (next day) ERROR: {err}")
    else:
        res, err = api_solarman.db_update_weather(df_data, epoch, eng)
    if err:
        logger.error(f"sm_weather db_update_weather (next day) ERROR: {err}")

    #next day
    epoch = epoch + 86400
    MAX_LOOPS -= 1
    if MAX_LOOPS <= 0:
        logger.warning("Max loops reached, exiting...")
        break
