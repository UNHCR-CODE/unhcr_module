"""
    main_bulk_fuel_v2.py
    Updates the bulk fuel data in the MySQL database.
    It reads data from CSV files, concatenates them, and updates the database with the new data.
    The script uses the constants module to manage environment-specific settings and the db module to
    interact with the database. It also uses the utils module for logging and versioning.
    The script includes conditional logic to control which sites are updated and whether the data is saved
    to a temporary CSV file or directly to the database.

    The script is designed to be run periodically to keep the database up to date with the latest fuel data.
"""

import os
import pandas as pd

import unhcr.constants as const

# OPTIONAL: set your own environment
##ef = const.load_env(r'E:\_UNHCR\CODE\unhcr_module\.env')
## print(ef)
# OPTIONAL: set your own environment

import unhcr.db as db
import unhcr.galooli_sm_fuel as sm_fuel
import unhcr.utils as utils

mods = const.import_local_libs(
        mods=[
            ["constants", "const"],
            ["db", "db"],
            ["galooli_sm_fuel", "sm_fuel"],
            ["utils", "utils"],
        ]
    )
logger, *rest = mods
if const.LOCAL: # testing with local python files
    logger, const, db, sm_fuel, utils = mods

if not utils.is_version_greater_or_equal('0.4.7'):
    logger.error(
        "This version of the script requires at least version 0.4.7 of the unhcr module."
    )
    exit(47)

# Solarman API credentials
APP_ID = const.SM_APP_ID
APP_SECRET = const.SM_APP_SECRET
# !!!!!!!!! this expires every 2 months
BIZ_ACCESS_TOKEN = const.SM_BIZ_ACCESS_TOKEN
URL = const.SM_URL

engines = db.set_db_engines()

# Load CSV and get data

tz = "GMT"
fn = None
site = None  ## Element in BULK --- set to None to do all sites
table = None
label = None
# TODO move to .env file
dpath = r"E:\steve\Downloads" + "\\"

# FALSE = add rows from Galooli downloaded file to local xlsx and save to DB
# TRUE = save all rows from Galooli downloaded file to local temp csv file -- does not touch DB
# TODO: commandline var sometimes you want to rerun without combining the Galooli bulk fuel files ?????
mashup = True
#mashup = False

for idx in range(len(sm_fuel.BULK)):

    site, table, fn, label = utils.extract_data(sm_fuel.BULK[idx])

    if site is None:
        continue

    if mashup:
        df = sm_fuel.concat_csv_files(dpath, fn, label)
        logger.debug(f"concat_csv_files: {len(df)} rows")
        if len(df) == 0:
            continue

    if not os.path.exists(dpath + fn):
        print("File not found", dpath + fn)
        continue
    
    if not label in df.values[0][0]:
        continue
    
    df["Time"] = pd.to_datetime(df["Time"], dayfirst=False)
    df = df.sort_values(by="Time")
    df["Value"] = df["Value"].astype(str)

    for engine in engines:
        res, err = db.update_bulk_fuel(engine, df, table)
        if res:
            print(f"{site} {table} update_bulk_fuel DB rows:  inserted/updated: {res[0][0]}/{res[0][1]}")
        if err:
            print(f"{site} {table}  update_bulk_fuel DB ERROR:  {err}")
    pass

