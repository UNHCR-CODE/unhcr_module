"""
Overview:
    Processes the 5-minute solar data from Solarman API and updates the fuel data in the database.

Environment setup: 
    Checks if the const.LOCAL flag is set, and if so, imports local libraries using the 
    const.import_local_libs function.
    
Logging setup: 
    Sets up logging using the utils.log_setup function, specifying the log level as "INFO" 
    and the log file as "unhcr.solar_5min.log".
    
Version check: 
    Checks if the version of the unhcr module is greater than or equal to "0.4.7", and if not, 
    logs an error message and exits with an error code.
    
Database engine setup: 
    Sets up database engines using the db.set_local_defaultdb_engine and 
    db.set_azure_defaultdb_engine functions.
    
Data retrieval: 
    Retrieves data from the Solarman API using the api_solarman.get_solarman_data function.
    
Data processing: 
    Process data from the Solarman API, using the api_solarman module to retrieve 
    data and the db module to interact with the database.
    
Error handling: 
    The file handles errors that may occur during the processing of data, such as logging errors and 
    exiting with an error code. The error messages are logged using the logging.error function. 
"""

from decimal import Decimal
import glob
import logging
import numpy as np
import os
import pandas as pd
import sys

import unhcr.constants as const

# OPTIONAL: set your own environment
##ef = const.load_env(r'E:\_UNHCR\CODE\unhcr_module\.env')
## print(ef)
# OPTIONAL: set your own environment

from unhcr import api_solarman
from unhcr import db
from unhcr import nigeria_sm_fuel as sm_fuel
from unhcr import utils

if const.LOCAL:  # testing with local python files
    const, api_solarman, db, sm_fuel, utils = const.import_local_libs(
        # mpath=const.MOD_PATH,
        mods=[
            ["constants", "const"],
            ["api_solarman", "api_solarman"],
            ["db", "db"],
            ["nigeria_sm_fuel", "sm_fuel"],
            ["utils", "utils"],
        ]
    )

utils.log_setup(level="INFO", log_file="unhcr.solar_5min.log", override=True)
logging.info(
    f"{sys.argv[0]} Process ID: {os.getpid()}   Log Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}"
)

if not utils.is_version_greater_or_equal("0.4.7"):
    logging.error(
        "This version of the script requires at least version 0.4.7 of the unhcr module."
    )
    exit(47)

engines = db.set_db_engines()

dpath = r"E:\_UNHCR\CODE\NIGERIA_FUEL_BIOHENRY\data\galooli\solarman" + "\\"

from_dt = None
from_dt = "2024-10-01"  # to process all data starting here

from deepdiff import DeepDiff

diff = DeepDiff(sm_fuel.INVERTERS, api_solarman.INVERTERS, ignore_order=True)
print(diff.pretty())

engines= [engines[0]]
for engine in engines:
    for office in api_solarman.INVERTERS:
        site = office["site"]
        # only supports 3 sites currently that have solarman and galooli fuel data
        if not site in ['ABUJA', 'OGOJA', 'OGOJA_GH']:
            continue

        ts, err = db.get_fuel_max_ts(site, engine)
        if err:
            logging.error(err)
            continue
        site1, table, fn, label = utils.extract_data(api_solarman.INVERTERS, site)

        if site1 is None:
            continue
        from_dt = ts.strftime("%Y-%m-%d %H:%M")

        # Path to the directory containing downloaded Galooli CSV files (change as needed)
        download_dir = "D:/steve/Downloads"


        # Read all CSV files in the directory
        all_files = glob.glob(f"{download_dir}/Detailed Fuel*.csv")

        # List to store filtered DataFrames
        df_list = []

        for file in all_files:
            df = pd.read_csv(file)  # Read CSV file

            # Check if "Unit Name" column exists and filter by required value
            if "Unit Name" in df.columns:
                df_filtered = df[df["Unit Name"] == label]

                if not df_filtered.empty:  # Only keep files that have matching rows
                    df_list.append(df_filtered)

        # Combine filtered DataFrames
        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)

            # Convert 'Time' column to datetime format
            combined_df["Time"] = pd.to_datetime(
                combined_df["Time"], format="%d/%m/%Y %H:%M:%S"
            )

            # Sort by 'Time' column
            combined_df = combined_df.sort_values(by="Time")
            df_filtered = combined_df[combined_df["Time"] > from_dt].copy()
            # Save the combined sorted CSV
            df_filtered["Time"] = df_filtered["Time"].dt.strftime("%Y-%m-%dT%H:%M:%S")

            df_filtered.to_csv("combined_sorted_filtered.csv", index=False)
            logging.debug(
                f"{len(df_filtered)}  {len(combined_df)}  ✅ Combined and sorted CSV saved as 'combined_sorted_filtered.csv'."
            )
        else:
            logging.warning("⚠️ No matching data found in any CSV files.")
            continue

        if df_filtered.empty:
            logging.warning(f"⚠️ {site} No new data found.")
            continue

        liters, sm_data = sm_fuel.extract_csv_data_new(
            site, df_filtered, from_dt=from_dt
        )
        # remove data older than ts
        threshold = pd.Timestamp("2025-03-10 13:00:00", tz="UTC") 

        df_liters = pd.DataFrame(liters)
        df_liters["datetime"] = pd.to_datetime(pd.to_numeric(df_liters["epoch"]), unit="s", utc=True)
        df_liters["date"] = df_liters["datetime"].dt.date
        df_liters["hour"] = df_liters["datetime"].dt.hour
        # Filter rows where datetime_column is less than or equal to the threshold
        df_liters = df_liters[df_liters['datetime'] > threshold]
        # Group data by date and hour, summing values
        hourly_sums_liters = (df_liters.groupby(["date", "hour"])[["dl1", "dl2"]].sum().reset_index())

        # Process the provided data into DataFrame
        df_sm_data = pd.DataFrame(sm_data, columns=["epoch", "value", "cnt"])
        df_sm_data["datetime"] = pd.to_datetime(pd.to_numeric(df_sm_data["epoch"]), unit="s", utc=True)
        df_sm_data["date"] = df_sm_data["datetime"].dt.date
        df_sm_data["hour"] = df_sm_data["datetime"].dt.hour
        # Filter rows where datetime_column is less than or equal to the threshold
        df_sm_data = df_sm_data[df_sm_data['datetime'] > threshold]
        # Group by date and hour to sum kWh values
        hourly_sums_kwh = (
            df_sm_data.groupby(["date", "hour"])["value"].sum().reset_index()
        )

        # Set MultiIndex for merging
        hourly_sums_kwh.set_index(["date", "hour"], inplace=True)
        hourly_sums_liters.set_index(["date", "hour"], inplace=True)

        # Merge DataFrames
        merged_hourly_sums = pd.concat(
            [hourly_sums_kwh, hourly_sums_liters], axis=1
        ).reset_index()
        # Replace NaN values with 0 to avoid InvalidOperation
        merged_hourly_sums.fillna(0, inplace=True)

        # Convert columns to Decimal safely
        decimal_cols = ["value", "dl1", "dl2"]
        for col in decimal_cols:
            merged_hourly_sums[col] = merged_hourly_sums[col].apply(
                lambda x: Decimal(str(x)) if x is not None else Decimal(0)
            )

        # Calculate ratios (avoiding division by zero)
        merged_hourly_sums["kWh/L1"] = merged_hourly_sums.apply(
            lambda row: (
                (
                    row["value"]
                    * (
                        row["dl1"]
                        / (row["dl1"] + (0 if pd.isna(row["dl2"]) else row["dl2"]))
                    )
                )
                / row["dl1"]
                if row["value"] > 0.2 and row["dl1"] > 0
                else None
            ),
            axis=1,
        )
        merged_hourly_sums["kWh/L2"] = merged_hourly_sums.apply(
            lambda row: (
                (
                    row["value"]
                    * (
                        row["dl2"]
                        / (row["dl2"] + (0 if pd.isna(row["dl1"]) else row["dl1"]))
                    )
                )
                / row["dl2"]
                if row["value"] > 0.2 and row["dl2"] != 0
                else None
            ),
            axis=1,
        )

        merged_hourly_sums.iloc[:, 1:] = (
            merged_hourly_sums.iloc[:, 1:].replace(0, np.nan).round(3).astype("float64")
        )

        # merged_hourly_sums.index = merged_hourly_sums.index.astype(int)
        # merged_hourly_sums.iloc[:, 1:] = (
        #     merged_hourly_sums.iloc[:, 1:] = merged_hourly_sums.iloc[:, 1:].replace(0, np.nan).round(3)
        # )
        # fix NAN hour back to zero
        merged_hourly_sums["hour"] = merged_hourly_sums["hour"].fillna(0)



        res, err = db.update_fuel_data(engine, merged_hourly_sums, table, site)
        if err:
            logging.error(err.message[:100])
