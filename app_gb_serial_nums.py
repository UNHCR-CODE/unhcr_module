import csv
import json
import logging
import pandas as pd
import requests
from unhcr import app_utils
import unhcr.constants as const

mods = [
    ["constants", "const"],
    ["utils", "utils"],
    ["db", "db"],
    ["app_utils", "app_utils"],
]

res = app_utils.init(mods, "unhcr.gb_serial_nums.log", "0.4.7", level="INFO", override=True)
if const.LOCAL:
    const, utils, db, app_utils = res

engines = db.set_db_engines()

if const.is_running_on_azure():
    istvan_csv = "~/code/DATA/gaps/new_top_20_2025-03-11_trimmed.csv"
    gaps_csv = "~/code/DATA/gaps/gaps.csv"
    gtb_gaps_excel = "~/code/DATA/gaps/gtb_gaps.xlsx"
    all_api_gbs_csv = "~/code/DATA/all_api_gbs.csv"
else:
    istvan_csv = r"E:\_UNHCR\CODE\DATA\gaps\new_top_20_2025-03-11_trimmed.csv"
    gaps_csv = r"E:\_UNHCR\CODE\DATA\gaps\gaps.csv"
    gtb_gaps_excel = r"E:\_UNHCR\CODE\DATA\gaps\gtb_gaps.xlsx"
    all_api_gbs_csv = r"E:\_UNHCR\CODE\DATA\all_api_gbs.csv"

def get_gb_user_info_data(csv_file):
    url = const.GB_API_V1_API_BASE_URL + const.GB_API_V1_GET_DEVICE_LIST + const.GB_API_V1_USER_KEY
    payload = {}
    headers = {}

    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
        user_info_data = response.json()
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Network error: {e}")
        return None, app_utils.NetworkError(f"Network error: {e}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"Invalid response: {e}")
        return None, app_utils.InvalidResponseError(f"Invalid response: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"API request error: {e}")
        return None, app_utils.APIRequestError(f"API request error: {e}")
    except ValueError as e:
        logging.error(f"Invalid response: {e}")
        return None, app_utils.InvalidResponseError(f"Invalid response: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
        return None, app_utils.JSONDecodeError(f"Error decoding JSON: {e}")
    
    # Extract top-level DeviceSerialList
    top_level = user_info_data.get('UserInfo', {}).get('DeviceSerialList', [])

    top_level_serials = []
    for sn in top_level:
        top_level_serials.append(str(sn)[:3] + "-" + str(sn)[3:])


    # Extract DeviceSerials and SiteLabels from DisplayGroupList
    device_data = []
    for site in user_info_data.get('UserInfo', {}).get('SiteList', []):
        site_label = site.get('SiteLabel')
        for group in site.get('DisplayGroupList', []):
            for device in group.get('DeviceList', []):
                sn = device.get('DeviceSerial')
                sn = str(sn)[:3] + "-" + str(sn)[3:]
                device_data.append((site_label, sn))

    # Combine top-level serials with device data
    all_serials = [(None, serial) for serial in top_level_serials] + device_data

    # Remove duplicates if any
    all_serials = list(set(all_serials))

    # Write to CSV file
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['site_label', 'device_serial'])
        writer.writerows(all_serials)

    return all_serials, None

# #!!!!! gen all_api_gbs csv with eyedro label & serial
all_serials, err = get_gb_user_info_data(all_api_gbs_csv)
    # Print the result
x = 1
for item in all_serials:
    print(f" {x}  device_serial: {item[1]}  site_label: {item[0] if item[0] else 'N/A'}")
    x += 1
# #!!!!! gen all_api_gbs csv with eyedro label & serial

df_serial = pd.read_csv(all_api_gbs_csv)
df_serial['site_label'] = df_serial['site_label'].fillna('N/A')

df_gaps = pd.read_csv(gaps_csv)
df_gaps['gb_serial'] = df_gaps['gb_serial'].str[:3] + '-' + df_gaps['gb_serial'].str[3:]

df_serials = pd.merge(df_gaps, df_serial, left_on='gb_serial', right_on='device_serial', how='left')

df_top20 = pd.read_csv(istvan_csv, encoding="ISO-8859-1")

# # Merge with all rows from df2 and matching rows from df1
# merged_df = pd.merge(df_serials, df_top20, left_on='device_serial', right_on='Meter Serial No.', how='left')
# merged_df['site_label'] = merged_df['site_label'].fillna('')
# #merged_df['device_serial'] = merged_df['device_serial'].fillna('')
# merged_df = merged_df.fillna('')

matching_rows = []
# Loop through each row in df_serials
for idx, row in df_serials.iterrows():
    device_serial = row['device_serial']
    # Find all matching rows in df_top20
    matches = df_top20[df_top20['Meter Serial No.'] == device_serial]
    
    if not matches.empty:
        if len(matches) == 1:
            # Pick the first match (you can modify this to pick based on any condition)
            selected_match = matches.iloc[0]
        else:
            matches['Date Installed'] = matches['Date Installed'].fillna(pd.Timestamp('1900-01-01'))
            
            # Sort matches by 'Date Installed' to get the most recent first
            sorted_matches = matches.sort_values(by='Date Installed', ascending=False)
            
            # Pick the most recent match (first row after sorting)
            selected_match = sorted_matches.iloc[0]
    else:
        # If no match is found, append the original row from df_serials
        matching_rows.append(row)
        continue
    # Concatenate the original row from df_serials with the selected match
    merged_row = pd.concat([row, selected_match], axis=0)
    matching_rows.append(merged_row)

# Create a new DataFrame from the list of matching rows
matching_df = pd.DataFrame(matching_rows)
matching_df.to_excel(gtb_gaps_excel, index=False)

pass

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


# Load the JSON data
with open(r"D:\steve\Downloads\gb_userdata_response.json", 'r') as file:
    user_info_data = json.load(file)

# Extract top-level DeviceSerialList
top_level = user_info_data.get('UserInfo', {}).get('DeviceSerialList', [])

top_level_serials = []
for sn in top_level:
    top_level_serials.append(str(sn)[:3] + "-" + str(sn)[3:])


# Extract DeviceSerials and SiteLabels from DisplayGroupList
device_data = []
for site in user_info_data.get('UserInfo', {}).get('SiteList', []):
    site_label = site.get('SiteLabel')
    for group in site.get('DisplayGroupList', []):
        for device in group.get('DeviceList', []):
            sn = device.get('DeviceSerial')
            sn = str(sn)[:3] + "-" + str(sn)[3:]
            device_data.append((site_label, sn))

# Combine top-level serials with device data
all_serials = [(None, serial) for serial in top_level_serials] + device_data

# Remove duplicates if any
all_serials = list(set(all_serials))

# Print the result
x = 1
for item in all_serials:
    print(f" {x}  SiteLabel: {item[0] if item[0] else 'N/A'}, DeviceSerial: {item[1]}")
    x += 1


# Write to CSV file
gb_ds_csv = r"D:\steve\Downloads\gb_device_serials.csv"
with open(gb_ds_csv, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['SiteLabel', 'DeviceSerial'])
    writer.writerows(all_serials)


import json
import pandas as pd


import requests

url = "https://api.eyedro.com/Unhcr/DeviceData?DeviceSerial=0098084D&DateStartSecUtc=1739471000&DateNumSteps=1440&UserKey=UNkQm84Wyt61agj9jZShDFtS6fN8k7jAkxER2cSU"

payload = {}
headers = {
  'Cookie': 'AWSALB=rB0Fb18ZGBqCsyx3d6B8X6YFUZiiJBU9TUgelsqNmyY47JwM7Cihmp27MNpxNER/HkQaYjQH2jY5xxf/zKG7Nn5k+hn2GxXasqJ9/RS/WBiF1xQ55t0XQaua7QI0; AWSALBCORS=rB0Fb18ZGBqCsyx3d6B8X6YFUZiiJBU9TUgelsqNmyY47JwM7Cihmp27MNpxNER/HkQaYjQH2jY5xxf/zKG7Nn5k+hn2GxXasqJ9/RS/WBiF1xQ55t0XQaua7QI0'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)

user_info_data = json.loads(response.text)


# data = {
#     "DateMsUtc": 1739471830171,
#     "Errors": [],
#     "LastCommSecUtc": 1739471822,
#     "DeviceData": {
#         "A": [
#             [
#                 [
#                     1739471040,
#                     5.9
#                 ],
#                 [
#                     1739471100,
#                     5.9
#                 ],
#                 [
#                     1739471160,
#                     5.9
#                 ],
#                 [
#                     1739471220,
#                     5.9
#                 ],
#                 [
#                     1739471280,
#                     5.9
#                 ],
#                 [
#                     1739471340,
#                     5.9
#                 ],
#                 [
#                     1739471400,
#                     5.9
#                 ],
#                 [
#                     1739471460,
#                     5.9
#                 ],
#                 [
#                     1739471520,
#                     5.9
#                 ],
#                 [
#                     1739471580,
#                     5.9
#                 ],
#                 [
#                     1739471640,
#                     5.586
#                 ],
#                 [
#                     1739471700,
#                     5.544
#                 ],
#                 [
#                     1739471760,
#                     5.788
#                 ]
#             ],
#             [
#                 [
#                     1739471040,
#                     7.285
#                 ],
#                 [
#                     1739471100,
#                     7.285
#                 ],
#                 [
#                     1739471160,
#                     7.285
#                 ],
#                 [
#                     1739471220,
#                     7.285
#                 ],
#                 [
#                     1739471280,
#                     7.285
#                 ],
#                 [
#                     1739471340,
#                     7.285
#                 ],
#                 [
#                     1739471400,
#                     11.051
#                 ],
#                 [
#                     1739471460,
#                     12.218
#                 ],
#                 [
#                     1739471520,
#                     12.218
#                 ],
#                 [
#                     1739471580,
#                     8.778
#                 ],
#                 [
#                     1739471640,
#                     7.303
#                 ],
#                 [
#                     1739471700,
#                     7.303
#                 ],
#                 [
#                     1739471760,
#                     4.31
#                 ]
#             ],
#             [
#                 [
#                     1739471040,
#                     7.685
#                 ],
#                 [
#                     1739471100,
#                     6.98
#                 ],
#                 [
#                     1739471160,
#                     7.298
#                 ],
#                 [
#                     1739471220,
#                     7.773
#                 ],
#                 [
#                     1739471280,
#                     7.541
#                 ],
#                 [
#                     1739471340,
#                     7.276
#                 ],
#                 [
#                     1739471400,
#                     7.263
#                 ],
#                 [
#                     1739471460,
#                     6.942
#                 ],
#                 [
#                     1739471520,
#                     7.2
#                 ],
#                 [
#                     1739471580,
#                     7.575
#                 ],
#                 [
#                     1739471640,
#                     7.404
#                 ],
#                 [
#                     1739471700,
#                     6.961
#                 ],
#                 [
#                     1739471760,
#                     5.218
#                 ]
#             ]
#         ],
#         "V": [
#             [
#                 [
#                     1739471040,
#                     None
#                 ],
#                 [
#                     1739471100,
#                     None
#                 ],
#                 [
#                     1739471160,
#                     None
#                 ],
#                 [
#                     1739471220,
#                     None
#                 ],
#                 [
#                     1739471280,
#                     None
#                 ],
#                 [
#                     1739471340,
#                     None
#                 ],
#                 [
#                     1739471400,
#                     None
#                 ],
#                 [
#                     1739471460,
#                     None
#                 ],
#                 [
#                     1739471520,
#                     None
#                 ],
#                 [
#                     1739471580,
#                     None
#                 ],
#                 [
#                     1739471640,
#                     None
#                 ],
#                 [
#                     1739471700,
#                     None
#                 ],
#                 [
#                     1739471760,
#                     None
#                 ]
#             ],
#             [
#                 [
#                     1739471040,
#                     None
#                 ],
#                 [
#                     1739471100,
#                     None
#                 ],
#                 [
#                     1739471160,
#                     None
#                 ],
#                 [
#                     1739471220,
#                     None
#                 ],
#                 [
#                     1739471280,
#                     None
#                 ],
#                 [
#                     1739471340,
#                     None
#                 ],
#                 [
#                     1739471400,
#                     None
#                 ],
#                 [
#                     1739471460,
#                     None
#                 ],
#                 [
#                     1739471520,
#                     None
#                 ],
#                 [
#                     1739471580,
#                     None
#                 ],
#                 [
#                     1739471640,
#                     None
#                 ],
#                 [
#                     1739471700,
#                     None
#                 ],
#                 [
#                     1739471760,
#                     None
#                 ]
#             ],
#             [
#                 [
#                     1739471040,
#                     None
#                 ],
#                 [
#                     1739471100,
#                     None
#                 ],
#                 [
#                     1739471160,
#                     None
#                 ],
#                 [
#                     1739471220,
#                     None
#                 ],
#                 [
#                     1739471280,
#                     None
#                 ],
#                 [
#                     1739471340,
#                     None
#                 ],
#                 [
#                     1739471400,
#                     None
#                 ],
#                 [
#                     1739471460,
#                     None
#                 ],
#                 [
#                     1739471520,
#                     None
#                 ],
#                 [
#                     1739471580,
#                     None
#                 ],
#                 [
#                     1739471640,
#                     None
#                 ],
#                 [
#                     1739471700,
#                     None
#                 ],
#                 [
#                     1739471760,
#                     None
#                 ]
#             ]
#         ],
#         "PF": [
#             [
#                 [
#                     1739471040,
#                     None
#                 ],
#                 [
#                     1739471100,
#                     None
#                 ],
#                 [
#                     1739471160,
#                     None
#                 ],
#                 [
#                     1739471220,
#                     None
#                 ],
#                 [
#                     1739471280,
#                     None
#                 ],
#                 [
#                     1739471340,
#                     None
#                 ],
#                 [
#                     1739471400,
#                     None
#                 ],
#                 [
#                     1739471460,
#                     None
#                 ],
#                 [
#                     1739471520,
#                     None
#                 ],
#                 [
#                     1739471580,
#                     None
#                 ],
#                 [
#                     1739471640,
#                     None
#                 ],
#                 [
#                     1739471700,
#                     None
#                 ],
#                 [
#                     1739471760,
#                     None
#                 ]
#             ],
#             [
#                 [
#                     1739471040,
#                     None
#                 ],
#                 [
#                     1739471100,
#                     None
#                 ],
#                 [
#                     1739471160,
#                     None
#                 ],
#                 [
#                     1739471220,
#                     None
#                 ],
#                 [
#                     1739471280,
#                     None
#                 ],
#                 [
#                     1739471340,
#                     None
#                 ],
#                 [
#                     1739471400,
#                     None
#                 ],
#                 [
#                     1739471460,
#                     None
#                 ],
#                 [
#                     1739471520,
#                     None
#                 ],
#                 [
#                     1739471580,
#                     None
#                 ],
#                 [
#                     1739471640,
#                     None
#                 ],
#                 [
#                     1739471700,
#                     None
#                 ],
#                 [
#                     1739471760,
#                     None
#                 ]
#             ],
#             [
#                 [
#                     1739471040,
#                     None
#                 ],
#                 [
#                     1739471100,
#                     None
#                 ],
#                 [
#                     1739471160,
#                     None
#                 ],
#                 [
#                     1739471220,
#                     None
#                 ],
#                 [
#                     1739471280,
#                     None
#                 ],
#                 [
#                     1739471340,
#                     None
#                 ],
#                 [
#                     1739471400,
#                     None
#                 ],
#                 [
#                     1739471460,
#                     None
#                 ],
#                 [
#                     1739471520,
#                     None
#                 ],
#                 [
#                     1739471580,
#                     None
#                 ],
#                 [
#                     1739471640,
#                     None
#                 ],
#                 [
#                     1739471700,
#                     None
#                 ],
#                 [
#                     1739471760,
#                     None
#                 ]
#             ]
#         ],
#         "Wh": [
#             [
#                 [
#                     1739471040,
#                     21.633
#                 ],
#                 [
#                     1739471100,
#                     21.633
#                 ],
#                 [
#                     1739471160,
#                     21.633
#                 ],
#                 [
#                     1739471220,
#                     21.633
#                 ],
#                 [
#                     1739471280,
#                     21.633
#                 ],
#                 [
#                     1739471340,
#                     21.633
#                 ],
#                 [
#                     1739471400,
#                     21.633
#                 ],
#                 [
#                     1739471460,
#                     21.633
#                 ],
#                 [
#                     1739471520,
#                     21.633
#                 ],
#                 [
#                     1739471580,
#                     21.633
#                 ],
#                 [
#                     1739471640,
#                     20.485
#                 ],
#                 [
#                     1739471700,
#                     20.333
#                 ],
#                 [
#                     1739471760,
#                     21.226
#                 ]
#             ],
#             [
#                 [
#                     1739471040,
#                     26.717
#                 ],
#                 [
#                     1739471100,
#                     26.717
#                 ],
#                 [
#                     1739471160,
#                     26.717
#                 ],
#                 [
#                     1739471220,
#                     26.717
#                 ],
#                 [
#                     1739471280,
#                     26.717
#                 ],
#                 [
#                     1739471340,
#                     26.717
#                 ],
#                 [
#                     1739471400,
#                     40.522
#                 ],
#                 [
#                     1739471460,
#                     44.8
#                 ],
#                 [
#                     1739471520,
#                     44.8
#                 ],
#                 [
#                     1739471580,
#                     32.188
#                 ],
#                 [
#                     1739471640,
#                     26.783
#                 ],
#                 [
#                     1739471700,
#                     26.783
#                 ],
#                 [
#                     1739471760,
#                     15.807
#                 ]
#             ],
#             [
#                 [
#                     1739471040,
#                     28.173
#                 ],
#                 [
#                     1739471100,
#                     25.587
#                 ],
#                 [
#                     1739471160,
#                     26.756
#                 ],
#                 [
#                     1739471220,
#                     28.497
#                 ],
#                 [
#                     1739471280,
#                     27.645
#                 ],
#                 [
#                     1739471340,
#                     26.679
#                 ],
#                 [
#                     1739471400,
#                     26.63
#                 ],
#                 [
#                     1739471460,
#                     25.45
#                 ],
#                 [
#                     1739471520,
#                     26.4
#                 ],
#                 [
#                     1739471580,
#                     27.777
#                 ],
#                 [
#                     1739471640,
#                     27.147
#                 ],
#                 [
#                     1739471700,
#                     25.518
#                 ],
#                 [
#                     1739471760,
#                     19.126
#                 ]
#             ]
#         ]
#     }
# }

# Function to extract data ignoring nulls
def extract_data_ignore_nulls(device_data):
    # Iterate through each inner list and filter out entries where the second element is None
    return [[timestamp, data_point] for timestamp, data_point in device_data if data_point is not None]

# Iterate over the data, applying the extraction for each list in the "DeviceData"
cleaned_device_data = {
    key: [extract_data_ignore_nulls(value) for value in device_data_list] 
    for key, device_data_list in user_info_data["DeviceData"].items()
}

import pandas as pd

# Function to flatten cleaned_device_data into DataFrame format
import pandas as pd

# Function to convert device data to a DataFrame with the epoch as index
def convert_to_df_with_epoch_as_index(cleaned_device_data):
    # Initialize a dictionary to hold all columns (for each reading)
    epoch_data = {}

    # Determine the maximum length of device data lists (assuming all device types have the same number of entries)
    max_length = max(len(device_data) for device_data in cleaned_device_data.values())

    # Loop through each device type (A, V, PF, Wh) and process each index
    for idx in range(len(cleaned_device_data["A"][0])):
        for i in range(max_length):
            # Process each device type (A, V, PF, Wh) for each index (0, 1, 2, ...)
            for key, device_data_list in cleaned_device_data.items():
                if i < len(device_data_list):  # Ensure there's data at this index
                    if len(device_data_list[i]) == 0:  # Skip empty lists
                        continue

                    epoch, reading = device_data_list[i][idx]

                    # Create the column name based on the device type and index
                    column_name = f"p{i + 1}_{key}"

                    if 'epoch' not in epoch_data:
                        epoch_data['epoch'] = []
                    if epoch not in epoch_data['epoch']:
                        epoch_data['epoch'].append(epoch)
                    # Append the reading value to the respective column
                    if column_name not in epoch_data:
                        epoch_data[column_name] = []
                    epoch_data[column_name].append(reading)

    # Handle the case where there is no data
    if not epoch_data:
        print("No valid device data found.")
        return pd.DataFrame()  # Return an empty DataFrame if no data was processed

    # # Assuming that all device data lists have the same epoch values at the same indices, use the first one
    # epoch = cleaned_device_data["A"][0][idx] if "A" in cleaned_device_data else None

    # # Create a DataFrame from the epoch_data dictionary
    df = pd.DataFrame(epoch_data)

    # Use the first epoch value to set the index
    # if epoch is not None:
    #     df['Epoch'] = epoch[0]
    #    df.set_index('Epoch', inplace=True)

    return df

# Convert cleaned_device_data to DataFrame
df = convert_to_df_with_epoch_as_index(cleaned_device_data)
df.columns = df.columns.str.lower()
df = df[sorted(df.columns)]

# Display the DataFrame
print(df)


# Output the cleaned data
print(json.dumps(cleaned_device_data, indent=2))
exit(777)




from fuzzywuzzy import fuzz
from fuzzywuzzy import process

# Load the Excel file
file_path = r'D:\Downloads\device_serials.xlsx'  # Update with your path
xls = pd.ExcelFile(file_path)

# Read the correct sheets
df1 = pd.read_excel(xls, sheet_name='device_serials')
df2 = pd.read_excel(xls, sheet_name='serials')

# Drop NaN values
device_serials = df1['Serial Number'].dropna()
site_sns = df2['DeviceSerial'].dropna()

# Perform fuzzy matching
matches = []
for serial in device_serials:
    best_match = process.extractOne(serial, site_sns, scorer=fuzz.ratio)
    matches.append((serial, best_match[0], best_match[1]))

# Convert matches to DataFrame
matches_df = pd.DataFrame(matches, columns=['DeviceSerial', 'MatchedSerial', 'Score'])

# Merge df1 and matches_df
merged_df = df1.merge(matches_df, left_on='Serial Number', right_on='DeviceSerial')

# Merge with df2 to get more details from serials sheet
final_output = merged_df.merge(df2, left_on='MatchedSerial', right_on='DeviceSerial', how='left')

# Filter matches with a high score
final_output = final_output[final_output['Score'] > 99]

# Remove duplicate rows based on all columns
no_dups_output = final_output.drop_duplicates()

# Save the results to an Excel file with two sheets
output_path = 'fuzzy_matches.xlsx'
with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    final_output.to_excel(writer, sheet_name='fuzzy', index=False)
    no_dups_output.to_excel(writer, sheet_name='no-dups', index=False)

print(f"Matches saved to {output_path}")

"""
{
    1739471066416,
    1739471056,
"""
