import csv
import json

#2025-03-07
all_api_gbs = ['00980639',	'0098063D',	'00980640',	'00980641',	'00980644',	'00980645',	'0098064A',	'0098064C',	'0098064D',	'0098064E',	'00980741',	'00980742',	'00980747',	'0098074A',	'00980753',	'00980755',	'00980756',	'00980757',	'00980759',	'0098075A',	'0098075B',	'00980760',	'00980780',	'00980781',	'00980785',	'00980786',	'00980789',	'0098078A',	'0098078B',	'0098079A',	'009807B3',	'009807B4',	'009807B5',	'009807C4',	'009807D2',	'009807D4',	'009807D5',	'009807D6',	'009807D7',	'009807D8',	'009807D9',	'009807DB',	'009807DE',	'009807F5',	'009807F6',	'009807FB',	'00980820',	'00980822',	'00980823',	'00980825',	'00980826',	'00980827',	'00980828',	'0098082A',	'0098082B',	'0098082D',	'0098082E',	'0098082F',	'00980830',	'00980831',	'00980832',	'00980833',	'00980834',	'00980835',	'00980836',	'00980838',	'00980839',	'0098083A',	'0098083B',	'0098083C',	'0098083E',	'0098083F',	'00980840',	'00980841',	'00980842',	'00980843',	'00980844',	'00980845',	'00980846',	'00980847',	'00980848',	'00980849',	'0098084A',	'0098084B',	'0098084C',	'0098084D',	'0098084E',	'0098084F',	'00980850',	'00980855',	'00980856',	'00980857',	'00980858',	'00980859',	'0098085A',	'0098085B',	'0098085C',	'0098085E',	'0098085F',	'00980860',	'00980861',	'00980862',	'00980863',	'00980864',	'00980865',	'00980866',	'00980867',	'00980868',	'00980869',	'0098086B',	'0098086E',	'0098086F',	'00980870',	'00980871',	'00980872',	'00980873',	'00980874',	'00980875',	'00980876',	'00980877',	'00980878',	'00980879',	'0098087A',	'0098087A',	'0098087B',	'0098087C',	'0098087D',	'0098087F',	'00980880',	'00980881',	'00980882',	'00980883',	'00980885',	'00980887',	'00980888',	'00980889',	'0098088A',	'0098088B',	'0098088C',	'0098088D',	'0098088E',	'00980890',	'00980891',	'00980892',	'00980893',	'00980894',	'00980895',	'00980896',	'00980897',	'00980898',	'00980899',	'0098089A',	'0098089B',	'0098089C',	'0098089D',	'0098089E',	'0098089F',	'009808A8',	'009808A9',	'009808A9',	'009808AA',	'009808AF',	'009808B0',	'009808B1',	'009808B2',	'009808B3',	'009808B4',	'009808B5',	'009808B6',	'009808B7',	'009808B8',	'009808B9',	'009808BA',	'009808BB',	'009808BC',	'009808BD',	'009808BE',	'009808BF',	'009808C6',	'009808C7',	'009808C8',	'009808D5',	'009808D8',	'009808D9',	'009808DA',	'009808DB',	'009808DC',	'009808DD',	'009808DE',	'009808E0',	'009808E1',	'009808E2',	'009808E3',	'009808E5',	'009808E6',	'009808E8',	'009808E9',	'009808EA',	'009808EB',	'009808EC',	'009808EF',	'009808F0',	'009808F1',	'009808F2',	'009808F4',	'009808F5',	'009808F6',	'009808F7',	'009808F8',	'009808FC',	'009808FD',	'009808FE',	'00980901',	'00980902',	'00980905',	'00980906',	'00980907',	'0098090A',	'0098090B',	'0098090C',	'0098090D',	'0098090E',	'0098090F',	'00980910',	'00980911',	'00980912',	'00980914',	'00980915',	'00980916',	'00980918',	'00980919',	'0098091A',	'0098091B',	'00980921',	'00980923',	'00980925',	'00980926',	'00980927',	'00980929',	'0098092A',	'0098092B',	'0098092C',	'0098092D',	'0098092F',	'00980931',	'00980932',	'00980933',	'00980935',	'00980937',	'0098093A',	'0098093B',	'00980952',	'00980954',	'00980957',	'00980958',	'00980959',	'0098095A',	'0098095B',	'0098095F',	'00980960',	'00980961',	'00980962',	'00980966',	'00980967',	'00980978',	'0098097A',	'0098097B',	'0098097C',	'0098097E',	'0098097F',	'00980981',	'00980982',	'00980984',	'00980985',	'00980986',	'00980987',	'00980988',	'00980989',	'0098098D',	'009809A8',	'009809A8',	'009809AC',	'009809AD',	'009809AE',	'009809B5',	'009809B6',	'009809B8',	'009809C3',	'009809C4',	'009809C6',	'009809C7',	'009809C8',	'009809C9',	'009809CA',	'009809CB',	'009809CC',	'009809D4',	'009809DA',	'009809DF',	'009809E1',	'009809E4',	'009809E5',	'009809E6',	'009809E8',	'009809E9',	'009809EA',	'009809ED',	'009809EE',	'009809EF',	'009809F0',	'009809F2',	'009809F2',	'009809F3',	'009809F4',	'009809F5',	'009809F6',	'009809F7',	'009809F9',	'009809FA',	'009809FB',	'009809FC',	'009809FD',	'009809FE',	'009809FF',	'00980A00',	'00980A01',	'00980A02',	'00980A05',	'00980A06',	'00980A07',	'00980A09',	'00980A0A',	'00980A0B',	'00980A0C',	'00980A0D',	'00980A0F',	'00980A10',	'00980A11',	'00980A12',	'00980A13',	'00980A14',	'00980A15',	'00980A16',	'00980A17',	'00980A18',	'00980A19',	'00980A1A',	'00980A1B',	'00980A1C',	'00980A1E',	'00980A1F',	'00980A20',	'00980A21',	'00980A22',	'00980A24',	'00980A25',	'00980A26',	'00980A27',	'00980A28',	'00980A29',	'00980A2A',	'00980A2B',	'00980A2C',	'00980A2D',	'00980A2E',	'00980A2F',	'00980A30',	'00980A31',	'00980A32',	'00980A33',	'00980A34',	'00980A35',	'00980A36',	'00980A37',	'00980A38',	'00980A39',	'00980A3C',	'00980A3D',	'00980A3E',	'00980A3F',	'00980A40',	'00980A47',	'00980A48',	'00980A49',	'00980A4C',	'00980A4D',	'00980A4E',	'00980A4F',	'00980A50',	'00980A51',	'00980A52',	'00980A53',	'00980A57',	'00980A66',	'00980A67',	'00980A68',	'00980A69',	'00980A6A',	'00980A6B',	'00980A6D',	'00980A6E',	'00980A6F',	'00980A72',	'00980A73',	'00980A74',	'00980A75',	'00980A76',	'00980A97',	'00980A99',	'00980A9B',	'00980A9C',	'00980A9D',	'00980A9E',	'00980A9F',	'00980AA0',	'00980AA1',	'00980AA2',	'00980AA3',	'00980AA4',	'00980AA5',	'00980AAE',	'00980AB3',	'00980AB7',	'00980AB8',	'00980AB9',	'00980ABA',	'00980ABB',	'00980AC1',	'00980AC2',	'00980AC3',	'00980AC4',	'00980AC8',	'00980AC9',	'00980ACB',	'00980ACC',	'00980AD1',	'00980ADE',	'00980ADF',	'00980AE3',	'00980AE6',	'00980AEC',	'00980AEE',	'00980AF1',	'00980AF4',	'00980AF5',	'00980AFE',	'00980AFF',	'00980B00',	'00980B01',	'00980B11',	'00980B13',	'00980B14',	'00980B17',	'00980B19',	'00980B1A',	'00980B1C',	'00980B1E',	'00980B21',	'00980B22',	'00980B23',	'00980B24',	'00980B27',	'00980B28',	'00980B2A',	'00980B2C',	'00980B2F',	'00980B31',	'00980B33',	'00980B34',	'00980B35',	'00980B35',	'00980B36',	'00980B37',	'00980B38',	'00980B58',	'00980B5C',	'00980B5E',	'00980B5F',	'00980B60',	'00980B62',	'00980B63',	'00980B64',	'00980B66',	'00980B6A',	'00980B6B',	'00980B6C',	'00980B6E',	'00980B6F',	'00980B70',	'00980B74',	'00980B75',	'00980B76',	'00980B77',	'00980B78',	'00980B79',	'00980B7A',	'00980B7F',	'00980B80',	'00980B81',	'00980B84',	'00980B85',	'00980B86',	'00980B87',	'00980B8A',	'00980B8B',	'00980B8C',	'00980B90',	'00980B91',	'00980B95',	'00980B96',	'00980B97',	'00980B98',	'00980DA1',	'00980DA2',	'00980DA4',	'00980DA5',	'00980DA7',	'00980DA8',	'00980DA9',	'00980DAA',	'00980DAD',	'00980DB4',	'00980DB6',	'00980DB7',	'00980DB9',	'00980DBA',	'00980DBB',	'00980DBF',	'00980DC1',	'00980DC2',	'00980DC3',	'00980DC4',	'00980DC5',	'00980DC6',	'00980DCB',	'00980DCC',	'00980DCD',	'00980DCF',	'00980DD0',	'00980DD1',	'00980DD2',	'00980DD3',	'00980DD4',	'00980DD5',	'00980DD6',	'00980DD7',	'00980DD8',	'00980DD9',	'00980DDB',	'00980DDC',	'00980DDD',	'00980DDE',	'00980DDF',	'00980DE0',	'00980DE3',	'00980DE4',	'00980DE5',	'00980DE6',	'00980DE7',	'00980DE8',	'00980DEC',	'00980DEE',	'00980DF1',	'00980DF4',	'00980DF6',	'00980DF9',	'00980DFC',	'00980DFE',	'00980E05',	'00980E08',	'00980E09',	'00980E0B',	'00980E0D',	'00980E0E',	'00980E0F',	'00980E10',	'00980E11',	'00980E13',	'00980E14',	'00980E17',	'00980E18',	'00980E19',	'00980E1A',	'00980E1B',	'00980E1C',	'00980E1D',	'00980E1F',	'00980E22',	'00980E23',	'00980E24',	'00980E26',	'00980E28',	'00980E29',	'00980E2A',	'91880100',	'B120045D',	'B120045E',	'B120045F',	'B1200461',	'B1200462',	'B1200463',	'B1200464',	'B1200465',	'B1200466',	'B12004B7',	'B12005E8',	'B12005ED',	'B1200613',	'B120061A',	'B1200631',	'B1200632',	'B1400275',	'B1400276']


# Load the JSON data
with open(r"D:\steve\Downloads\gb_userdata_response.json", 'r') as file:
    data = json.load(file)

# Extract top-level DeviceSerialList
top_level = data.get('UserInfo', {}).get('DeviceSerialList', [])

top_level_serials = []
for sn in top_level:
    top_level_serials.append(str(sn)[:3] + "-" + str(sn)[3:])


# Extract DeviceSerials and SiteLabels from DisplayGroupList
device_data = []
for site in data.get('UserInfo', {}).get('SiteList', []):
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
csv_file = r"D:\steve\Downloads\gb_device_serials.csv"
with open(csv_file, 'w', newline='') as file:
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

data = json.loads(response.text)


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
    for key, device_data_list in data["DeviceData"].items()
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
