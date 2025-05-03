
# UNHCR Data Integration Module

  

## 2025-05-02 v_1.0.0 (WIP branch)

### NOTE:

	This is a work in progress branch. The current version is v_0.4.9

	Also the solarman API key expires every 2 months (July 1st 2025)

	One var added to .env file: PROS_GB_IN_KEY=06d01.........
	
You have to get the biz token and manually add it to the .env file:

	SM_BIZ_ACCESS_TOKEN="your token"

You have to use --no-deps to PIP install:

	pip install --no-deps .

Also:

	pip install -r --no-deps fedotreqs.txt

This is to allow incompatibilities with the fedotreqs.txt file. These will be resolved in the future.

##

2025-02-16 v_0.4.7

This module facilitates the integration of data from the Leonics API into UNHCR's systems, specifically a MySQL database, and the Prospect system. It handles authentication with the Leonics API, data retrieval, filtering, and updates to both target systems.

## Key Features

*  **Automated Data Retrieval:** Fetches data from the Leonics API within a specified timeframe, using an authentication token.

*  **Incremental Updates:** Updates both the MySQL database and Prospect with only new records since the last update, minimizing data transfer and redundancy.

*  **Data Filtering:** Cleans and preprocesses the data by removing placeholder values, ensuring data quality.

*  **Error Handling and Logging:** Includes logging for monitoring and debugging purposes, as well as error handling mechanisms.

*  **Configuration via Environment Variables:** Sensitive data such as API keys and connection strings are stored securely using environment variables.

## Module Structure  (outdated)

The module is organized into several Python files:

*  **`api.py`:** Handles interaction with the Leonics and Prospect APIs, including authentication and data retrieval/submission.

*  **`db.py`:** Manages database operations, including updates to the MySQL database and interaction with Prospect.

*  **`s3.py`:** Provides functionality for interacting with AWS S3 storage (currently used for listing files).

*  **`utils.py`:** Contains utility functions for data cleaning and processing, such as filtering specific values from nested data structures.

*  **`constants.py`:** Stores configuration constants such as API endpoints, database connection strings, and S3 credentials, loaded from a `.env` file.

*  **`__main__.py`:** Entry point for the module when executed directly.

*  **`__init__.py`:** Initializes the module and makes its functions accessible.

## Installation

For demonstration purposes I am using Windows and installing under: **_UNHCR\CODE**

1.  **Clone the repository:**

		git clone https://github.com/unhcr-smh/unhcr_module.git

		cd _UNHCR\CODE\unhcr_module

2.  **Create a virtual environment and activate it:**

		python3 -m venv .venv

		source .venv/bin/activate # On Windows: .venv\Scripts\activate

3. Install the module

		pip install .

4. Edit the run.bat file if your absolute path is different

		REM change to your venv path if necessary

		call E:\_UNHCR\CODE\unhcr_module\.venv\Scripts\activate.bat

		python E:\_UNHCR\CODE\unhcr_module\unhcr\full_test.py --log INFO

5. Create a **.env** file in the project root directory. You need to get the secrets and API keys from From Proton drive.

Here is the **.env** without values: (Note that this is out dated)

		#[KOBO]

		KOBO_CREATE_ENGINE=

		KOBO_DATABASE=

		KOBO_HOST=

		KOBO_PASSWORD=

		KOBO_PORT=

		KOBO_SOURCE_FILE=

		KOBO_TABLE_NAME=

		KOBO_SHEET_NAME=

		KOBO_USER=

		KOBO_URL=

		KOBO_PROJECT_URL=

		KOBO_SSO_LOGIN=

		  

		#[GB_2024]

		GB_2024_CREATE_ENGINE=

		  

		#[GB_MISC]

		GB_MISC_CREATE_ENGINE=

		  

		#[GB_2023]

		GB_2023_CREATE_ENGINE=

		  

		#[GB_API_V1]

		GB_API_V1_API_BASE_URL=

		GB_API_V1_GET_DATA=

		GB_API_V1_GET_DEVICE_LIST=

		GB_API_V1_USER_KEY=

		  

		#[GB_AWS]

		GB_AWS_ACCESS_KEY=

		GB_AWS_SECRET_KEY=

		GB_AWS_BUCKET_NAME=

		GB_AWS_FOLDER_NAME=

		  

		#[PROTON]

		PROTON_RECOVERY_PHRASE=

		PROTON_URL=

		PROTON_USER=

		PROTON_PW=

		  

		#[LEONICS]

		LEONICS_BASE_URL=

		LEONICS_USER_CODE=

		LEONICS_KEY=

		  

		#[PROSPECT]

		PROS_LOCAL_BASE_URL=

		PROS_IN_LOCAL_API_KEY=

		PROS_OUT_LOCAL_API_KEY=

		PROS_BASE_URL=

		PROS_IN_API_KEY=

		PROS_OUT_API_KEY=

		  

		#[AIVEN]

		AIVEN_TAKUM_LEONICS_API_RAW_CONN_STR=

		  

		#[SOLARMAN NIGERIA]

		SM_APP_ID=

		SM_APP_SECRET=

		#[SM_BIZ] will expire every 2 months

		SM_BIZ_ACCESS_TOKEN=

		SM_URL=

6. Run the main integration test code, Execute **run.bat**. Note this will actually update the Prospect and Aiven DBs. The output should be similar: (truncating the venv setup)

### Output from run.bat
		
		2025-01-17 17:32:04,665 - INFO - PROD: True, DEBUG: False, LOCAL: False 1 .env file @: E:\_UNHCR\CODE\unhcr_module\.env

		2025-01-17 17:32:04,665 - INFO - Process ID: 22500 Log Level: INFO

		2025-01-17 17:32:04,668 - INFO - Version: 0.4.3 Error: None

		2025-01-17 17:32:04,668 - INFO - Getting auth token for date: 2025-01-17

		2025-01-17 17:32:06,700 - INFO - Getting auth token for date: 2025-01-18

		2025-01-17 17:32:15,388 - INFO - ROWS UPDATED: TAKUM_LEONICS_API_RAW 3

		2025-01-17 17:32:16,638 - INFO - Server at http://localhost:3000 is responding. Status code: 200

		2025-01-17 17:32:16,638 - INFO - Starting update_prospect ts: None local = True

		2025-01-17 17:32:18,025 - INFO -

		  

		74a1680a57a88fbd56e4af59576d7d3e

		http://localhost:3000/api/v1/out/custom/?size=50&page=1&q[source_id_eq]=1&q[s]=created_at+desc

		2025-01-18 00:27

		2025-01-17 17:32:19,814 - INFO - 201: {"id":647,"created_at":"2025-01-17T23:32:19.734Z","status":"Import started"}

		2025-01-17 17:32:19,814 - INFO - Data has been saved to 'py_pros'

		2025-01-17 17:32:19,814 - INFO - LOCAL: TRUE 201: {"id":647,"created_at":"2025-01-17T23:32:19.734Z","status":"Import started"}

		2025-01-17 17:32:19,815 - INFO - Starting update_prospect ts: None local = False

		2025-01-17 17:32:20,571 - INFO -

		  

		c91c1da9b43658800b079d52953d83f7

		https://app.prospect.energy/api/v1/out/custom/?size=50&page=1&q[source_id_eq]=421&q[s]=created_at+desc

		2025-01-18 00:27

		2025-01-17 17:32:22,161 - INFO - 201: {"id":4974688,"created_at":"2025-01-17T23:32:22.114Z","status":"Import started"}

		2025-01-17 17:32:22,162 - INFO - Data has been saved to 'py_pros'

		2025-01-17 17:32:22,162 - INFO - LOCAL: FALSE 201: {"id":4974688,"created_at":"2025-01-17T23:32:22.114Z","status":"Import started"}

		  

		(.venv) E:\_UNHCR\CODE\unhcr_module>.venv\Scripts\python.exe unhcr\full_test.py --log INFO

		2025-01-17 17:33:15,404 - INFO - PROD: True, DEBUG: False, LOCAL: False 1 .env file @: E:\_UNHCR\CODE\unhcr_module\.env

		2025-01-17 17:33:15,404 - INFO - Process ID: 45588 Log Level: INFO

		2025-01-17 17:33:15,407 - INFO - Version: 0.4.3 Error: None

		2025-01-17 17:33:15,407 - INFO - Getting auth token for date: 2025-01-17

		2025-01-17 17:33:17,736 - INFO - Getting auth token for date: 2025-01-18

		2025-01-17 17:33:26,538 - INFO - ROWS UPDATED: TAKUM_LEONICS_API_RAW 1

		2025-01-17 17:33:30,629 - ERROR - Server at http://localhost:3000 is not responding.

		2025-01-17 17:33:30,630 - INFO - Starting update_prospect ts: None local = False

		2025-01-17 17:33:31,431 - INFO -

		https://app.prospect.energy/api/v1/out/custom/?size=50&page=1&q[source_id_eq]=421&q[s]=created_at+desc

		2025-01-18 00:30

		2025-01-17 17:33:33,049 - INFO - 201: {"id":4974689,"created_at":"2025-01-17T23:33:33.005Z","status":"Import started"}

		2025-01-17 17:33:33,050 - INFO - Data has been saved to 'py_pros'

		2025-01-17 17:33:33,050 - INFO - LOCAL: FALSE 201: {"id":4974689,"created_at":"2025-01-17T23:33:33.005Z","status":"Import started"}

  

1. Run tests. From the project root directory, execute **tests.bat**. Output: 

		================================================= test session starts ==================================================
		platform win32 -- Python 3.12.10, pytest-8.3.4, pluggy-1.5.0 -- E:\_UNHCR\CODE\unhcr_module\vfedot\Scripts\python.exe    
		cachedir: .pytest_cache
		rootdir: E:\_UNHCR\CODE\unhcr_module\tests
		configfile: pytest.ini
		plugins: anyio-4.9.0, cov-6.0.0, time-machine-2.16.0
		collecting ... 
		------------------------------------------------- live log collection --------------------------------------------------

		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		DEBUG    root:utils.py:371 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:utils.py:376 ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']        ['E:\\_UNHCR\\CODE\\unhcr_module\\vfedot\\Scripts\\pytest', '-v', '--cache-clear', '--cov=..', '--cov-report=html', '--env=E:/_UNHCR/CODE/unhcr_module/.env', '--log=INFO']
		DEBUG    root:constants.py:282 PROD: False, DEBUG: False, LOCAL: True 
		collected 158 items                                                                                                      

		test_api_leonics.py::test_getAuthToken[specific_date]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		INFO     root:api_leonics.py:65 Getting auth token for date: 2024-08-22
		response ZZZZZ <Mock id='2518272712416'> None
		PASSED
		test_api_leonics.py::test_getAuthToken[default_today]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		INFO     root:api_leonics.py:65 Getting auth token for date: 2025-05-03
		response ZZZZZ <Mock id='2518272714864'> None
		PASSED
		test_api_leonics.py::test_checkAuth[auth_and_check_auth_success]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		INFO     root:api_leonics.py:65 Getting auth token for date: 2025-05-03
		PASSED
		test_api_leonics.py::test_getData[happy_path] PASSED
		test_api_leonics.py::test_getData[no_token] PASSED
		test_api_leonics.py::test_getData[api_error_404] PASSED
		test_api_prospect.py::test_get_prospect_url_key[local_in]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:api_prospect.py:70 ZZZZZZZZZZZZZZZ
		local  True
		 out False
		url http://localhost:3000/api
		ZZZZZZZZZZZZZZ
		PASSED
		test_api_prospect.py::test_get_prospect_url_key[local_out]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:api_prospect.py:70 ZZZZZZZZZZZZZZZ
		local  True
		 out True
		url http://localhost:3000/api
		ZZZZZZZZZZZZZZ
		PASSED
		test_api_prospect.py::test_get_prospect_url_key[external_in]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:api_prospect.py:70 ZZZZZZZZZZZZZZZ
		local  False
		 out False
		url https://app.prospect.energy/api
		ZZZZZZZZZZZZZZ
		PASSED
		test_api_prospect.py::test_get_prospect_url_key[external_out]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:api_prospect.py:70 ZZZZZZZZZZZZZZZ
		local  False
		 out True
		url https://app.prospect.energy/api
		ZZZZZZZZZZZZZZ
		PASSED
		test_api_prospect.py::test_api_in_prospect[happy_path_local]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:api_prospect.py:70 ZZZZZZZZZZZZZZZ
		local  True
		 out False
		url http://localhost:3000/api
		ZZZZZZZZZZZZZZ
		PASSED
		test_api_prospect.py::test_api_in_prospect[happy_path_external]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:api_prospect.py:70 ZZZZZZZZZZZZZZZ
		local  False
		 out False
		url https://app.prospect.energy/api
		ZZZZZZZZZZZZZZ
		PASSED
		test_api_prospect.py::test_api_in_prospect[empty_dataframe_local] PASSED
		test_api_prospect.py::test_api_in_prospect[empty_dataframe_external] PASSED
		test_api_prospect.py::test_get_prospect_last_data[valid_response]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    charset_normalizer:api.py:461 Encoding detection: ascii is most likely the one.
		################# 2024-08-15T12:00:00Z 2024-08-15T12:00:00Z
		PASSED
		test_api_prospect.py::test_get_prospect_last_data[empty_data]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    charset_normalizer:api.py:461 Encoding detection: ascii is most likely the one.
		ERROR    root:api_prospect.py:289 ERROR: get_prospect_last_data list index out of range
		#################
		PASSED
		test_api_prospect.py::test_get_prospect_last_data[multiple_entries]
		---------------------------------------------------- live log call -----------------------------------------------------
		DEBUG    charset_normalizer:api.py:461 Encoding detection: ascii is most likely the one.
		################# 2024-08-16T12:00:00Z 2024-08-16T12:00:00Z
		PASSED
		test_api_prospect.py::test_get_prospect_last_data[invalid_json_no_data]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    charset_normalizer:api.py:461 Encoding detection: ascii is most likely the one.
		ERROR    root:api_prospect.py:289 ERROR: get_prospect_last_data 'data'
		#################
		PASSED
		test_api_prospect.py::test_get_prospect_last_data[missing_timestamp]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    charset_normalizer:api.py:461 Encoding detection: ascii is most likely the one.
		ERROR    root:api_prospect.py:289 ERROR: get_prospect_last_data 'custom'
		#################
		PASSED
		test_app_utils.py::test_app_init_basic Result: (<MagicMock name='log_setup()' id='2518273740048'>,)
		PASSED
		test_app_utils.py::test_app_init_custom_level PASSED
		test_app_utils.py::test_app_init_override_logging PASSED
		test_app_utils.py::test_app_init_version_check_fail PASSED
		test_app_utils.py::test_app_init_local_modules PASSED
		test_app_utils.py::test_app_init_custom_mpath PASSED
		test_app_utils.py::test_app_init_exception_handling PASSED
		test_db.py::test_set_db_engine PASSED
		test_db.py::test_set_db_engine_by_name_postgresql PASSED
		test_db.py::test_set_db_engine_by_name_non_postgresql PASSED
		test_db.py::test_get_db_session_success PASSED
		test_db.py::test_get_db_session_exception
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:db.py:186 Database update failed: Textual SQL expression 'INVALID SQL' should be explicitly declared as text('INVALID SQL')
		PASSED
		test_db.py::test_sql_execute_select SELECT 1

		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:233 SELECT 1
		PASSED
		test_db.py::test_sql_execute_insert INSERT INTO TAKUM_LEONICS_API_RAW (datetimeserver, BDI1_Power_P1_kW) VALUES ('2024-01-01 10:00:00', 1) returning datetimeserver

		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:233 INSERT INTO TAKUM_LEONICS_API_RAW (datetimeserver, BDI1_Power_P1_kW) VALUES ('2024-01-01 10:00:00', 1) returning datetimeserver
		PASSED
		test_db.py::test_sql_execute_with_parameters INSERT INTO TAKUM_LEONICS_API_RAW (datetimeserver, BDI1_Power_P1_kW) VALUES ('2024-08-03 10:00:00', 300) Returning datetimeserver

		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:233 INSERT INTO TAKUM_LEONICS_API_RAW (datetimeserver, BDI1_Power_P1_kW) VALUES ('2024-08-03 10:00:00', 300) Returning datetimeserver
		SELECT COUNT(*) FROM TAKUM_LEONICS_API_RAW
		DEBUG    root:db.py:233 SELECT COUNT(*) FROM TAKUM_LEONICS_API_RAW
		PASSED
		test_db.py::test_sql_execute_error INVALID SQL

		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:233 INVALID SQL
		ERROR    root:db.py:251 Database update failed: (sqlite3.OperationalError) near "INVALID": syntax error
		[SQL: INVALID SQL]
		(Background on this error at: https://sqlalche.me/e/20/e3q8)
		PASSED
		test_db.py::test_sql_execute_no_engine PASSED
		test_db.py::test_get_db_max_date INSERT INTO TAKUM_LEONICS_API_RAW (DatetimeServer, BDI1_Power_P1_kW) VALUES ('2024-08-01 10:00', 1)

		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:233 INSERT INTO TAKUM_LEONICS_API_RAW (DatetimeServer, BDI1_Power_P1_kW) VALUES ('2024-08-01 10:00', 1)
		ERROR    root:db.py:251 Database update failed: This result object does not return rows. It has been closed automatically.
		select max(DatetimeServer) FROM TAKUM_LEONICS_API_RAW
		DEBUG    root:db.py:233 select max(DatetimeServer) FROM TAKUM_LEONICS_API_RAW
		PASSED
		test_db.py::test_get_db_max_date_error
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:db.py:300 Can not get DB max timestanp
		PASSED
		test_db.py::test_db_update_leonics PASSED
		test_db.py::test_update_rows_filtering
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:447 ROWS UPDATED: 0
		PASSED
		test_db.py::test_update_rows_no_data PASSED
		test_db.py::test_update_rows_postgresql
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:447 ROWS UPDATED: 0
		PASSED
		test_db.py::test_update_rows_mysql
		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:447 ROWS UPDATED: 0
		<MagicMock name='set_db_engine_by_name' id='2518276493040'>
		PASSED
		test_db.py::test_get_db_session PASSED
		test_db.py::test_db_update_leonics_and_update_rows[db_update_leonics_success] SELECT COUNT(*) FROM takum_leonics_api_raw 

		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:233 SELECT COUNT(*) FROM takum_leonics_api_raw
		PASSED
		test_db.py::test_db_update_leonics_and_update_rows[db_update_leonics_failure] SELECT COUNT(*) FROM takum_leonics_api_raw

		---------------------------------------------------- live log call ----------------------------------------------------- 
		DEBUG    root:db.py:233 SELECT COUNT(*) FROM takum_leonics_api_raw
		PASSED
		test_db.py::test_update_prospect SKIPPED (Need to put data in the database first)
		test_db.py::test_backfill_prospect SKIPPED (Backfill is not ready yet)
		test_db.py::test_prospect_get_start_ts[local_no_start_ts] PASSED
		test_db.py::test_prospect_get_start_ts[external_no_start_ts] PASSED
		test_db.py::test_prospect_get_start_ts[local_with_start_ts] PASSED
		test_db.py::test_prospect_get_start_ts[external_with_start_ts] PASSED
		test_err_handler.py::test_err_details[exception0-Database connection error]
		---------------------------------------------------- live log call -----------------------------------------------------
		ERROR    root:err_handler.py:24 ERROR: Database connection error: Database connection error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.OperationalError: Database connection error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Database connection error: Database connection error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.OperationalError: Database connection error

		PASSED
		test_err_handler.py::test_err_details[exception1-Database query error]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Database query error: Database query error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.DatabaseError: Database query error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Database query error: Database query error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.DatabaseError: Database query error

		PASSED
		test_err_handler.py::test_err_details[exception2-Database interface error]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Database interface error: Database interface error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.InterfaceError: Database interface error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Database interface error: Database interface error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.InterfaceError: Database interface error

		PASSED
		test_err_handler.py::test_err_details[exception3-Database programming error]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Database query error: Database programming error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.ProgrammingError: Database programming error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Database query error: Database programming error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.ProgrammingError: Database programming error

		PASSED
		test_err_handler.py::test_err_details[exception4-Data error]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Database query error: Data error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.DataError: Data error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Database query error: Data error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		psycopg2.DataError: Data error

		PASSED
		test_err_handler.py::test_err_details[exception5-Memory error]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Memory error: Memory error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		MemoryError: Memory error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Memory error: Memory error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		MemoryError: Memory error

		PASSED
		test_err_handler.py::test_err_details[exception6-Invalid response ERROR]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Invalid response ERROR: Invalid response ERROR
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		ValueError: Invalid response ERROR
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Invalid response ERROR: Invalid response ERROR
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		ValueError: Invalid response ERROR

		PASSED
		test_err_handler.py::test_err_details[exception7-Error decoding JSON ERROR]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Invalid response ERROR: Error decoding JSON ERROR: line 1 column 1 (char 0)       
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		json.decoder.JSONDecodeError: Error decoding JSON ERROR: line 1 column 1 (char 0)
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Invalid response ERROR: Error decoding JSON ERROR: line 1 column 1 (char 0)
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		json.decoder.JSONDecodeError: Error decoding JSON ERROR: line 1 column 1 (char 0)

		PASSED
		test_err_handler.py::test_err_details[exception8-Error with type conversion in serial processing ERROR]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Error with type conversion in serial processing ERROR: Error with type conversion 
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		TypeError: Error with type conversion
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Error with type conversion in serial processing ERROR: Error with type conversion   
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		TypeError: Error with type conversion

		PASSED
		test_err_handler.py::test_err_details[exception9-Error with string slicing in serial processing ERROR]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Error with string slicing in serial processing ERROR: Error with string slicing   
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		IndexError: Error with string slicing
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Error with string slicing in serial processing ERROR: Error with string slicing     
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		IndexError: Error with string slicing

		PASSED
		test_err_handler.py::test_err_details[exception10-Missing expected key in data structure ERROR]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Missing expected key in data structure ERROR: 'Missing expected key'
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		KeyError: 'Missing expected key'
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Missing expected key in data structure ERROR: 'Missing expected key'
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		KeyError: 'Missing expected key'

		PASSED
		test_err_handler.py::test_err_details[exception11-HTTP ERROR]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: HTTP ERROR: HTTP ERROR : None
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		requests.exceptions.HTTPError: HTTP ERROR
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 HTTP ERROR: HTTP ERROR : None
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		requests.exceptions.HTTPError: HTTP ERROR

		PASSED
		test_err_handler.py::test_err_details[exception12-Connection Error]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Connection Error: Connection Error :
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		requests.exceptions.ConnectionError: Connection Error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Connection Error: Connection Error :
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		requests.exceptions.ConnectionError: Connection Error

		PASSED
		test_err_handler.py::test_err_details[exception13-Timeout Error]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Timeout Error: Timeout Error  :
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		requests.exceptions.Timeout: Timeout Error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Timeout Error: Timeout Error  :
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		requests.exceptions.Timeout: Timeout Error

		PASSED
		test_err_handler.py::test_err_details[exception14-Request Error]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Request Error: Request Error :
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		requests.exceptions.RequestException: Request Error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Request Error: Request Error :
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		requests.exceptions.RequestException: Request Error

		PASSED
		test_err_handler.py::test_err_details[exception15-Unexpected error]
		---------------------------------------------------- live log call ----------------------------------------------------- 
		ERROR    root:err_handler.py:24 ERROR: Unexpected error: Unexpected error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		Exception: Unexpected error
		 : E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py : test_err_details : 37
		DEBUG    root:test_err_handler.py:39 Unexpected error: Unexpected error
		TRACE:Traceback (most recent call last):
		  File "E:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\unhcr\err_handler.py", line 85, in error_wrapper
		    res = func()
		          ^^^^^^
		  File "E:\_UNHCR\CODE\unhcr_module\tests\test_err_handler.py", line 35, in func
		    raise exception
		Exception: Unexpected error

		PASSED
		test_gb_eyedro.py::test_db_create_tables_success test_123
		XXXXXX', test_123, []
		YYYYYY' test_123 -1  <sqlalchemy.engine.cursor.CursorResult object at 0x0000024A54FB2120>
		ZZZZZZZ' test_123 -1
		PASSED
		test_gb_eyedro.py::test_db_create_tables_invalid_table_name test_invalid!@#
		PASSED
		test_gb_eyedro.py::test_db_create_tables_connection_issue PASSED
		test_gb_eyedro.py::test_db_create_tables_insufficient_permissions PASSED
		test_gb_eyedro.py::test_db_hyper_gb_gaps_success Processed hypertable gb_test_123, gaps found: 2
		PASSED
		test_gb_eyedro.py::test_hyper_gb_gaps_concur_success Processed hypertable gb_test_123, gaps found: 2
		Processed hypertable gb_test_321, gaps found: 2
		PASSED
		test_gb_eyedro.py::test_api_get_gb_user_info_success PASSED
		test_gb_eyedro.py::test_api_get_gb_user_info_request_error PASSED
		test_gb_eyedro.py::test_api_get_gb_user_info_json_error PASSED
		test_gb_eyedro.py::test_parse_user_info_as_df_success PASSED
		test_gb_eyedro.py::test_parse_user_info_as_df_empty_data PASSED
		test_gb_eyedro.py::test_parse_user_info_as_df_missing_site_label PASSED
		test_gb_eyedro.py::test_parse_user_info_as_df_standalone_serial PASSED
		test_gb_eyedro.py::test_api_get_user_info_as_df_success PASSED
		test_gb_eyedro.py::test_api_get_user_info_as_df_api_error PASSED
		test_gb_eyedro.py::test_api_get_user_info_as_df_parse_error PASSED
		test_gb_eyedro.py::test_api_get_user_info_as_df_api_error_in_response PASSED
		test_gb_eyedro.py::test_db_create_tables_db_error z.test_123;
		PASSED
		test_gb_eyedro.py::test_db_create_tables_missing_index test_123
		XXXXXX', test_123, []
		YYYYYY' test_123 -1  <sqlalchemy.engine.cursor.CursorResult object at 0x0000024A54D2D470>
		ZZZZZZZ' test_123 -1
		PASSED
		test_gb_eyedro.py::test_db_create_gb_gaps_table_success PASSED
		test_gb_eyedro.py::test_db_create_gb_gaps_table_create_error
		        CREATE TABLE IF NOT EXISTS eyedro.gb_1min_gaps (
		            created_at TIMESTAMPTZ DEFAULT now(),
		            updated_at TIMESTAMPTZ DEFAULT now(),
		            hypertable_name TEXT,
		            epoch_secs BIGINT,
		            prev_epoch BIGINT,
		            diff_seconds INT,
		            days varchar(10),
		            start_ts TIMESTAMPTZ,
		            end_ts TIMESTAMPTZ,
		            deleted boolean DEFAULT false,
		            CONSTRAINT gb_gaps_epoch_secs_prev_epoch_key UNIQUE (hypertable_name, epoch_secs, prev_epoch, deleted)       
		        );
		        CREATE OR REPLACE FUNCTION eyedro.gb_1min_gaps()
		        RETURNS TRIGGER AS $$
		        BEGIN
		            NEW.updated_at = now();
		            NEW.start_ts = to_timestamp(NEW.prev_epoch::BIGINT);
		            NEW.end_ts = to_timestamp(NEW.epoch_secs::BIGINT);
		            RETURN NEW;
		        END;
		        $$ LANGUAGE plpgsql;
		        CREATE TRIGGER trigger__gb_1min_gaps
		        BEFORE INSERT OR UPDATE ON eyedro.gb_1min_gaps
		        FOR EACH ROW
		        EXECUTE FUNCTION eyedro.gb_1min_gaps();
		        SELECT column_name, data_type
		        FROM information_schema.columns
		        WHERE table_name = 'eyedro.gb_1min_gaps';

		PASSED
		test_gb_eyedro.py::test_db_get_gb_hypertables_success PASSED
		test_gb_eyedro.py::test_db_hyper_gb_gaps_db_error Database connection error: DB Error
		PASSED
		test_gb_eyedro.py::test_db_hyper_gb_gaps_general_error Unexpected error: General Error
		PASSED
		test_gb_eyedro.py::test_db_get_all_gb_gaps_success head
		✅ Data gaps found in 0 rows. Results saved to 'eyedro_data_gaps.csv'.
		PASSED
		test_utils.py::test_is_wsl_true_distro_name PASSED
		test_utils.py::test_is_wsl_true_interop PASSED
		test_utils.py::test_is_wsl_true_platform PASSED
		test_utils.py::test_is_wsl_false PASSED
		test_utils.py::test_is_running_on_azure_true PASSED
		test_utils.py::test_is_running_on_azure_false PASSED
		test_utils.py::test_is_linux_true PASSED
		test_utils.py::test_is_linux_false_wsl PASSED
		test_utils.py::test_is_ubuntu_true PASSED
		test_utils.py::test_is_ubuntu_false PASSED
		test_utils.py::test_is_ubuntu_file_not_found PASSED
		test_utils.py::test_show_dropdown_from_directory_success PASSED
		test_utils.py::test_show_dropdown_from_directory_invalid_dir PASSED
		test_utils.py::test_show_dropdown_from_directory_no_files PASSED
		test_utils.py::test_msgbox_yes_no_yes PASSED
		test_utils.py::test_msgbox_yes_no_no PASSED
		test_utils.py::test_config_log_handler PASSED
		test_utils.py::test_log_setup_defaults PASSED
		test_utils.py::test_log_setup_debug_env_var PASSED
		test_utils.py::test_log_setup_invalid_level PASSED
		test_utils.py::test_log_setup_already_configured PASSED
		test_utils.py::test_ts2Epoch[2023-10-27T10:30:00-0-1698402600] PASSED
		test_utils.py::test_ts2Epoch[2023-10-27T10:30:00-2-1698395400] PASSED
		test_utils.py::test_ts2Epoch[2023-10-27T10:30:00--1-1698406200] PASSED
		test_utils.py::test_ts2Epoch[1970-01-01T00:00:00-0-0] PASSED
		test_utils.py::test_filter_nested_dict PASSED
		test_utils.py::test_filter_nested_dict_different_value PASSED
		test_utils.py::test_filter_nested_dict_empty PASSED
		test_utils.py::test_filter_nested_dict_non_dict_list PASSED
		test_utils.py::test_create_cmdline_parser_args_provided 2025-05-03 12:11:45,489 - DEBUG - ['script.py', '--log', 'DEBUG', '--env', 'prod.env']        ['script.py', '--log', 'DEBUG', '--env', 'prod.env']
		2025-05-03 12:11:45,489 - DEBUG - ['script.py', '--log', 'DEBUG', '--env', 'prod.env']        ['script.py', '--log', 'DEBUG', '--env', 'prod.env']
		PASSED
		test_utils.py::test_create_cmdline_parser_defaults 2025-05-03 12:11:45,491 - DEBUG - ['script.py']        ['script.py']  
		2025-05-03 12:11:45,491 - DEBUG - ['script.py']        ['script.py']
		PASSED
		test_utils.py::test_create_cmdline_parser_invalid_log 2025-05-03 12:11:45,492 - DEBUG - ['script.py', '--log', 'INVALID']        ['script.py', '--log', 'INVALID']
		2025-05-03 12:11:45,492 - DEBUG - ['script.py', '--log', 'INVALID']        ['script.py', '--log', 'INVALID']
		PASSED
		test_utils.py::test_str_to_float_or_zero[123.45-123.45_0] PASSED
		test_utils.py::test_str_to_float_or_zero[123.45-123.45_1] PASSED
		test_utils.py::test_str_to_float_or_zero[123-123.0_0] PASSED
		test_utils.py::test_str_to_float_or_zero[123-123.0_1] PASSED
		test_utils.py::test_str_to_float_or_zero[-5.0--5.0] PASSED
		test_utils.py::test_str_to_float_or_zero[0-0.0_0] PASSED
		test_utils.py::test_str_to_float_or_zero[0-0.0_1] PASSED
		test_utils.py::test_str_to_float_or_zero[invalid-0.0] PASSED
		test_utils.py::test_str_to_float_or_zero[None-0.0] PASSED
		test_utils.py::test_str_to_float_or_zero[value9-0.0] PASSED
		test_utils.py::test_get_module_version_success PASSED
		test_utils.py::test_get_module_version_failure PASSED
		test_utils.py::test_is_version_greater_or_equal[0.4.8-0.4.8-True] PASSED
		test_utils.py::test_is_version_greater_or_equal[0.4.8-0.4.6-True] PASSED
		test_utils.py::test_is_version_greater_or_equal[0.4.8-0.5.0-False] PASSED
		test_utils.py::test_is_version_greater_or_equal[1.0.0-0.9.9-True] PASSED
		test_utils.py::test_is_version_greater_or_equal[1.0.0-1.0.0-True] PASSED
		test_utils.py::test_is_version_greater_or_equal[1.0.0-1.0.1-False] PASSED
		test_utils.py::test_is_version_greater_or_equal[1.0.0-1.1.0-False] PASSED
		test_utils.py::test_is_version_greater_or_equal[1.1.0-1.0.10-True] PASSED
		test_utils.py::test_is_version_greater_or_equal_error 2025-05-03 12:11:45,527 - ERROR - get_module_version Error occurred: Some Error
		PASSED
		test_utils.py::test_extract_data_site_provided PASSED
		test_utils.py::test_extract_data_site_provided_with_label PASSED
		test_utils.py::test_extract_data_site_none PASSED
		test_utils.py::test_extract_data_site_not_found PASSED
		test_utils.py::test_extract_data_empty_list PASSED
		test_utils.py::test_concat_csv_files_create_new 2025-05-03 12:11:45,536 - DEBUG - CSV files merged and appended if existing!
		PASSED
		test_utils.py::test_concat_csv_files_append 2025-05-03 12:11:45,538 - DEBUG - CSV files merged and appended if existing! 
		PASSED
		test_utils.py::test_is_port_in_use_true PASSED
		test_utils.py::test_is_port_in_use_false PASSED
		test_utils.py::test_prospect_running_partial PASSED
		test_utils.py::test_prospect_running_all PASSED
		test_utils.py::test_prospect_running_none PASSEDE:\_UNHCR\CODE\unhcr_module\vfedot\Lib\site-packages\coverage\report_core.py:110: CoverageWarning: Couldn't parse Python file 'E:\_UNHCR\CODE\unhcr_module\unhcr\db copy.py' (couldnt-parse)      
		  coverage._warn(msg, slug="couldnt-parse")


		---------- coverage: platform win32, python 3.12.10-final-0 ----------
		Coverage HTML written to dir htmlcov

		============================================ 156 passed, 2 skipped in 4.27s ============================================

## Notes

Update to v_0.4

Allow running from local module files, or installed files. Note that constants has to initially be loaded from the installed version -- so install package before running the code locally

Find .env file automatically

Run using local files for development --- You must set PROD=1 in .env file before releasing to production

Default to INFO logging level if bad commandline args

Seperate code logically into modules --- move some misc logic into test code --- might put in misc module

Break apart API module for Leonics and Prospect -- TODO -- same for db module

Logging goes to **_UNHCR\CODE\unhcr_module\unhcr.module.log**

If you set the log level to **DEBUG** your will get CSV files.

There is a bug when there are no new records:

	E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\unhcr\db.py:144: FutureWarning: Setting an item of incompatible dtype is deprecated and will raise in a future error of pandas. Value '['2025-01-08 21:26']' has dtype incompatible with datetime64[ns], please explicitly cast to a compatible dtype first.

	df_filtered.loc[:, 'DatetimeServer'] = df_filtered['DatetimeServer'].dt.strftime('%Y-%m-%d %H:%M')

	Traceback (most recent call last):

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\engine\base.py", line 1967, in exec_single_context

	self.dialect.do_execute(

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\engine\default.py", line 941, in do_execute

	cursor.execute(statement, parameters)

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\MySQLdb\cursors.py", line 179, in execute

	res = self._query(mogrified_query)

	^^^^^^^^^^^^^^^^^^^^^^^^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\MySQLdb\cursors.py", line 330, in _query

	db.query(q)

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\MySQLdb\connections.py", line 265, in query

	_mysql.connection.query(self, query)

	MySQLdb.ProgrammingError: (1064, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '21:26)' at line 1")

	  

	The above exception was the direct cause of the following exception:

	  

	Traceback (most recent call last):

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\unhcr\db.py", line 104, in update_mysql

	update_rows(max_dt, token)

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\unhcr\db.py", line 166, in update_rows

	res = mysql_execute(sql_query)

	^^^^^^^^^^^^^^^^^^^^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\unhcr\db.py", line 76, in mysql_execute

	raise e

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\unhcr\db.py", line 71, in mysql_execute

	result = session.execute(text(sql), {"data": data})

	^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\orm\session.py", line 2362, in execute

	return self._execute_internal(

	^^^^^^^^^^^^^^^^^^^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\orm\session.py", line 2256, in _execute_internal

	result = conn.execute(

	^^^^^^^^^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\engine\base.py", line 1418, in execute

	return meth(

	^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\sql\elements.py", line 515, in _execute_on_connection

	return connection._execute_clauseelement(

	^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\engine\base.py", line 1640, in _execute_clauseelement

	ret = self._execute_context(

	^^^^^^^^^^^^^^^^^^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\engine\base.py", line 1846, in_execute_context

	return self._exec_single_context(

	^^^^^^^^^^^^^^^^^^^^^^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\engine\base.py", line 1986, in_exec_single_context

	self._handle_dbapi_exception(

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\engine\base.py", line 2355, in _handle_dbapi_exception

	raise sqlalchemy_exception.with_traceback(exc_info[2]) from e

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\engine\base.py", line 1967, in_exec_single_context

	self.dialect.do_execute(

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\sqlalchemy\engine\default.py", line 941, in do_execute

	cursor.execute(statement, parameters)

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\MySQLdb\cursors.py", line 179, in execute

	res = self._query(mogrified_query)

	^^^^^^^^^^^^^^^^^^^^^^^^^^^^

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\MySQLdb\cursors.py", line 330, in_query

	db.query(q)

	File "E:\_UNHCR\CODE\unhcr_module\.venv\Lib\site-packages\MySQLdb\connections.py", line 265, in query

	_mysql.connection.query(self, query)

	sqlalchemy.exc.ProgrammingError: (MySQLdb.ProgrammingError) (1064, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '21:26)' at line 1")

	[SQL: INSERT INTO TAKUM_LEONICS_API_RAW (BDI1_ACinput_P1_kW, BDI1_ACinput_P2_kW, BDI1_ACinput_P3_kW, BDI1_ACinput_Total_kW, BDI1_ACinput_Voltage_L1, BDI1_ACinput_Voltage_L2, BDI1_ACinput_Voltage_L3, BDI1_Batt_Voltage, BDI1_Freq, BDI1_Power_P1_kW, BDI1_Power_P2_kW, BDI1_Power_P3_kW, BDI1_Todate_Supply_AC_kWh, BDI1_Today_Supply_AC_kWh, BDI1_Total_Power_kW, BDI2_ACinput_P1_kW, BDI2_ACinput_P2_kW, BDI2_ACinput_P3_kW, BDI2_ACinput_Total_kW, BDI2_ACinput_Voltage_L1, BDI2_ACinput_Voltage_L2, BDI2_ACinput_Voltage_L3, BDI2_Freq, BDI2_Power_P1_kW, BDI2_Power_P2_kW, BDI2_Power_P3_kW, BDI2_Todate_Batt_Chg_kWh, BDI2_Todate_Batt_DisChg_kWh, BDI2_Today_Batt_Chg_kWh, BDI2_Today_Batt_DisChg_kWh, BDI2_Total_Power_kW, DCgen_Alternator_Current, DCgen_Alternator_Power_kW, DCgen_Alternator_Temp, DCgen_Alternator_Voltage, DCgen_Ambient_Temp, DCgen_Coolant_Temp, DCgen_Diode_Temp, DCgen_Engine_Runtime, DCgen_Fuel_Level, DCgen_High_Voltage_Stop, DCgen_LoadBattery_Current, DCgen_LoadBattery_Power_kW, DCgen_LoadBattery_Voltage, DCgen_Low_Current_Stop, DCgen_Low_Voltage_Start, DCgen_Max_Current, DCgen_Max_RPM, DCgen_Max_Voltage, DCgen_Min_RPM, DCgen_Oil_Pressure, DCgen_RPM, DCgen_RPM_Frequency, DCgen_StartingBatteryVoltage, DCgen_Throttle_Stop, DCgen_Today_kWh, DCgen_Total_kWh, FlowMeter_Fuel_Temp, FlowMeter_Hourly_Fuel_consumptionRate, FlowMeter_Today_Fuel_consumption, FlowMeter_Total_Fuel_consumption, HVB1_Avg_V, HVB1_Batt_I, HVB1_SOC, In1_BDI_Fail, In2_ATS_Status, In3_door_sw, In4, In5, In6, In7, In8, LoadPM_Import_kWh, LoadPM_Power_P1_kW, LoadPM_Power_P2_kW, LoadPM_Power_P3_kW, LoadPM_Today_Import_kWh, LoadPM_Total_P_kW, Out1_CloseMC1, Out2_StartGen, Out3_EmergencyStop, Out4, Out5, Out6, Out7, Out8, SCC1_Chg_Current, SCC1_Chg_Power_kW, SCC1_Chg_Voltage, SCC1_PV_Current, SCC1_PV_Power_kW, SCC1_PV_Voltage, SCC1_Todate_Chg_kWh, SCC1_Todate_PV_kWh, SCC1_Today_Chg_kWh, SCC1_Today_PV_kWh, ana1_Inv_Room_Temp, ana2_Inv_room_RH, ana3_Batt_Room_Temp, ana4_Batt_room_RH, ana5_Fuel_Level1, ana6_Fuel_Level2, DatetimeServer) VALUES (0.0, 0.0, 0.0, 0.0, 0.6, 0.5, 0.4, 267.6, 50.0, 2.9, 1.3, 1.2, 15025.0, 136.0, 5.4, 0.0, 0.0, 0.0, 0.0, 0.2, 0.2, 0.7, 47.35, 0.0, 0.0, 0.0, 2736.52, 4261.7, 74.06, 68.74, 0.0, 64.17, 17.23, 77.38, 268.52, 84.68, 84.68, 51.08, 727.0, 24.37, 268.0, 64.17, 0.0, 0.0, 48.0, 260.0, 133.0, 28.0, 269.5, 6.5, 653.65, 22.8, 0.0, 13.66, 0.0, 139.0, 9342.0, NULL, NULL, 0.0, NULL, 267.6, 42.5, 86.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 16027.4, 2.96, 1.39, 1.12, 137.66, 5.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.3, 0.07, 268.6, 0.5, 0.02, 44.9, 1231.4, 1208.6, 11.0, 10.5, 30.4, 21.4, 13.5, 53.8, 8.6, 3.9, 2025-01-08 21:26);]

	(Background on this error at: <https://sqlalche.me/e/20/f405>)

	2025-01-08 14:28:11,949 - ERROR - ?????????????????update_mysql Error occurred: (MySQLdb.ProgrammingError) (1064, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '21:26)' at line 1")

	[SQL: INSERT INTO TAKUM_LEONICS_API_RAW (BDI1_ACinput_P1_kW, BDI1_ACinput_P2_kW, BDI1_ACinput_P3_kW, BDI1_ACinput_Total_kW, BDI1_ACinput_Voltage_L1, BDI1_ACinput_Voltage_L2, BDI1_ACinput_Voltage_L3, BDI1_Batt_Voltage, BDI1_Freq, BDI1_Power_P1_kW, BDI1_Power_P2_kW, BDI1_Power_P3_kW, BDI1_Todate_Supply_AC_kWh, BDI1_Today_Supply_AC_kWh, BDI1_Total_Power_kW, BDI2_ACinput_P1_kW, BDI2_ACinput_P2_kW, BDI2_ACinput_P3_kW, BDI2_ACinput_Total_kW, BDI2_ACinput_Voltage_L1, BDI2_ACinput_Voltage_L2, BDI2_ACinput_Voltage_L3, BDI2_Freq, BDI2_Power_P1_kW, BDI2_Power_P2_kW, BDI2_Power_P3_kW, BDI2_Todate_Batt_Chg_kWh, BDI2_Todate_Batt_DisChg_kWh, BDI2_Today_Batt_Chg_kWh, BDI2_Today_Batt_DisChg_kWh, BDI2_Total_Power_kW, DCgen_Alternator_Current, DCgen_Alternator_Power_kW, DCgen_Alternator_Temp, DCgen_Alternator_Voltage, DCgen_Ambient_Temp, DCgen_Coolant_Temp, DCgen_Diode_Temp, DCgen_Engine_Runtime, DCgen_Fuel_Level, DCgen_High_Voltage_Stop, DCgen_LoadBattery_Current, DCgen_LoadBattery_Power_kW, DCgen_LoadBattery_Voltage, DCgen_Low_Current_Stop, DCgen_Low_Voltage_Start, DCgen_Max_Current, DCgen_Max_RPM, DCgen_Max_Voltage, DCgen_Min_RPM, DCgen_Oil_Pressure, DCgen_RPM, DCgen_RPM_Frequency, DCgen_StartingBatteryVoltage, DCgen_Throttle_Stop, DCgen_Today_kWh, DCgen_Total_kWh, FlowMeter_Fuel_Temp, FlowMeter_Hourly_Fuel_consumptionRate, FlowMeter_Today_Fuel_consumption, FlowMeter_Total_Fuel_consumption, HVB1_Avg_V, HVB1_Batt_I, HVB1_SOC, In1_BDI_Fail, In2_ATS_Status, In3_door_sw, In4, In5, In6, In7, In8, LoadPM_Import_kWh, LoadPM_Power_P1_kW, LoadPM_Power_P2_kW, LoadPM_Power_P3_kW, LoadPM_Today_Import_kWh, LoadPM_Total_P_kW, Out1_CloseMC1, Out2_StartGen, Out3_EmergencyStop, Out4, Out5, Out6, Out7, Out8, SCC1_Chg_Current, SCC1_Chg_Power_kW, SCC1_Chg_Voltage, SCC1_PV_Current, SCC1_PV_Power_kW, SCC1_PV_Voltage, SCC1_Todate_Chg_kWh, SCC1_Todate_PV_kWh, SCC1_Today_Chg_kWh, SCC1_Today_PV_kWh, ana1_Inv_Room_Temp, ana2_Inv_room_RH, ana3_Batt_Room_Temp, ana4_Batt_room_RH, ana5_Fuel_Level1, ana6_Fuel_Level2, DatetimeServer) VALUES (0.0, 0.0, 0.0, 0.0, 0.6, 0.5, 0.4, 267.6, 50.0, 2.9, 1.3, 1.2, 15025.0, 136.0, 5.4, 0.0, 0.0, 0.0, 0.0, 0.2, 0.2, 0.7, 47.35, 0.0, 0.0, 0.0, 2736.52, 4261.7, 74.06, 68.74, 0.0, 64.17, 17.23, 77.38, 268.52, 84.68, 84.68, 51.08, 727.0, 24.37, 268.0, 64.17, 0.0, 0.0, 48.0, 260.0, 133.0, 28.0, 269.5, 6.5, 653.65, 22.8, 0.0, 13.66, 0.0, 139.0, 9342.0, NULL, NULL, 0.0, NULL, 267.6, 42.5, 86.4, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 16027.4, 2.96, 1.39, 1.12, 137.66, 5.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.3, 0.07, 268.6, 0.5, 0.02, 44.9, 1231.4, 1208.6, 11.0, 10.5, 30.4, 21.4, 13.5, 53.8, 8.6, 3.9, 2025-01-08 21:26);]

	(Background on this error at: <https://sqlalche.me/e/20/f405>)