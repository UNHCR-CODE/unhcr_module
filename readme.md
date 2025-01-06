# UNHCR Data Integration Module

This module facilitates the integration of data from the Leonics API into UNHCR's systems, specifically a MySQL database, and the Prospect system.  It handles authentication with the Leonics API, data retrieval, filtering, and updates to both target systems.

## Key Features

* **Automated Data Retrieval:**  Fetches data from the Leonics API within a specified timeframe, using an authentication token.
* **Incremental Updates:**  Updates both the MySQL database and Prospect with only new records since the last update, minimizing data transfer and redundancy.
* **Data Filtering:** Cleans and preprocesses the data by removing placeholder values, ensuring data quality.
* **Error Handling and Logging:** Includes logging for monitoring and debugging purposes, as well as error handling mechanisms.
* **Configuration via Environment Variables:** Sensitive data such as API keys and connection strings are stored securely using environment variables.

## Module Structure

The module is organized into several Python files:

* **`api.py`:** Handles interaction with the Leonics and Prospect APIs, including authentication and data retrieval/submission.
* **`db.py`:** Manages database operations, including updates to the MySQL database and interaction with Prospect.
* **`s3.py`:** Provides functionality for interacting with AWS S3 storage (currently used for listing files).
* **`utils.py`:** Contains utility functions for data cleaning and processing, such as filtering specific values from nested data structures.
* **`constants.py`:** Stores configuration constants such as API endpoints, database connection strings, and S3 credentials, loaded from a `.env` file.
* **`__main__.py`:** Entry point for the module when executed directly.
* **`__init__.py`:** Initializes the module and makes its functions accessible.

## Installation

For demonstration purposes I am using Windows and installing under: **_UNHCR\CODE**

1. **Clone the repository:**
   
    ```bash
    git clone https://github.com/unhcr-smh/unhcr_module.git
    cd _UNHCR\CODE\unhcr_module

2. **Create a virtual environment:**
   
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate

3. Install the module
   
    ```bash
    pip install .

4. Edit the run.bat file if your absolute path is different
   
    ```batch
    REM change to your venv path if necessary
    call E:\_UNHCR\CODE\unhcr_module\.venv\Scripts\activate.bat
    python E:\_UNHCR\CODE\unhcr_module\unhcr\full_test.py --log INFO

6. Create a **.env** file in the project root directory. You need to get the secrets and API keys from Steve Hermes.
   Here is the **.env** without values:

    ```plaintext
    # Leonics API
    LEONICS_BASE_URL = ''
    LEONICS_USER_CODE = ''
    LEONICS_KEY = ''

    # Verify SSL --- note that leonic's cert does not verify
    VERIFY = False

    # Aiven Mysql DB
    AIVEN_TAKUM_CONN_STR = ''

    # Eyedro S3
    ACCESS_KEY = ''
    SECRET_KEY = ''
    BUCKET_NAME = ''
    FOLDER_NAME = ''

    # Prospect API
    PROS_BASE_URL = ''
    PROS_API_IN_KEY = ''
    PROS_API_OUT_KEY = ''

    # if you're running a local instance of Prospect
    PROS_LOCAL_BASE_URL = ''
    PROS_LOCAL_API_IN_KEY = ''
    PROS_LOCAL_API_OUT_KEY = ''

7. Execute **run.bat** The output should be similar:

    ```cmd
    _UNHCR\CODE\unhcr_module>run

    E:\_UNHCR\CODE\unhcr_module>REM change to your venv path

    E:\_UNHCR\CODE\unhcr_module>call E:\_UNHCR\CODE\unhcr_module\.venv\Scripts\activate.bat
    2025-01-06 13:41:47,541 - INFO - Process ID: 46944 Logging level: INFO
    2025-01-06 13:41:47,543 - INFO - Getting auth token for date: 2025-01-06
    2025-01-06 13:41:49,677 - INFO - Getting auth token for date: 2025-01-07
    2025-01-06 13:41:51,766 - INFO - {
    "API ver.": "v1.25",
    "Pass": "Authentication successful"
    }

    2025-01-06 13:42:04,307 - INFO - ROWS UPDATED: TAKUM_LEONICS_API_RAW  18
    2025-01-06 13:42:04,308 - INFO - Starting update_prospect ts: 2090-11-14 01:52  local = True
    2025-01-06 13:42:05,813 - INFO - 

    74a1680a57a88fbd56e4af59576d7d3e
    http://localhost:3000/api/v1/out/custom/?size=50&page=1&q[source_id_eq]=1&q[s]=created_at+desc
    2025-01-06 20:22
    2025-01-06 13:42:12,245 - INFO - 201:  {"id":503,"created_at":"2025-01-06T19:42:12.136Z","status":"Import started"}
    2025-01-06 13:42:12,245 - INFO - Data has been saved to 'py_pros'
    2025-01-06 13:42:12,246 - INFO - Starting update_prospect ts: 2090-11-14 01:48  local = False
    2025-01-06 13:42:12,997 - INFO - 

    c91c1da9b43658800b079d52953d83f7
    https://app.prospect.energy/api/v1/out/custom/?size=50&page=1&q[source_id_eq]=421&q[s]=created_at+desc
    2025-01-06 20:22
    2025-01-06 13:42:19,566 - INFO - 201:  {"id":4935699,"created_at":"2025-01-06T19:42:19.550Z","status":"Import started"}
    2025-01-06 13:42:19,567 - INFO - Data has been saved to 'py_pros'

## Notes

Logging goes to **_UNHCR\CODE\unhcr_module\unhcr.module.log**

If you set the log level to **DEBUG** your will get CSV files.
