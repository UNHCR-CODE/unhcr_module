import pytest
import psycopg2
import requests
import json
import logging
logging.basicConfig(level=logging.DEBUG)
import traceback
import inspect

from unhcr import err_handler

# Assuming the code with err_details and error_wrapper is already imported.


@pytest.mark.parametrize("exception, expected_msg", [
    (psycopg2.OperationalError("Database connection error"), "Database connection error"),
    (psycopg2.DatabaseError("Database query error"), "Database query error"),
    (psycopg2.InterfaceError("Database interface error"), "Database interface error"),
    (psycopg2.ProgrammingError("Database programming error"), "Database programming error"),
    (psycopg2.DataError("Data error"), "Data error"),
    (MemoryError("Memory error"), "Memory error"),
    (ValueError("Invalid response ERROR"), "Invalid response ERROR"),
    (json.JSONDecodeError("Error decoding JSON ERROR", "", 0), "Error decoding JSON ERROR"),
    (TypeError("Error with type conversion"), "Error with type conversion in serial processing ERROR"),
    (IndexError("Error with string slicing"), "Error with string slicing in serial processing ERROR"),
    (KeyError("Missing expected key"), "Missing expected key in data structure ERROR"),
    (requests.exceptions.HTTPError("HTTP ERROR"), "HTTP ERROR"),
    (requests.exceptions.ConnectionError("Connection Error"), "Connection Error"),
    (requests.exceptions.Timeout("Timeout Error"), "Timeout Error"),
    (requests.exceptions.RequestException("Request Error"), "Request Error"),
    (Exception("Unexpected error"), "Unexpected error"),
])
def test_err_details(exception, expected_msg):
    def func():
        raise exception
    
    _, result = err_handler.error_wrapper(func)
    assert expected_msg in result
    logging.debug(result)

