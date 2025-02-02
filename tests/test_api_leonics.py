"""
Tests for api_leonics.py
"""

import logging
from typing import Tuple
import pytest
import pandas as pd
from datetime import date
import requests
from unittest.mock import Mock

from unhcr import api_leonics, constants as const

# Arrange
TEST_TOKEN = "test_token"
TEST_BASE_URL = "https://test_leonics_url.com"  # Replace with a dummy URL


# Mock constants for testing
@pytest.fixture(autouse=True)
def mock_constants(monkeypatch):
    """
    Mock constants for testing.

    This fixture sets the following constants to dummy values for testing:

    - `const.LEONICS_BASE_URL` is set to `TEST_BASE_URL`.
    - `const.VERIFY` is set to `False` to disable SSL verification for testing.

    This fixture is marked as `autouse=True`, so it will be automatically applied
    to all tests in this module.
    """
    monkeypatch.setattr(const, "LEONICS_BASE_URL", TEST_BASE_URL)
    monkeypatch.setattr(const, "VERIFY", False)  # Disable SSL verification for testing


@pytest.mark.parametrize(
    "dt, expected_payload",
    [
        (
            date(2024, 8, 22),
            {
                "SystemCode": "LEONICS",
                "CurrentDate": "2024-08-22",
                "SiteId": "unhcr-001",
                "UserCode": const.LEONICS_USER_CODE,
                "Key": const.LEONICS_KEY,
            },
        ),
        (
            None,  # Test with dt=None (defaults to today)
            {
                "SystemCode": "LEONICS",
                "CurrentDate": date.today().isoformat(),
                "SiteId": "unhcr-001",
                "UserCode": const.LEONICS_USER_CODE,
                "Key": const.LEONICS_KEY,
            },
        ),
    ],
    ids=["specific_date", "default_today"],
)
def test_getAuthToken(dt, expected_payload, monkeypatch):
    # Arrange
    """
    Tests the getAuthToken function.

    This test uses the pytest.mark.parametrize decorator to run the same test
    function with different inputs. The inputs are:

    - dt: a datetime.date object specifying the date to use for authentication.
    - expected_payload: a dictionary containing the expected payload for the
      authentication request.

    The test function:

    - Sets up a mock response object with a status code of 200 and a text
      response of "KEY: " + TEST_TOKEN.
    - Uses the monkeypatch fixture to replace the requests.post function with a
      lambda function that returns the mock response object.
    - Calls the getAuthToken function with the given dt parameter.
    - Asserts that the status code of the response is 200 and the text response
      matches the expected value.

    The test is run twice, once with dt set to a specific date (2024-08-22) and
    once with dt set to None (which defaults to the current date).
    """
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "KEY: " + TEST_TOKEN  # Mock the text response directly

    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: mock_response)

    # Act
    response = api_leonics.getAuthToken(dt)

    # Assert
    assert response.status_code == 200
    assert response.text == "KEY: " + TEST_TOKEN  # Check the text directly


@pytest.mark.parametrize(
    "auth_status_code, check_auth_status_code, expected_token",
    [
        (200, 200, TEST_TOKEN),  # auth_and_check_auth_success
        # (401, 200, None),  # auth_fail_401
        # (200, 401, None),  # check_auth_fail_401
        # (500, 200, None),  # auth_fail_500
    ],
    ids=[
        "auth_and_check_auth_success",
        # "auth_fail_401",
        # "check_auth_fail_401",
        # "auth_fail_500",
    ],
)
def test_checkAuth(
    monkeypatch, auth_status_code, check_auth_status_code, expected_token
):
    # Arrange
    """
    Tests the checkAuth function.

    This test uses the pytest.mark.parametrize decorator to run the same test
    function with different inputs. The inputs are:

    - auth_status_code: the status code of the authentication response.
    - check_auth_status_code: the status code of the check_auth response.
    - expected_token: the expected token returned by the checkAuth function.

    The test function:

    - Sets up mock response objects for the authentication and check_auth
      requests.
    - Uses the monkeypatch fixture to replace the requests.post and
      requests.request functions with the mock response objects.
    - Calls the checkAuth function.
    - Asserts that the returned token matches the expected token.

    The test is run multiple times with different inputs.
    """

    class MockResponse:
        def __init__(self, status_code, text=None):
            self.status_code = status_code
            self.text = text

    def mock_auth(*args, **kwargs):
        """
        Mocks the authentication request.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Mock or MockResponse: A mock response object with the specified status
            code and text. If the auth_status_code is 200, returns a Mock object
            with status code 200 and a text containing the API key. Otherwise,
            returns a MockResponse with the given auth_status_code.
        """

        if auth_status_code == 200:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = '"API-KEY: ' + TEST_TOKEN + '"'  # Mock text response
            return mock_response
        else:
            return MockResponse(auth_status_code)

    def mock_check_auth(*args, **kwargs):
        """
        Mocks the check_auth request.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            MockResponse: A mock response object with the specified status code
            and an empty string as text response.
        """
        return MockResponse(check_auth_status_code, "")

    monkeypatch.setattr(requests, "post", mock_auth)
    monkeypatch.setattr(requests, "request", mock_check_auth)

    # Act
    token = api_leonics.checkAuth()

    # Assert
    assert token == expected_token


@pytest.mark.parametrize(
    "start, end, token, api_status_code, expected_df, expected_error",
    [
        (
            "20240820",
            "20240821",
            TEST_TOKEN,
            200,
            pd.DataFrame(
                {
                    "BDI1_Power_P1_kW": [10.5],
                    "DatetimeServer": ["2024-08-20 00:00"],
                }
            ),
            None,
        ),  # happy_path
        (
            "20240820",
            "20240821",
            None,
            200,
            None,
            "You must provide a token",
        ),  # no_token
        (
            "20240820",
            "20240821",
            TEST_TOKEN,
            404,
            None,
            404,
        ),  # api_error_404
    ],
    ids=["happy_path", "no_token", "api_error_404"],
)
def test_getData(
    monkeypatch, start, end, token, api_status_code, expected_df, expected_error
):
    # Arrange
    """
    Tests the getData function of the API.

    The test function uses the parametrize decorator to run the same test
    function with different parameters. The parameters are the start date,
    end date, token, API status code, expected DataFrame, and expected error.

    The test function first sets up a mock response for the API request,
    then calls the getData function and asserts that the returned DataFrame
    and error match the expected values.

    The test function is run three times with the following parameters:

    - happy_path: The test is run with a valid token and a successful API
      request. The expected result is a DataFrame with the power data for
      the specified date range.
    - no_token: The test is run with no token and a successful API request.
      The expected result is an error message.
    - api_error_404: The test is run with a valid token and an API request
      that returns a 404 status code. The expected result is an error message
      with the status code.
    """

    class MockResponse:
        def __init__(self, status_code, json_data):
            """
            Initializes the MockResponse object with a given status code and JSON data.

            Args:
                status_code (int): The HTTP status code for the response.
                json_data (dict): The JSON data to be returned by the response.
            """

            self.status_code = status_code
            self._json = json_data

        def json(self):
            return self._json

    def mock_request_func(*args, **kwargs):
        """
        Mocks the API request and returns a MockResponse object.

        This function simulates an API request and returns a response
        based on the provided arguments. If the token is None, it returns
        a MockResponse with the given API status code and an empty JSON response.
        If the API status code is 200, it returns a MockResponse with the
        status code and a JSON response containing server date, time, and power data.
        Otherwise, it returns a MockResponse with the specified status code
        and an empty JSON response.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            MockResponse: A mock response object with the specified status
            code and JSON data.
        """

        if token is None:
            return MockResponse(api_status_code, {})
        elif api_status_code == 200:
            return MockResponse(
                api_status_code,
                [
                    {
                        "A_DateServer": "2024-08-20",
                        "A_TimeServer": "00:00",
                        "BDI1_Power_P1_kW": 10.5,
                    }
                ],
            )
        else:
            return MockResponse(api_status_code, {})

    monkeypatch.setattr(requests, "request", mock_request_func)

    # Act
    df, error = api_leonics.getData(start, end, token)

    # Assert
    if expected_df is not None:
        pd.testing.assert_frame_equal(df, expected_df)
    assert error == expected_error
