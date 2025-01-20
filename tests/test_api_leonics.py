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
    class MockResponse:
        def __init__(self, status_code, text=None):
            self.status_code = status_code
            self.text = text

    def mock_auth(*args, **kwargs):
        if auth_status_code == 200:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = '"API-KEY: ' + TEST_TOKEN + '"' # Mock text response
            return mock_response
        else:
            return MockResponse(auth_status_code)

    def mock_check_auth(*args, **kwargs):
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
                    "DateTimeServer": ["2024-08-20 00:00"],
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
    class MockResponse:
        def __init__(self, status_code, json_data):
            self.status_code = status_code
            self._json = json_data

        def json(self):
            return self._json

    def mock_request_func(*args, **kwargs):
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

