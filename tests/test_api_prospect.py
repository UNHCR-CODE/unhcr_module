import pytest
import requests
import pandas as pd
from unhcr import constants as const
from unhcr.api_prospect import (
    get_prospect_url_key,
    api_in_prospect,
    get_prospect_last_data,
)


# Mocking requests.request to avoid actual API calls during testing
@pytest.fixture
def mock_request(monkeypatch):
    """
    Mocks the requests.request method to return a MockResponse object.

    Returns a fixture that replaces the requests.request method with a mock
    object that returns a MockResponse with a status code of 200 and a JSON
    response containing a single data point with a custom field "DatetimeServer"
    set to "2024-08-15T12:00:00Z" and an "external_id" set to "123".
    """

    class MockResponse:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

    def mock_request_func(*args, **kwargs):
        return MockResponse(
            200,
            '{"data": [{"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}',
        )

    monkeypatch.setattr(requests, "request", mock_request_func)


@pytest.mark.parametrize(
    "local, out, expected_url, expected_key",
    [
        (True, False, const.LOCAL_BASE_URL, const.LOCAL_API_IN_KEY),  # local_in
        (True, True, const.LOCAL_BASE_URL, const.LOCAL_API_OUT_KEY),  # local_out
        (False, False, const.BASE_URL, const.API_IN_KEY),  # external_in
        (False, True, const.BASE_URL, const.API_OUT_KEY),  # external_out
    ],
    ids=["local_in", "local_out", "external_in", "external_out"],
)
def test_get_prospect_url_key(local, out, expected_url, expected_key):
    """
    Tests the get_prospect_url_key function with different parameter combinations.

    This test case exercises the get_prospect_url_key function with different
    combinations of the local and out parameters. It verifies that the function
    returns the expected URL and API key for each combination.

    Parameters:
        local (bool): Indicates whether to retrieve data from the local or external
            Prospect API.
        out (bool): Indicates whether to retrieve the outgoing API key.
        expected_url (str): The expected URL returned by the function.
        expected_key (str): The expected API key returned by the function.
    """
    url, key = get_prospect_url_key(local, out)

    # Assert
    assert url == expected_url
    assert key == expected_key


@pytest.mark.parametrize(
    "df, local, status_code, expected_response",
    [
        (
            pd.DataFrame({"a": [1]}),
            True,
            200,
            '{"data": [{"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}',
        ),  # happy_path_local
        (
            pd.DataFrame({"a": [1]}),
            False,
            200,
            '{"data": [{"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}',
        ),  # happy_path_external
        (None, True, None, None),  # empty_dataframe_local
        (None, False, None, None),  # empty_dataframe_external
    ],
    ids=[
        "happy_path_local",
        "happy_path_external",
        "empty_dataframe_local",
        "empty_dataframe_external",
    ],
)
def test_api_in_prospect(
    monkeypatch, df, local, status_code, expected_response, mock_request
):
    """
    Tests the api_in_prospect function with different parameter combinations and
    responses from the Prospect API.

    This test case exercises the api_in_prospect function with different
    combinations of the df and local parameters, and verifies that the function
    returns the expected response when the Prospect API returns a valid JSON
    response. It also tests that the function returns None when the Prospect API
    returns an invalid response.

    Parameters:
        df (pd.DataFrame): The input DataFrame to be sent to the Prospect API.
        local (bool): Indicates whether to use the local or external Prospect API.
        status_code (int): The expected status code of the response from the
            Prospect API.
        expected_response (str): The expected JSON response from the Prospect API.
    """

    class MockResponse:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

    def mock_request_func(*args, **kwargs):
        return MockResponse(
            status_code if status_code is not None else 200,
            expected_response if expected_response is not None else "",
        )

    monkeypatch.setattr(
        requests,
        "request",
        mock_request_func if status_code is not None else mock_request,
    )

    # Act
    response = api_in_prospect(df, local)

    # Assert
    if status_code is None:
        assert response is None
    else:
        assert response.status_code == status_code
        if expected_response:
            assert response.text == expected_response


@pytest.mark.parametrize(
    "response_text, expected_timestamp",
    [
        (
            '{"data": [{"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}',
            "2024-08-15T12:00:00Z",
        ),  # valid_response
        ('{"data": []}', ""),  # empty_data
        (
            '{"data": [{"custom": {"DatetimeServer": "2024-08-16T12:00:00Z"}, "external_id": "456"}, {"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}',
            "2024-08-16T12:00:00Z",
        ),  # multiple_entries
        ("{}", ""),  # invalid_json_no_data
        ('{"data": [{"external_id": "123"}]}', ""),  # missing_timestamp
    ],
    ids=[
        "valid_response",
        "empty_data",
        "multiple_entries",
        "invalid_json_no_data",
        "missing_timestamp",
    ],
)
def test_get_prospect_last_data(response_text, expected_timestamp):
    """
    Tests the get_prospect_last_data function with different responses from the Prospect API.

    This test case exercises the get_prospect_last_data function with different
    responses from the Prospect API, and verifies that the function returns the
    expected timestamp when the response is valid and contains a single data point
    with a custom field "DatetimeServer", and returns an empty string otherwise.

    Parameters:
        response_text (str): The JSON response from the Prospect API.
        expected_timestamp (str): The expected timestamp returned by the function.
    """
    response = requests.Response()
    response.status_code = 200
    response._content = str.encode(response_text)

    # Act
    timestamp = get_prospect_last_data(response)
    print("#################", timestamp, expected_timestamp)
    # Assert
    assert timestamp == expected_timestamp
