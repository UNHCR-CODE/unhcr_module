import pytest
import requests
import pandas as pd
from unhcr import constants as const
from unhcr.api_prospect import get_prospect_url_key, api_in_prospect, get_prospect_last_data, prospect_get_start_ts

# Mocking requests.request to avoid actual API calls during testing
@pytest.fixture
def mock_request(monkeypatch):
    class MockResponse:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

    def mock_request_func(*args, **kwargs):
        return MockResponse(200, '{"data": [{"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}')

    monkeypatch.setattr(requests, 'request', mock_request_func)


@pytest.mark.parametrize(
    "local, out, expected_url, expected_key",
    [
        (True, False, const.LOCAL_BASE_URL, const.LOCAL_API_IN_KEY),  # local_in
        (True, True, const.LOCAL_BASE_URL, const.LOCAL_API_OUT_KEY),  # local_out
        (False, False, const.BASE_URL, const.API_IN_KEY),  # external_in
        (False, True, const.BASE_URL, const.API_OUT_KEY),  # external_out
    ],
    ids=["local_in", "local_out", "external_in", "external_out"]
)
def test_get_prospect_url_key(local, out, expected_url, expected_key):

    # Act
    url, key = get_prospect_url_key(local, out)

    # Assert
    assert url == expected_url
    assert key == expected_key


@pytest.mark.parametrize(
    "df, local, status_code, expected_response",
    [
        (pd.DataFrame({'a': [1]}), True, 200, '{"data": [{"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}'),  # happy_path_local
        (pd.DataFrame({'a': [1]}), False, 200, '{"data": [{"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}'),  # happy_path_external
        (None, True, None, None),  # empty_dataframe_local
        (None, False, None, None),  # empty_dataframe_external
    ],
    ids=["happy_path_local", "happy_path_external", "empty_dataframe_local", "empty_dataframe_external"]
)
def test_api_in_prospect(monkeypatch, df, local, status_code, expected_response, mock_request):
    # Mocking requests.request to simulate different responses

    class MockResponse:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

    def mock_request_func(*args, **kwargs):
        return MockResponse(status_code if status_code is not None else 200, expected_response if expected_response is not None else '')

    monkeypatch.setattr(requests, 'request', mock_request_func if status_code is not None else mock_request)

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
        ('{"data": [{"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}', "2024-08-15T12:00:00Z"),  # valid_response
        ('{"data": []}', ""),  # empty_data
        ('{"data": [{"custom": {"DatetimeServer": "2024-08-16T12:00:00Z"}, "external_id": "456"}, {"custom": {"DatetimeServer": "2024-08-15T12:00:00Z"}, "external_id": "123"}]}', "2024-08-16T12:00:00Z"),  # multiple_entries
        ('{}', ""),  # invalid_json_no_data
        ('{"data": [{"external_id": "123"}]}', ""),  # missing_timestamp
    ],
    ids=["valid_response", "empty_data", "multiple_entries", "invalid_json_no_data", "missing_timestamp"]
)
def test_get_prospect_last_data(response_text, expected_timestamp):

    # Arrange
    response = requests.Response()
    response.status_code = 200
    response._content = str.encode(response_text)

    # Act
    timestamp = get_prospect_last_data(response)
    print('#################', timestamp,expected_timestamp)
    # Assert
    assert timestamp == expected_timestamp


@pytest.mark.parametrize(
    "local, start_ts, expected_timestamp",
    [
        (True, None, "2024-08-15T12:00:00Z"),  # local_no_start_ts
        (False, None, "2024-08-15T12:00:00Z"),  # external_no_start_ts
        (True, "2024-08-14T12:00:00Z", "2024-08-14T12:00:00Z"),  # local_with_start_ts
        (False, "2024-08-14T12:00:00Z", "2024-08-14T12:00:00Z"),  # external_with_start_ts
    ],
    ids=["local_no_start_ts", "external_no_start_ts", "local_with_start_ts", "external_with_start_ts"]
)
def test_prospect_get_start_ts(local, start_ts, expected_timestamp, mock_request):

    # Act
    timestamp = prospect_get_start_ts(local, start_ts)

    # Assert
    assert timestamp == expected_timestamp

