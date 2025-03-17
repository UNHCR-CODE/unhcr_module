
import pytest
from unittest import mock

# Import the module to test
from unhcr import app_utils
from unhcr import constants as const

@pytest.fixture
def setup_environment():
    """Fixture to save and restore environment state"""
    original_local = const.LOCAL
    yield
    const.LOCAL = original_local

@pytest.fixture
def mock_logging():
    """Fixture to mock logging setup"""
    with mock.patch('unhcr.utils.log_setup') as mock_log_setup:
        with mock.patch('logging.info') as mock_info:
            with mock.patch('logging.error') as mock_error:
                yield mock_log_setup, mock_info, mock_error

@pytest.fixture
def mock_version_check():
    """Fixture to mock version checking"""
    with mock.patch('unhcr.utils.is_version_greater_or_equal') as mock_version:
        yield mock_version

@pytest.fixture
def mock_import_local():
    """Fixture to mock local library import"""
    with mock.patch('unhcr.constants.import_local_libs') as mock_import:
        yield mock_import

def test_app_init_basic(mock_logging, mock_version_check):
    """Test basic initialization with default parameters"""
    mock_log_setup, mock_info, _ = mock_logging
    mock_version_check.return_value = True
    
    result = app_utils.app_init(mods=[], log_file="test.log", version="0.4.6")
    
    # Assert log_setup was called with correct parameters
    mock_log_setup.assert_called_once_with(level="INFO", log_file="test.log", override=False)
    # Assert logging.info was called
    assert mock_info.called
    # Assert no modules returned since LOCAL is false by default
    print(result, 'ZZZZZZZZZZ')
    assert result is ()
    # Assert version was checked
    mock_version_check.assert_called_once_with("0.4.6")

def test_app_init_custom_level(mock_logging, mock_version_check):
    """Test initialization with custom log level"""
    mock_log_setup, _, _ = mock_logging
    mock_version_check.return_value = True
    
    app_utils.app_init(mods=[], log_file="test.log", version="0.4.6", level="DEBUG")
    
    # Assert log_setup was called with custom level
    mock_log_setup.assert_called_once_with(level="DEBUG", log_file="test.log", override=False)

def test_app_init_override_logging(mock_logging, mock_version_check):
    """Test initialization with logging override"""
    mock_log_setup, _, _ = mock_logging
    mock_version_check.return_value = True
    
    app_utils.app_init(mods=[], log_file="test.log", version="0.4.6", override=True)
    
    # Assert log_setup was called with override=True
    mock_log_setup.assert_called_once_with(level="INFO", log_file="test.log", override=True)

def test_app_init_version_check_fail(mock_logging, mock_version_check):
    """Test behavior when version check fails"""
    _, _, mock_error = mock_logging
    mock_version_check.return_value = False
    
    with pytest.raises(SystemExit) as excinfo:
        app_utils.app_init(mods=[], log_file="test.log", version="0.4.6")
    
    # Assert logging.error was called
    assert mock_error.called
    # Assert exit code matches version number without dots
    assert excinfo.value.code == 46

def test_app_init_local_modules(setup_environment, mock_logging, mock_version_check, mock_import_local):
    """Test importing local modules when const.LOCAL is True"""
    mock_log_setup, _, _ = mock_logging
    mock_version_check.return_value = True
    mock_import_local.return_value = ["mock_module1", "mock_module2"]
    
    # Set LOCAL to True for this test
    const.LOCAL = True
    test_mods = ["module1", "module2"]
    result = app_utils.app_init(mods=test_mods, log_file="test.log", version="0.4.6")
    
    # Assert import_local_libs was called with correct parameters
    mock_import_local.assert_called_once_with(mpath=const.MOD_PATH, mods=test_mods)
    # Assert the result matches what import_local_libs returned
    assert result == ["mock_module1", "mock_module2"]

def test_app_init_custom_mpath(setup_environment, mock_logging, mock_version_check, mock_import_local):
    """Test using custom module path"""
    mock_version_check.return_value = True
    custom_path = "/custom/path"
    
    # Set LOCAL to True for this test
    const.LOCAL = True
    app_utils.app_init(mods=["test"], log_file="test.log", version="0.4.6", mpath=custom_path)
    
    # Assert import_local_libs was called with custom path
    mock_import_local.assert_called_once_with(mpath=custom_path, mods=["test"])

def test_app_init_exception_handling(mock_logging):
    """Test exception handling during initialization"""
    mock_log_setup, _, mock_error = mock_logging
    
    # Force an exception by making log_setup raise an exception
    mock_log_setup.side_effect = Exception("Test exception")
    
    result = app_utils.app_init(mods=[], log_file="test.log", version="0.4.6")
    
    # Assert logging.error was called with the exception message
    mock_error.assert_called_once()
    error_message = mock_error.call_args[0][0]
    assert "app_utils ERROR: Test exception" in error_message
    # Assert None is returned on exception
    assert result is None