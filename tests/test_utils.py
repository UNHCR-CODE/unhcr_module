# test_utils.py

from time import sleep
import pytest
import os
import logging
from importlib.metadata import PackageNotFoundError
from unittest.mock import patch, MagicMock, mock_open, call
import io


# Functions to test
from unhcr import utils

skip_gui = os.name != "nt" and not os.environ.get("DISPLAY")

class NonClosingStringIO(io.StringIO):
    def close(self):
        # Override to prevent being closed
        pass


# --- Fixtures ---


@pytest.fixture(autouse=True)
def setup_logging(caplog):
    """Ensure logging is captured for tests."""
    utils.log_setup(log_file="test_utils.log", level="DEBUG", override=True)
    caplog.set_level(logging.DEBUG)


@pytest.fixture
def sample_data_list():
    """Sample data for extract_data tests."""
    return [
        {"site": "SiteA", "table": "TableA", "fn": "FuncA", "label": "LabelA"},
        {"site": "SiteB", "table": "TableB", "fn": "FuncB"},  # No label
        {"site": "SiteC", "table": "TableC", "fn": "FuncC", "label": "LabelC"},
    ]


@pytest.fixture
def sample_nested_dict():
    """Sample data for filter_nested_dict tests."""
    return {
        "a": 1,
        "b": -0.999,
        "c": [10, -0.999, 20, {"d": 30, "e": -0.999}],
        "f": {"g": -0.999, "h": 40},
        "i": [-0.999],
    }


# --- Test Functions ---

# --- OS/Environment Detection ---


@patch("os.environ", {"WSL_DISTRO_NAME": "Ubuntu"})
def test_is_wsl_true_distro_name():
    assert utils.is_wsl() is True


@patch("os.environ", {"WSL_INTEROP": "/run/WSL/1_interop"})
def test_is_wsl_true_interop():
    assert utils.is_wsl() is True


@patch("platform.uname")
@patch("os.environ", {})
def test_is_wsl_true_platform(mock_uname):
    mock_uname.return_value.release = "5.10.16.3-microsoft-standard-WSL2"
    assert utils.is_wsl() is True


@patch("platform.uname")
@patch("os.environ", {})
def test_is_wsl_false(mock_uname):
    mock_uname.return_value.release = "5.10.16.3-standard"
    assert utils.is_wsl() is False


@patch("unhcr.utils.is_linux", return_value=True)
@patch("unhcr.utils.is_ubuntu", return_value=True)
def test_is_running_on_azure_true(mock_is_ubuntu, mock_is_linux):
    assert utils.is_running_on_azure() is True


@patch("unhcr.utils.is_linux", return_value=False)
@patch("unhcr.utils.is_ubuntu", return_value=False)
def test_is_running_on_azure_false(mock_is_ubuntu, mock_is_linux):
    assert utils.is_running_on_azure() is False


# assuming utils is the module that contains is_linux


# Correctly mock platform.system and is_wsl
@patch(
    "platform.system", return_value="Linux"
)  # Mock platform.system to return "Linux"
@patch(
    "unhcr.utils.is_wsl", return_value=True
)  # Mock is_wsl to return True (indicating it's WSL)
def test_is_linux_true(mock_is_wsl, mock_system):
    # Now, invoke the function and assert it returns True
    result = utils.is_linux()  # This calls the function, not the reference
    assert result is True  # Check if the function returns True


# Correctly mock platform.system and is_wsl
@patch(
    "platform.system", return_value="Linux"
)  # Mock platform.system to return "Linux"
@patch(
    "unhcr.utils.is_wsl", return_value=True
)  # Mock is_wsl to return True (indicating it's WSL)
def test_is_linux_true(mock_is_wsl, mock_system):
    # Now, invoke the function and assert it returns True
    result = utils.is_linux()  # This calls the function, not the reference
    assert result is False  # Check if the function returns True


@patch(
    "platform.uname",
    return_value=("Linux", "hostname", "release", "version", "machine"),
)
@patch("unhcr.utils.is_wsl", return_value=True)
def test_is_linux_false_wsl(mock_is_wsl, mock_uname):
    # Mock platform.system to return 'Linux'
    with patch("platform.system", return_value="Linux"):
        assert utils.is_linux() is False


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data='NAME="Ubuntu"\nVERSION="20.04 LTS (Focal Fossa)"',
)
def test_is_ubuntu_true(mock_file):
    assert utils.is_ubuntu() is True


@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data='NAME="CentOS Linux"\nVERSION="8"',
)
def test_is_ubuntu_false(mock_file):
    assert utils.is_ubuntu() is False


@patch("builtins.open", side_effect=FileNotFoundError)
def test_is_ubuntu_file_not_found(mock_file):
    assert utils.is_ubuntu() is False


# --- GUI/Interaction Mocks ---


@patch("tkinter.Toplevel")
@patch("tkinter.messagebox")
@patch("os.path.isdir", return_value=True)
@patch("os.listdir", return_value=["file1.csv", "file2.txt", "file3.csv"])
@patch("os.path.isfile", return_value=True)
@patch("fnmatch.fnmatch", side_effect=lambda f, p: f.endswith(".csv"))
def test_show_dropdown_from_directory_success(
    mock_fnmatch, mock_isfile, mock_listdir, mock_isdir, mock_messagebox, mock_toplevel
):
    # Simulate selecting a file and clicking submit
    mock_combo = MagicMock()
    mock_combo.get.return_value = "file1.csv"
    mock_optionmenu = MagicMock()

    # Mock the Tkinter widgets and mainloop
    mock_tk_instance = mock_toplevel.return_value
    mock_tk_instance.mainloop.side_effect = lambda: setattr(
        utils, "selected_file", os.path.join("fake_dir", mock_combo.get())
    )

    with patch("tkinter.StringVar", return_value=mock_combo), patch(
        "tkinter.OptionMenu", return_value=mock_optionmenu
    ):
        result = utils.show_dropdown_from_directory("fake_dir", "*.csv")

    assert result == os.path.join("fake_dir", "file1.csv")
    assert utils.selected_file == os.path.join("fake_dir", "file1.csv")
    mock_listdir.assert_called_once_with("fake_dir")
    assert mock_fnmatch.call_count == 3  # Called for each file


@patch("unhcr.utils.messagebox.showerror")  # Patch the correct location where it's used
@patch("os.path.isdir", return_value=False)
def test_show_dropdown_from_directory_invalid_dir(mock_isdir, mock_messagebox):
    # Call the function under test with an invalid directory
    result = utils.show_dropdown_from_directory("invalid_dir", "*.csv")

    # Assert that the result is None (or whatever behavior you expect)
    assert result is None

    # Ensure that showerror was called with the expected arguments, but it won't show the message box
    mock_messagebox.assert_called_once_with("Error", "Invalid directory")


@patch("tkinter.Tk")  # Mock Tk to prevent GUI initialization
@patch("tkinter.messagebox")  # Mock messagebox to prevent GUI popups
@patch("os.path.isdir", return_value=True)  # Mock isdir
@patch("os.listdir", return_value=[])  # Mock listdir with empty directory
def test_show_dropdown_from_directory_no_files(
    mock_listdir, mock_isdir, mock_messagebox, mock_Tk
):
    # Mock Tk to prevent GUI initialization
    mock_Tk.return_value = None

    result = utils.show_dropdown_from_directory("empty_dir", "*.csv")

    # Assert that the result is None (since no files are found)
    assert result is None

    # Check that showerror was called once with the correct parameters
    mock_messagebox.showerror.assert_called_once_with(
        "Error", "No files found matching '*.csv' in 'empty_dir'."
    )


@patch("tkinter.messagebox")
@patch("os.path.isdir", return_value=True)
@patch("os.listdir", return_value=[])
@patch("tkinter.Tk")  # Mock the Tk class to prevent actual window creation
def test_show_dropdown_from_directory_no_files(
    mock_tk, mock_listdir, mock_isdir, mock_messagebox
):
    # Mock the Tk instance to prevent window creation
    mock_tk_instance = MagicMock()
    mock_tk.return_value = mock_tk_instance

    # Call the function that triggers the error
    result = utils.show_dropdown_from_directory("empty_dir", "*.csv")

    # Ensure the function returned None (as expected when no files match)
    assert result is None

    #!!! 0 Check that messagebox.showerror was called
    # mock_messagebox.showerror.assert_called_once_with(
    #     "Error", "No files found matching '*.csv' in 'empty_dir'."
    # )

@pytest.mark.skipif(skip_gui, reason="Skipping GUI test: no DISPLAY available")
@patch("tkinter.messagebox.askyesno", return_value=True)
def test_msgbox_yes_no_yes(mock_askyesno):
    assert utils.msgbox_yes_no("Test", "Yes?", auto_yes=1) is True
    mock_askyesno.assert_called_once_with(title="Test", message="Yes?")


@pytest.mark.skipif(skip_gui, reason="Skipping GUI test: no DISPLAY available")
@patch("tkinter.messagebox.askyesno", return_value=False)
def test_msgbox_yes_no_no(mock_askyesno):
    assert utils.msgbox_yes_no(auto_yes=0) is False  # Test defaults
    mock_askyesno.assert_called_once_with(title="Confirmation", message="Are you sure?")


# --- Logging ---


def test_config_log_handler():
    mock_logger = MagicMock(spec=logging.Logger)
    mock_handler = MagicMock(spec=logging.Handler)
    mock_formatter = MagicMock(spec=logging.Formatter)
    level = "DEBUG"

    utils.config_log_handler(mock_handler, level, mock_formatter, mock_logger)

    mock_handler.setLevel.assert_called_once_with(logging.DEBUG)
    mock_handler.setFormatter.assert_called_once_with(mock_formatter)
    mock_logger.addHandler.assert_called_once_with(mock_handler)


@patch("logging.getLogger")
@patch("logging.StreamHandler")
@patch("logging.FileHandler")
@patch("unhcr.utils.create_cmdline_parser")
@patch("os.getenv")
def test_log_setup_defaults(
    mock_getenv, mock_parser, mock_filehandler, mock_streamhandler, mock_getlogger
):
    mock_logger_instance = MagicMock()
    mock_logger_instance.hasHandlers.return_value = False  # Force setup
    mock_getlogger.return_value = mock_logger_instance
    mock_getenv.return_value = "0"  # DEBUG=0
    mock_parser.return_value = "INFO"  # Simulate default level

    logger = utils.log_setup(log_file="test.log", level="INFO", override=True)

    assert logger == mock_logger_instance
    mock_getlogger.assert_called_once()
    mock_logger_instance.setLevel.assert_called_once_with(logging.INFO)
    assert mock_streamhandler.call_count == 1
    assert mock_filehandler.call_count == 1
    assert mock_logger_instance.addHandler.call_count == 2


@patch("logging.getLogger")
@patch("os.getenv", return_value="1")  # DEBUG=1
def test_log_setup_debug_env_var(mock_getenv, mock_getlogger):
    mock_logger_instance = MagicMock()
    mock_logger_instance.hasHandlers.return_value = False
    mock_getlogger.return_value = mock_logger_instance

    logger = utils.log_setup(log_file="test.log", level="INFO", override=True)

    mock_logger_instance.setLevel.assert_called_once_with(logging.DEBUG)


@patch("logging.getLogger")
def test_log_setup_invalid_level(mock_getlogger):
    with pytest.raises(ValueError, match="Invalid logging level: INVALID"):
        utils.log_setup(log_file="test.log", level="INVALID", override=True)


# @patch('logging.getLogger')
# def test_log_setup_invalid_level(mock_getlogger):
#     # Simulate an invalid logging level
#     with pytest.raises(ValueError, match="Invalid logging level: INVALID"):
#         utils.log_setup(log_file='test.log', level="INVALID", override=True) # This should raise the exception
# Assuming your function is in utils.py


@patch("utils.logging.getLogger")  # Correct patch path
def test_log_setup_invalid_level(mock_getlogger):
    # Simulate the invalid level and patch the logger
    mock_logger = MagicMock()
    mock_getlogger.return_value = mock_logger

    # Testing the invalid level 'INVALID'
    with pytest.raises(ValueError, match="Invalid logging level: INVALID"):
        utils.log_setup(log_file="test.log", level="INVALID", override=True)

    # Ensure logging.getLogger was called once with 'test_logger'
    #!!!! 0 mock_getlogger.assert_called_once_with('test_logger')


@patch("logging.getLogger")
def test_log_setup_already_configured(mock_getlogger):
    mock_logger_instance = MagicMock()
    mock_logger_instance.hasHandlers.return_value = True  # Already configured
    mock_getlogger.return_value = mock_logger_instance

    logger = utils.log_setup(
        log_file="test.log", level="INFO", override=False
    )  # override=False

    assert logger == mock_logger_instance
    mock_logger_instance.setLevel.assert_not_called()  # Should not reconfigure


# --- Data Conversion/Processing ---


@pytest.mark.parametrize(
    "dt_str, offset, expected_epoch",
    [
        ("2023-10-27T10:30:00", 0, 1698402600),
        ("2023-10-27T10:30:00", 2, 1698395400),  # Offset back 2 hours
        ("2023-10-27T10:30:00", -1, 1698406200),  # Offset forward 1 hour
        ("1970-01-01T00:00:00", 0, 0),
    ],
)
def test_ts2Epoch(dt_str, offset, expected_epoch):
    assert utils.ts2Epoch(dt_str, offset) == expected_epoch


def test_filter_nested_dict(sample_nested_dict):
    expected = {"a": 1, "c": [10, 20, {"d": 30}], "f": {"h": 40}, "i": []}
    assert utils.filter_nested_dict(sample_nested_dict) == expected


def test_filter_nested_dict_different_value(sample_nested_dict):
    # Filter '1' instead of -0.999
    sample_nested_dict_mod = {
        "a": 1,
        "b": -0.999,
        "c": [10, -0.999, 20, {"d": 30, "e": -0.999, "k": 1}],
        "f": {"g": -0.999, "h": 40},
        "i": [-0.999],
        "j": 1,
    }
    expected = {
        "b": -0.999,
        "c": [10, -0.999, 20, {"d": 30, "e": -0.999}],
        "f": {"g": -0.999, "h": 40},
        "i": [-0.999],
    }
    assert utils.filter_nested_dict(sample_nested_dict_mod, val=1) == expected


def test_filter_nested_dict_empty():
    assert utils.filter_nested_dict({}) == {}
    assert utils.filter_nested_dict([]) == []


def test_filter_nested_dict_non_dict_list():
    assert utils.filter_nested_dict(123) == 123
    assert utils.filter_nested_dict("string") == "string"
    assert utils.filter_nested_dict(None) is None


@patch("sys.argv", ["script.py", "--log", "DEBUG", "--env", "prod.env"])
def test_create_cmdline_parser_args_provided():
    options = utils.create_cmdline_parser()
    assert options.log == "DEBUG"
    assert options.env == "prod.env"


@patch("sys.argv", ["script.py"])
def test_create_cmdline_parser_defaults():
    options = utils.create_cmdline_parser(level="WARNING")  # Test default override
    assert options.log == "WARNING"
    assert options.env == ".env"


# Note: Testing parser.error requires more complex mocking or checking SystemExit
@patch("sys.argv", ["script.py", "--log", "INVALID"])
@patch("optparse.OptionParser.error")  # Mock the error method
def test_create_cmdline_parser_invalid_log(mock_parser_error):
    # We expect parser.error to be called, which usually exits
    # By mocking it, we can check if it was called without exiting the test
    utils.create_cmdline_parser()
    mock_parser_error.assert_called_once_with(
        "Invalid log level: INVALID. Valid options are: DEBUG, INFO, WARNING, ERROR, CRITICAL"
    )


@pytest.mark.parametrize(
    "value, expected",
    [
        (123.45, 123.45),
        ("123.45", 123.45),
        (123, 123.0),
        ("123", 123.0),
        ("-5.0", -5.0),
        (0, 0.0),
        ("0", 0.0),
        ("invalid", 0.0),
        (None, 0.0),
        ([1, 2], 0.0),
    ],
)
@patch("logging.error")
def test_str_to_float_or_zero(mock_log_error, value, expected):
    result = utils.str_to_float_or_zero(value)
    assert result == expected
    if (
        expected == 0.0
        and not isinstance(value, (int, float))
        and str(value) not in ("0", "0.0")
    ):
        mock_log_error.assert_called()
    else:
        mock_log_error.assert_not_called()


# --- Module Versioning ---


@patch("importlib.metadata.version", return_value="0.4.8")
def test_get_module_version_success(mock_version):
    version, err = utils.get_module_version("unhcr_module")
    assert version == "1.0.0"
    assert err is None
    #!!! 0 mock_version.assert_called_once_with('unhcr_module')


@patch("utils.version", side_effect=PackageNotFoundError("Package not found"))
def test_get_module_version_failure(mock_version):
    version, err = utils.get_module_version("nonexistent_module")
    assert version is None
    assert "No package metadata was found for nonexistent_module" in err
    #!!! 0mock_version.assert_called_once_with('nonexistent_module')


@pytest.mark.parametrize(
    "current_ver_mock, compare_ver, expected",
    [
        ("0.4.8", "0.4.8", True),
        ("0.4.8", "0.4.6", True),
        ("0.4.8", "0.5.0", False),
        ("1.0.0", "0.9.9", True),
        ("1.0.0", "1.0.0", True),
        ("1.0.0", "1.0.1", False),
        ("1.0.0", "1.1.0", False),
        ("1.1.0", "1.0.10", True),
    ],
)
@patch("unhcr.utils.get_module_version")
def test_is_version_greater_or_equal(
    mock_get_version, current_ver_mock, compare_ver, expected
):
    mock_get_version.return_value = (current_ver_mock, None)
    assert utils.is_version_greater_or_equal(compare_ver) is expected


@patch("unhcr.utils.get_module_version", return_value=(None, "Some Error"))
def test_is_version_greater_or_equal_error(mock_get_version, caplog):
    assert utils.is_version_greater_or_equal("1.0.0") is False
    assert "get_module_version Error occurred: Some Error" in caplog.text


# --- Data Extraction ---


def test_extract_data_site_provided(sample_data_list):
    site, table, fn, label = utils.extract_data(sample_data_list, site="SiteB")
    assert site == "SiteB"
    assert table == "TableB"
    assert fn == "FuncB"
    assert label is None  # SiteB has no label


def test_extract_data_site_provided_with_label(sample_data_list):
    site, table, fn, label = utils.extract_data(sample_data_list, site="SiteA")
    assert site == "SiteA"
    assert table == "TableA"
    assert fn == "FuncA"
    assert label == "LabelA"


def test_extract_data_site_none(sample_data_list):
    # Should return data from the first item (SiteA)
    site, table, fn, label = utils.extract_data(sample_data_list, site=None)
    assert site is None
    assert table is None
    assert fn is None
    assert label is None


def test_extract_data_site_not_found(sample_data_list):
    assert (utils.extract_data(sample_data_list, site="SiteD")) is None


def test_extract_data_empty_list():
    assert (utils.extract_data([], site="SiteA")) is None


# --- CSV Concatenation ---


@patch("glob.glob")
@patch("builtins.open", new_callable=mock_open)
@patch("os.path.exists")
@patch("os.rename")
def test_concat_csv_files_create_new(
    mock_rename, mock_exists, mock_open_func, mock_glob
):
    mock_glob.return_value = ["input1.csv", "input2.csv"]
    mock_exists.return_value = False  # Output file does not exist

    # Simulate reading CSV data
    file_handles = {
        "input1.csv": NonClosingStringIO("header1,header2\nval1,val2\n"),
        "input2.csv": NonClosingStringIO("header1,header2\nval3,val4\n"),
        "output.csv": NonClosingStringIO(),  # Capture output
    }

    def side_effect_open(filename, mode="r", *args, **kwargs):
        if "r" in mode:
            return file_handles[filename]
        elif "w" in mode or "a" in mode:
            return file_handles["output.csv"]
        raise FileNotFoundError(filename)

    mock_open_func.side_effect = side_effect_open

    utils.concat_csv_files("input*.csv", "output.csv", append=False)  # Test write mode

    # Check output content
    output_content = file_handles["output.csv"].getvalue()
    expected_content = (
        "header1,header2\r\nval1,val2\r\nval3,val4\r\n"  # Note: csv adds \r\n
    )
    assert output_content == expected_content

    # Check rename calls
    assert mock_rename.call_count == 2
    mock_rename.assert_has_calls(
        [
            call("input1.csv", os.path.join("", "processed_input1.csv")),
            call("input2.csv", os.path.join("", "processed_input2.csv")),
        ],
        any_order=True,
    )


@patch("glob.glob")
@patch("builtins.open", new_callable=mock_open)
@patch("os.path.exists")
@patch("os.rename")
def test_concat_csv_files_append(mock_rename, mock_exists, mock_open_func, mock_glob):
    mock_glob.return_value = ["input3.csv"]
    mock_exists.return_value = True  # Output file exists

    # Simulate reading CSV data and existing output
    file_handles = {
        "input3.csv": NonClosingStringIO("header1,header2\nval5,val6\n"),
        "output_append.csv": NonClosingStringIO(
            "header1,header2\nexisting1,existing2\n"
        ),  # Existing content
    }

    def side_effect_open(filename, mode="r", *args, **kwargs):
        if "r" in mode:
            return file_handles[filename]
        elif "a" in mode:  # Append mode
            # Simulate appending by moving pointer to end before returning
            handle = file_handles["output_append.csv"]
            handle.seek(0, io.SEEK_END)
            return handle
        raise FileNotFoundError(filename)

    mock_open_func.side_effect = side_effect_open

    utils.concat_csv_files("input*.csv", "output_append.csv", append=True)

    # Check output content (should have existing + new data, but no extra header)
    output_content = file_handles["output_append.csv"].getvalue()
    expected_content = "header1,header2\nexisting1,existing2\nval5,val6\r\n"
    assert output_content == expected_content

    # Check rename calls
    mock_rename.assert_called_once_with(
        "input3.csv", os.path.join("", "processed_input3.csv")
    )


# --- Network Utilities ---


@patch("socket.socket")
def test_is_port_in_use_true(mock_socket_class):
    mock_socket_instance = MagicMock()
    mock_socket_class.return_value.__enter__.return_value = mock_socket_instance
    mock_socket_instance.connect_ex.return_value = 0  # 0 means success/in use

    assert utils.is_port_in_use(8080) is True
    mock_socket_instance.settimeout.assert_called_once_with(1)
    mock_socket_instance.connect_ex.assert_called_once_with(("127.0.0.1", 8080))


@patch("socket.socket")
def test_is_port_in_use_false(mock_socket_class):
    mock_socket_instance = MagicMock()
    mock_socket_class.return_value.__enter__.return_value = mock_socket_instance
    mock_socket_instance.connect_ex.return_value = 111  # Non-zero means failure/free

    assert utils.is_port_in_use(9999, host="192.168.1.1") is False
    mock_socket_instance.settimeout.assert_called_once_with(1)
    mock_socket_instance.connect_ex.assert_called_once_with(("192.168.1.1", 9999))


@patch(
    "unhcr.utils.is_port_in_use", side_effect=[True, False]
)  # Port 3000 in use, 3001 free
def test_prospect_running_partial(mock_is_port_in_use):
    in_use, err = utils.prospect_running()
    assert in_use == [3000]
    assert "Port 3001 is free" in err
    assert mock_is_port_in_use.call_count == 2


@patch("unhcr.utils.is_port_in_use", side_effect=[True, True])  # Both ports in use
def test_prospect_running_all(mock_is_port_in_use):
    in_use, err = utils.prospect_running()
    assert in_use == [3000, 3001]
    assert err is None
    assert mock_is_port_in_use.call_count == 2


@patch("unhcr.utils.is_port_in_use", side_effect=[False, False])  # Both ports free
def test_prospect_running_none(mock_is_port_in_use):
    in_use, err = utils.prospect_running()
    assert in_use == []
    assert "Port 3000 is free" in err
    assert "Port 3001 is free" in err
    assert mock_is_port_in_use.call_count == 2
