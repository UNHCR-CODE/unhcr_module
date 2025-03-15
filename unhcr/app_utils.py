import logging
import os
import sys
from unhcr import constants as const
from unhcr import utils

def app_init(mods, log_file, version, mpath=const.MOD_PATH, level="INFO", override=False):
    """
    Initialize the application by setting up logging, checking the module version,
    and optionally importing local libraries for testing.

    Parameters
    ----------
    mods : list
        A list of modules to import if testing locally.
    log_file : str
        The name of the log file for logging output.
    version : str
        The minimum required version of the unhcr module.
    mpath : str, optional
        The module path to use when importing local libraries, by default const.MOD_PATH.
    level : str, optional
        The logging level, by default 'INFO'.
    override : bool, optional
        If True, existing log handlers will be cleared and new ones set up, by default False.

    Returns
    -------
    list or None
        Returns the imported local libraries if const.LOCAL is True, otherwise None.

    Exits
    -----
    Exits the program with an error code if the current unhcr module version is less than the required version.
    """
    try:
        utils.log_setup(level=level, log_file=log_file, override=override)
        logging.info(
            f"{sys.argv[0]} Process ID: {os.getpid()}   Log Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}"
        )

        if not utils.is_version_greater_or_equal(version):
            logging.error(
                "This version of the script requires at least version 0.4.6 of the unhcr module."
            )
            exit(int(version.replace(".", "")))

        if const.LOCAL:  # testing with local python files
            return const.import_local_libs(mpath=mpath, mods=mods)
    except Exception as e:
        logging.error(f"app_utils ERROR: {e}")
    return None