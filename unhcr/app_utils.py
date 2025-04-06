import logging
import os
import sys
from unhcr import constants as const
from unhcr import utils


def get_previous_midnight_epoch(epoch: int = None) -> int:
    seconds_per_day = 24 * 60 * 60
    seconds_since_midnight = epoch % seconds_per_day

    # Subtract seconds since midnight to get to the most recent midnight
    last_midnight_epoch = epoch - seconds_since_midnight

    return last_midnight_epoch


def app_init(mods, log_file, version, mpath=const.MOD_PATH, level="INFO", override=True, quiet=True):
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
    logger = None
    try:
        logger = utils.log_setup(log_file=log_file, level=level, override=override)
    except Exception as e:
        logger = logging.getLogger("app_utils")  # Ensure logger exists
        logger.error(f"app_utils ERROR: {e}")
        return (logger,)

    try:
        if not quiet:
            logger.info(
                f"{sys.argv[0]} Process ID: {os.getpid()}   Log Level: {logging.getLevelName(int(logger.level))}"
            )

        if not utils.is_version_greater_or_equal(version):
            logger.error(
                "This version of the script requires at least version 0.4.6 of the unhcr module."
            )
            exit(int(version.replace(".", "")))

        if const.LOCAL:  # testing with local python files
            return const.import_local_libs(mpath=mpath, mods=mods, logger=logger)
    except Exception as e:
        logger.error(f"app_utils ERROR: {e}")
    return (logger,)