"""
Custom loggers for external code such as user processes & drivers.

This is necessary because if using the logging module directly, the namespace
is not assigned properly and log levels & log handlers cannot be assigned
correctly.
"""
from itertools import chain
import logging
import pkg_resources
import warnings

all_mapchete_packages = set(
    v.module_name.split(".")[0]
    for v in chain(
        pkg_resources.iter_entry_points("mapchete.formats.drivers"),
        pkg_resources.iter_entry_points("mapchete.processes")
    )
)

# lower stream output log level
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.WARNING)
for i in all_mapchete_packages:
    logging.getLogger(i).addHandler(stream_handler)


def add_module_logger(module_name):
    # set loglevel
    logging.getLogger(module_name).setLevel(
        logging.getLogger("mapchete").getEffectiveLevel()
    )
    # add all configured handlers
    for handler in logging.getLogger("mapchete").handlers:
        logging.getLogger(module_name).addHandler(handler)


def set_log_level(loglevel):
    stream_handler.setLevel(loglevel)
    for i in all_mapchete_packages:
        logging.getLogger(i).setLevel(loglevel)


def setup_logfile(logfile):
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    for i in all_mapchete_packages:
        logging.getLogger(i).addHandler(file_handler)
        logging.getLogger(i).setLevel(logging.DEBUG)


def user_process_logger(pname):
    """Logger to be used within a user process file."""
    warnings.warn(
        DeprecationWarning(
            "user_process_logger() deprecated, you can use standard logging module "
            "instead."
        )
    )
    return logging.getLogger("mapchete.user_process." + pname)


def driver_logger(dname):
    """Logger to be used from a driver plugin."""
    warnings.warn(
        DeprecationWarning(
            "driver_logger() deprecated, you can use standard logging module instead."
        )
    )
    return logging.getLogger("mapchete.formats.drivers." + dname)
