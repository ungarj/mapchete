"""
Custom loggers for external code such as user processes & drivers.

This is necessary because if using the logging module directly, the namespace
is not assigned properly and log levels & log handlers cannot be assigned
correctly.
"""

import logging


# lower stream output log level
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.WARNING)
logging.getLogger().addHandler(stream_handler)


def set_log_level(loglevel):
    logging.getLogger("mapchete").setLevel(loglevel)
    stream_handler.setLevel(loglevel)


def setup_logfile(logfile):
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    logging.getLogger("mapchete").setLevel(logging.DEBUG)


def user_process_logger(pname):
    """Logger to be used within a user process file."""
    return logging.getLogger("mapchete.user_process." + pname)


def driver_logger(dname):
    """Logger to be used from a driver plugin."""
    return logging.getLogger("mapchete.formats.drivers." + dname)
