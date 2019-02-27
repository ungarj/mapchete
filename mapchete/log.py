"""
Custom loggers for external code such as user processes & drivers.

This is necessary because if using the logging module directly, the namespace
is not assigned properly and log levels & log handlers cannot be assigned
correctly.
"""
from iterable import chain
import logging

from mapchete.formats import registered_driver_packages


registered_modules = registered_driver_packages()

# lower stream output log level
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.WARNING)
for i in chain(["mapchete"], registered_modules):
    logging.getLogger(i).addHandler(stream_handler)


def set_log_level(loglevel):
    stream_handler.setLevel(loglevel)
    for i in chain(["mapchete"], registered_modules):
        logging.getLogger(i).setLevel(loglevel)


def setup_logfile(logfile):
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    for i in chain(["mapchete"], registered_modules):
        logging.getLogger(i).addHandler(file_handler)
        logging.getLogger(i).setLevel(logging.DEBUG)


def user_process_logger(pname):
    """Logger to be used within a user process file."""
    return logging.getLogger("mapchete.user_process." + pname)


def driver_logger(dname):
    """Logger to be used from a driver plugin."""
    return logging.getLogger("mapchete.formats.drivers." + dname)
