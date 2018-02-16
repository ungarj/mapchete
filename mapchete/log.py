"""
Custom loggers for external code such as user processes & drivers.

This is necessary because if using the logging module directly, the namespace
is not assigned properly and log levels & log handlers cannot be assigned
correctly.
"""

import logging


def user_process_logger(pname):
    """Logger to be used within a user process file."""
    return logging.getLogger("mapchete.user_process." + pname)


def driver_logger(dname):
    """Logger to be used from a driver plugin."""
    return logging.getLogger("mapchete.formats.drivers." + dname)
