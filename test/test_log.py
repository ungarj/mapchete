"""Test for custom logging functions."""

import logging
import pytest

from mapchete.log import user_process_logger, driver_logger


def test_user_process_logger():
    with pytest.deprecated_call():
        logger = user_process_logger(__name__)
        assert isinstance(logger, logging.Logger)
        assert logger.name == "mapchete.user_process.test_log"


def test_driver_logger():
    with pytest.deprecated_call():
        logger = driver_logger(__name__)
        assert isinstance(logger, logging.Logger)
        assert logger.name == "mapchete.formats.drivers.test_log"
