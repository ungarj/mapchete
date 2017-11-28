"""
Functions handling output formats.

This module deserves a cleaner rewrite some day.
"""

import os
import pkg_resources
import warnings

from mapchete import errors

_DRIVERS_ENTRY_POINT = "mapchete.formats.drivers"
_FILE_EXT_TO_DRIVER = {}


def _file_ext_to_driver():
    global _FILE_EXT_TO_DRIVER
    if _FILE_EXT_TO_DRIVER:
        return _FILE_EXT_TO_DRIVER
    else:
        _FILE_EXT_TO_DRIVER = {}
        for v in pkg_resources.iter_entry_points(_DRIVERS_ENTRY_POINT):
            _driver = v.load()
            if not hasattr(_driver, "METADATA"):
                warnings.warn(
                    "driver %s cannot be loaded, METADATA is missing" % (
                        str(v).split(" ")[-1]
                    )
                )
                continue
            else:
                metadata = v.load().METADATA
            try:
                driver_name = metadata["driver_name"]
                for ext in metadata["file_extensions"]:
                    if ext in _FILE_EXT_TO_DRIVER:
                        _FILE_EXT_TO_DRIVER[ext].append(driver_name)
                    else:
                        _FILE_EXT_TO_DRIVER[ext] = [driver_name]
            except Exception:
                pass
        if not _FILE_EXT_TO_DRIVER:
            raise errors.MapcheteDriverError("no drivers could be found")
        return _FILE_EXT_TO_DRIVER


def available_output_formats():
    """
    Return all available output formats.

    Returns
    -------
    formats : list
        all available output formats
    """
    output_formats = []
    for v in pkg_resources.iter_entry_points(_DRIVERS_ENTRY_POINT):
        driver_ = v.load()
        if hasattr(driver_, "METADATA") and (
            driver_.METADATA["mode"] in ["w", "rw"]
        ):
            output_formats.append(driver_.METADATA["driver_name"])
    return output_formats


def available_input_formats():
    """
    Return all available input formats.

    Returns
    -------
    formats : list
        all available input formats
    """
    input_formats = []
    for v in pkg_resources.iter_entry_points(_DRIVERS_ENTRY_POINT):
        driver_ = v.load()
        if hasattr(driver_, "METADATA") and (
            driver_.METADATA["mode"] in ["r", "rw"]
        ):
            input_formats.append(driver_.METADATA["driver_name"])
    return input_formats


def load_output_writer(output_params):
    """
    Return output class of driver.

    Returns
    -------
    output : ``OutputData``
        output writer object
    """
    if not isinstance(output_params, dict):
        raise TypeError("output_params must be a dictionary")
    driver_name = output_params["format"]
    for v in pkg_resources.iter_entry_points(_DRIVERS_ENTRY_POINT):
        _driver = v.load()
        if all(
            [hasattr(_driver, attr) for attr in ["OutputData", "METADATA"]]
            ) and (
            _driver.METADATA["driver_name"] == driver_name
        ):
            return _driver.OutputData(output_params)
    raise errors.MapcheteDriverError(
        "no loader for driver '%s' could be found." % driver_name)


def load_input_reader(input_params, readonly=False):
    """
    Return input class of driver.

    Returns
    -------
    input_params : ``InputData``
        input parameters
    """
    if not isinstance(input_params, dict):
        raise TypeError("input_params must be a dictionary")
    if "abstract" in input_params:
        driver_name = input_params["abstract"]["format"]
    elif "path" in input_params:
        input_file = input_params["path"]
        driver_name = driver_from_file(input_file)
    else:
        raise errors.MapcheteDriverError(
            "invalid input parameters %s" % input_params)
    for v in pkg_resources.iter_entry_points(_DRIVERS_ENTRY_POINT):
        driver_ = v.load()
        if hasattr(driver_, "METADATA") and (
            driver_.METADATA["driver_name"] == driver_name
        ):
            return v.load().InputData(input_params, readonly=readonly)
    raise errors.MapcheteDriverError(
        "no loader for driver '%s' could be found." % driver_name)


def driver_from_file(input_file):
    """
    Guess driver from file extension.

    Returns
    -------
    driver : string
        driver name
    """
    file_ext = os.path.splitext(input_file)[1].split(".")[1]
    if file_ext not in _file_ext_to_driver():
        raise errors.MapcheteDriverError(
            "no driver could be found for file extension %s" % file_ext
        )
    driver = _file_ext_to_driver()[file_ext]
    if len(driver) > 1:
        warnings.warn(
            "more than one driver for file found, taking %s" % driver[0]
        )
    return driver[0]
