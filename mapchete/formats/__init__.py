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
            try:
                metadata = v.load().METADATA
            except AttributeError:
                warnings.warn(
                    "driver %s cannot be loaded" % str(v).split(" ")[-1]
                )
                continue
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
        try:
            output_formats.append(
                v.load().OutputData.METADATA["driver_name"])
        except AttributeError:
            pass
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
    # Extensions.
    for v in pkg_resources.iter_entry_points(_DRIVERS_ENTRY_POINT):
        try:
            input_formats.append(v.load().InputData.METADATA["driver_name"])
        except ImportError:
            raise
        except Exception:
            pass
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
        raise ValueError("output_params must be a dictionary")
    driver_name = output_params["format"]
    if driver_name not in available_output_formats():
        raise errors.MapcheteDriverError("driver %s not found" % driver_name)
    for v in pkg_resources.iter_entry_points(_DRIVERS_ENTRY_POINT):
        try:
            output_writer = v.load().OutputData(output_params)
            if output_writer.METADATA["driver_name"] == driver_name:
                return output_writer
        except ImportError:
            raise
        except AttributeError:
            pass
    raise errors.MapcheteDriverError(
        "no loader for driver '%s' could be found." % driver_name)


def load_input_reader(input_params):
    """
    Return input class of driver.

    Returns
    -------
    input_params : ``InputData``
        input parameters
    """
    if "abstract" in input_params:
        driver_name = input_params["abstract"]["format"]
    elif "path" in input_params:
        input_file = input_params["path"]
        driver_name = driver_from_file(input_file)
    else:
        raise errors.MapcheteDriverError(
            "invalid input parameters %s" % input_params)
    if driver_name not in available_input_formats():
        raise errors.MapcheteDriverError(
            "driver %s not found in %s" % (
                driver_name, available_input_formats())
            )
    for v in pkg_resources.iter_entry_points(_DRIVERS_ENTRY_POINT):
        try:
            # instanciate dummy input reader to read metadata
            input_reader = v.load().InputData.__new__(
                v.load().InputData, input_params)
            if input_reader.METADATA["driver_name"] == driver_name:
                return v.load().InputData(input_params)
        except (AttributeError, errors.MapcheteConfigError):
            pass
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
    try:
        driver = _file_ext_to_driver()[file_ext]
    except KeyError:
        raise errors.MapcheteDriverError(
            "no driver could be found for file extension %s" % file_ext)
    if len(driver) == 1:
        return driver[0]
    else:
        raise errors.MapcheteDriverError(
            "error determining read driver from file %s" % input_file)
