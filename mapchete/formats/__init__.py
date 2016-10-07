"""Functions handling output formats."""

import os
import pkgutil
import pkg_resources


_FORMATS_DEFAULT_LOCATION = "mapchete/formats/default/"
_EXTENSIONS_DEFAULT_LOCATION = "mapchete.formats.extensions"


def load_output_writer(output_params):
    """Return output class of driver."""
    assert isinstance(output_params, dict)
    driver_name = output_params["format"]
    try:
        assert driver_name in available_output_formats()
    except AssertionError:
        raise KeyError("driver %s not found" % driver_name)
    return pkgutil.get_loader(
        _FORMATS_DEFAULT_LOCATION+_name_to_default_module(driver_name)
        ).load_module(_name_to_default_module(driver_name)).OutputData(
        output_params)


def load_input_reader(input_params):
    """Return input class of driver."""
    input_file = input_params["path"]
    driver_name = driver_from_file(input_file)
    try:
        assert driver_name in available_input_formats()
    except AssertionError:
        raise KeyError(
            "driver %s not found in %s" % (
                driver_name, available_input_formats()))
    try:
        return pkgutil.get_loader(
            _FORMATS_DEFAULT_LOCATION+_name_to_default_module(driver_name)
            ).load_module(_name_to_default_module(driver_name)).InputData(
                input_params)
    except (AttributeError, TypeError):
        raise
        pass
    for v in pkg_resources.iter_entry_points(_EXTENSIONS_DEFAULT_LOCATION):
        try:
            input_reader = v.load().InputData(input_params)
        except:
            raise
        if input_reader.driver_name == driver_name:
            return input_reader
    raise AttributeError(
        "no loader for driver %s could be found." % driver_name)


def available_output_formats():
    """Return all available output formats."""
    output_formats = []
    # Default formats.
    for driver_module in _default_driver_modules():
        try:
            loaded_module = pkgutil.get_loader(
                _FORMATS_DEFAULT_LOCATION+driver_module).load_module(
                driver_module)
            output_formats.append(
                loaded_module.OutputData.METADATA["driver_name"])
        except AttributeError:
            pass
    # Extensions.
    for v in pkg_resources.iter_entry_points(_EXTENSIONS_DEFAULT_LOCATION):
        try:
            output_formats.append(
                v.load().OutputData.METADATA["driver_name"])
        except:
            pass
    return output_formats


def available_input_formats():
    """Return all available input formats."""
    input_formats = []
    # Default formats.
    for driver_module in _default_driver_modules():
        try:
            input_formats.append(pkgutil.get_loader(
                 _FORMATS_DEFAULT_LOCATION+driver_module
                 ).load_module(driver_module).InputData.METADATA["driver_name"]
                 )
        except:
            pass
    # Extensions.
    for v in pkg_resources.iter_entry_points(_EXTENSIONS_DEFAULT_LOCATION):
        try:
            input_formats.append(v.load().InputData.METADATA["driver_name"])
        except:
            pass
    return input_formats


def driver_from_file(input_file):
    """Return appropriate driver for input file."""
    file_ext = os.path.splitext(input_file)[1].split(".")[1]
    try:
        drivers = _file_ext_to_driver()
        driver = drivers[file_ext]
    except KeyError:
        raise ValueError(
            "no driver could be found for file extension %s" % file_ext)
    try:
        assert len(driver) == 1
        return driver[0]
    except:
        raise RuntimeError(
            "error determining read driver from file %s" % input_file)


def _file_ext_to_driver():
    mapping = {}
    # Default formats.
    for driver_module in _default_driver_modules():
        try:
            data_loader = pkgutil.get_loader(
                 _FORMATS_DEFAULT_LOCATION+driver_module
                 ).load_module(driver_module)
            driver_name = data_loader.InputData.METADATA["driver_name"]
            for ext in data_loader.InputData.METADATA["file_extensions"]:
                if ext in mapping:
                    mapping[ext].append(driver_name)
                else:
                    mapping[ext] = [driver_name]
        except AttributeError:
            pass
    # Extensions.
    for v in pkg_resources.iter_entry_points(_EXTENSIONS_DEFAULT_LOCATION):
        try:
            data_loader = v.load().InputData.METADATA["driver_name"]
            driver_name = data_loader.InputData.METADATA["driver_name"]
            for ext in data_loader.InputData.METADATA["file_extensions"]:
                if ext in mapping:
                    mapping[ext].append(driver_name)
                else:
                    mapping[ext] = [driver_name]
        except:
            pass
    if not mapping:
        raise RuntimeError("no drivers could be found")
    return mapping


def _name_to_default_module(driver_name):
    for module in _default_driver_modules():
        loaded = pkgutil.get_loader(
            _FORMATS_DEFAULT_LOCATION+module).load_module(module)
        try:
            if loaded.InputData.METADATA["driver_name"] == driver_name:
                return module
        except AttributeError:
            pass
        try:
            if loaded.OutputData.METADATA["driver_name"] == driver_name:
                return module
        except AttributeError:
            pass
    return AttributeError


def _default_driver_modules():
    return [
            modname
            for importer, modname, ispkg in pkgutil.walk_packages(
                path=['mapchete/formats/default'], onerror=lambda x: None
            )
        ]
