import inspect
import logging
import os
from typing import Dict, Optional

from mapchete.errors import MapcheteDriverError
from mapchete.formats.models import DriverMetadata
from mapchete.io import path_exists
from mapchete._registered import DRIVER_MODULES


logger = logging.getLogger(__name__)

_DETECTED_DRIVERS = {}


def load_output_reader(output_params: Dict) -> "OutputDataReader":
    """
    Return OutputReader class of driver.

    Parameters
    ----------
    output_params : dict
        Output parameters as given in mapchete configuration.

    Returns
    -------
    output : ``OutputDataReader``
        output reader object
    """
    if not isinstance(output_params, dict):
        raise TypeError("output_params must be a dictionary")
    driver_name = output_params["format"]
    mode = "r"
    try:
        driver = detect_driver_classes()[driver_name]
    except KeyError:
        raise MapcheteDriverError(
            "no loader for driver '%s' could be found." % driver_name
        )
    if mode not in driver.mode:
        raise MapcheteDriverError(
            f"mode '{mode}' not supported in {driver.name} driver: {driver.mode}"
        )
    try:
        return driver.output_reader_cls(output_params, readonly=True)
    except Exception as exc:  # pragma: no cover
        raise MapcheteDriverError(
            f"cannot instantiate driver object in {driver}"
        ) from exc


def load_output_writer(
    output_params: Dict, readonly: bool = False
) -> "OutputDataWriter":
    """
    Return output class of driver.

    Parameters
    ----------
    output_params : dict
        Output parameters as given in mapchete configuration.
    readonly : bool
        Initialize as readonly. (default: False)

    Returns
    -------
    output : ``OutputDataWriter``
        output writer object
    """
    if not isinstance(output_params, dict):
        raise TypeError("output_params must be a dictionary")
    driver_name = output_params["format"]
    mode = "w"
    try:
        driver = detect_driver_classes()[driver_name]
    except KeyError:
        raise MapcheteDriverError(
            "no loader for driver '%s' could be found." % driver_name
        )
    if mode not in driver.mode:
        raise MapcheteDriverError(
            f"mode '{mode}' not supported in {driver.name} driver: {driver.mode}"
        )
    try:
        return driver.output_writer_cls(output_params, readonly=True)
    except Exception as exc:  # pragma: no cover
        raise MapcheteDriverError(
            f"cannot instantiate driver object in {driver}"
        ) from exc


def load_input_reader(
    input_params: Dict, readonly: bool = False, input_key: Optional[str] = None
) -> "InputData":
    """
    Return input class of driver.

    Parameters
    ----------
    input_params : dict
        Input parameters as given in mapchete configuration.
    readonly : bool
        Initialize as readonly. (default: False)
    input_key : str
        Pass on unique input key assigned by mapchete.

    Returns
    -------
    input_params : ``InputData``
        input parameters
    """
    logger.debug("find input reader with params %s", input_params)
    if not isinstance(input_params, dict):
        raise TypeError("input_params must be a dictionary")

    # find out driver name
    if "abstract" in input_params:
        driver_name = input_params["abstract"]["format"]
    elif "path" in input_params:
        # if path has a file extension it is considered a single file
        if os.path.splitext(input_params["path"])[1]:
            input_file = input_params["path"]
            driver_name = driver_from_file(input_file)
        # else a TileDirectory is assumed
        else:
            logger.debug("%s is a directory", input_params["path"])
            driver_name = "TileDirectory"
    else:
        raise MapcheteDriverError("invalid input parameters %s" % input_params)

    # load this driver module and try to initialize an InputData object
    mode = "r"
    try:
        driver = detect_driver_classes()[driver_name]
    except KeyError:
        raise MapcheteDriverError(
            "no loader for driver '%s' could be found." % driver_name
        )
    if mode not in driver.mode:
        raise MapcheteDriverError(
            f"mode '{mode}' not supported in {driver.name} driver: {driver.mode}"
        )
    try:
        return driver.input_reader_cls(
            input_params, readonly=readonly, input_key=input_key
        )
    except Exception as exc:  # pragma: no cover
        raise MapcheteDriverError(
            f"cannot instantiate driver object in {driver}"
        ) from exc


def driver_metatata_from_obj(obj, existing=None):
    if inspect.isclass(obj) and hasattr(obj, "__mp_driver__"):
        obj_metadata = obj.__mp_driver__
        existing_metadata = existing.dict() if existing else {}
        mode = [existing_metadata.pop("mode", "")]
        # copy over from object metadata
        new_metadata = dict(
            existing_metadata,
            name=obj_metadata.name,
            data_type=obj_metadata.data_type,
            file_extensions=obj_metadata.file_extensions,
        )
        if obj_metadata.input_reader:
            mode.append("r")
            new_metadata.update(input_reader_cls=obj)
        if obj_metadata.output_reader:
            mode.append("r")
            new_metadata.update(output_reader_cls=obj)
        if obj_metadata.output_writer:
            mode.append("w")
            new_metadata.update(output_writer_cls=obj)
        new_metadata.update(mode="".join(sorted(set(mode))).strip())
        return DriverMetadata(**new_metadata)
    else:
        raise TypeError("object is not a valid mapchete driver")


def driver_metatata_from_obj_legacy(obj_name, obj, existing=None):
    if hasattr(obj, "METADATA"):
        obj_metadata = obj.METADATA
        existing_metadata = existing.dict() if existing else {}
        mode = [existing_metadata.pop("mode", "")]
        # copy over from object metadata
        new_metadata = dict(
            existing_metadata,
            name=obj_metadata["driver_name"],
            data_type=obj_metadata["data_type"],
            file_extensions=obj_metadata.get("file_extensions"),
        )
        if obj_name == "InputData":
            mode.append("r")
            new_metadata.update(input_reader_cls=obj)
        if obj_name == "OutputDataReader":
            mode.append("r")
            new_metadata.update(output_reader_cls=obj)
        if obj_name == "OutputDataWriter":
            mode.append("w")
            new_metadata.update(output_writer_cls=obj)
        new_metadata.update(mode="".join(sorted(set(mode))).strip())
        return DriverMetadata(**new_metadata)
    else:
        raise TypeError("object is not a valid mapchete driver")


def detect_driver_classes() -> Dict:
    if not _DETECTED_DRIVERS:

        for v in DRIVER_MODULES:
            driver_ = v.load()

            for name, obj in inspect.getmembers(driver_):

                # detecting decorated driver
                if inspect.isclass(obj) and hasattr(obj, "__mp_driver__"):
                    driver_name = obj.__mp_driver__.name
                    _DETECTED_DRIVERS[driver_name] = driver_metatata_from_obj(
                        obj, existing=_DETECTED_DRIVERS.get(driver_name)
                    )

                # legacy detection
                elif (
                    inspect.isclass(obj)
                    and name in ["InputData", "OutputDataReader", "OutputDataWriter"]
                    and hasattr(obj, "METADATA")
                    and obj.METADATA.get("driver_name") is not None
                ):
                    driver_name = obj.METADATA["driver_name"]
                    try:
                        _DETECTED_DRIVERS[
                            driver_name
                        ] = driver_metatata_from_obj_legacy(
                            name, obj, existing=_DETECTED_DRIVERS.get(driver_name)
                        )
                    except Exception as exc:
                        logger.error(
                            "cannot load legacy driver %s because %s", driver_name, exc
                        )

    return _DETECTED_DRIVERS


def available_output_formats() -> Dict:
    """
    Return all available output formats.

    Returns
    -------
    formats : list
        all available output formats
    """
    return {k: v for k, v in detect_driver_classes().items() if "w" in v.mode}


def available_input_formats() -> Dict:
    """
    Return all available input formats.

    Returns
    -------
    formats : list
        all available input formats
    """
    return {k: v for k, v in detect_driver_classes().items() if "r" in v.mode}


def driver_metadata(driver_name: str) -> Dict:
    """
    Return driver metadata.

    Parameters
    ----------
    driver_name : str
        Name of driver.

    Returns
    -------
    Driver metadata as dictionary.
    """
    for v in DRIVER_MODULES:
        driver_ = v.load()
        if hasattr(driver_, "METADATA") and (
            driver_.METADATA["driver_name"] == driver_name
        ):
            return dict(driver_.METADATA)
    else:  # pragma: no cover
        raise ValueError(f"driver '{driver_name}' not found")


def driver_from_file(input_file: str, quick: bool = True) -> str:
    """
    Guess driver from file by opening it.

    Parameters
    ----------
    input_file : str
        Path to file.
    quick : bool
        Try to guess driver from known file extensions instead of trying to open with
        fiona and rasterio. (default: True)

    Returns
    -------
    driver : string
        driver name
    """
    file_ext = os.path.splitext(input_file)[1].split(".")[1]

    # mapchete files can immediately be returned:
    if file_ext == "mapchete":
        return "Mapchete"

    # use the most common file extensions to quickly determine input driver for file:
    if quick:
        try:
            return driver_from_extension(file_ext)
        except ValueError:
            pass

    # brute force by trying to open file with rasterio and fiona:
    try:
        logger.debug("try to open %s with rasterio...", input_file)
        with rasterio.open(input_file):  # pragma: no cover
            return "raster_file"
    except Exception as rio_exception:
        try:
            logger.debug("try to open %s with fiona...", input_file)
            with fiona.open(input_file):  # pragma: no cover
                return "vector_file"
        except Exception as fio_exception:
            if path_exists(input_file):
                logger.exception(f"fiona error: {fio_exception}")
                logger.exception(f"rasterio error: {rio_exception}")
                raise MapcheteDriverError(
                    "%s has an unknown file extension and could not be opened by neither "
                    "rasterio nor fiona." % input_file
                )
            else:
                raise FileNotFoundError("%s does not exist" % input_file)


def driver_from_extension(file_extension: str) -> str:
    """
    Guess driver name from file extension.

    Paramters
    ---------
    file_extension : str
        File extension to look for.

    Returns
    -------
    driver : string
        driver name
    """
    all_drivers_extensions = {}
    for v in DRIVER_MODULES:
        driver = v.load()
        try:
            driver_extensions = driver.METADATA.get("file_extensions", []).copy()
            all_drivers_extensions[driver.METADATA["driver_name"]] = driver_extensions
            if driver_extensions and file_extension in driver_extensions:
                return driver.METADATA["driver_name"]
        except AttributeError:  # pragma: no cover
            pass
    else:
        raise ValueError(
            f"driver name for file extension {file_extension} could not be found: {all_drivers_extensions}"
        )


def data_type_from_extension(file_extension: str) -> str:
    """
    Guess data_type (raster or vector) from file extension.

    Paramters
    ---------
    file_extension : str
        File extension to look for.

    Returns
    -------
    driver data type : string
        driver data type
    """
    for v in DRIVER_MODULES:
        driver = v.load()
        try:
            driver_extensions = driver.METADATA.get("file_extensions", [])
            if driver_extensions and file_extension in driver_extensions:
                return driver.METADATA["data_type"]
        except AttributeError:  # pragma: no cover
            pass
    else:
        raise ValueError(
            f"data type for file extension {file_extension} could not be found"
        )
