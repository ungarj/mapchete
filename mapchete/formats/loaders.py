import logging
from typing import Optional

from mapchete.errors import MapcheteDriverError
from mapchete.formats.protocols import (
    InputDataProtocol,
    OutputDataReaderProtocol,
    OutputDataWriterProtocol,
)
from mapchete.formats.tools import driver_from_file
from mapchete.registered import drivers

logger = logging.getLogger(__name__)


def load_output_reader(output_params: dict) -> OutputDataReaderProtocol:
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
    for v in drivers:
        _driver = v.load()
        if all(
            [hasattr(_driver, attr) for attr in ["OutputDataReader", "METADATA"]]
        ) and (_driver.METADATA["driver_name"] == driver_name):
            return _driver.OutputDataReader(output_params, readonly=True)
    raise MapcheteDriverError("no loader for driver '%s' could be found." % driver_name)


def load_output_writer(
    output_params: dict, readonly: bool = False
) -> OutputDataWriterProtocol:
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
    for v in drivers:
        _driver = v.load()
        if all(
            [hasattr(_driver, attr) for attr in ["OutputDataWriter", "METADATA"]]
        ) and (_driver.METADATA["driver_name"] == driver_name):
            return _driver.OutputDataWriter(output_params, readonly=readonly)
    raise MapcheteDriverError("no loader for driver '%s' could be found." % driver_name)


def load_input_reader(
    input_params: dict, readonly: bool = False, input_key: Optional[str] = None
) -> InputDataProtocol:
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
        if input_params["path"].suffix:
            input_file = input_params["path"]
            driver_name = driver_from_file(input_file)
        # else a TileDirectory is assumed
        else:
            logger.debug("%s is a directory", input_params["path"])
            driver_name = "TileDirectory"
    else:
        raise MapcheteDriverError("invalid input parameters %s" % input_params)

    # load this driver module and try to initialize an InputData object
    for v in drivers:
        driver_ = v.load()
        if hasattr(driver_, "METADATA") and (
            driver_.METADATA["driver_name"] == driver_name
        ):
            return driver_.InputData(
                input_params, readonly=readonly, input_key=input_key
            )
    raise MapcheteDriverError("no loader for driver '%s' could be found." % driver_name)
