"""
Generic driver for any data type (raster or vector that is) stored as a
TileDirectory.
"""
from __future__ import annotations

import logging
from typing import Union

from mapchete.enums import DataType
from mapchete.errors import MapcheteConfigError
from mapchete.formats import (
    base,
    data_type_from_extension,
    driver_metadata,
    read_output_metadata,
)
from mapchete.formats.default.raster.tile_directory import RasterTileDirectory
from mapchete.formats.default.vector.tile_directory import VectorTileDirectory
from mapchete.path import MPath
from mapchete.validate import validate_values

logger = logging.getLogger(__name__)
METADATA = {
    "driver_name": "TileDirectory",
    "data_type": None,
    "mode": "r",
    "file_extensions": None,
}


class InputData:
    """Factory class to decide whether to return a TileDirectory for raster or vector data."""

    METADATA = METADATA

    def __new__(
        cls, input_params: dict, **kwargs
    ) -> Union[RasterTileDirectory, VectorTileDirectory]:
        """Initialize."""
        logger.debug("InputData params: %s", input_params)
        # populate internal parameters initially depending on whether this input was
        # defined as simple or abstract input
        params = input_params.get("abstract") or dict(path=input_params["path"])
        # construct path and append optional filesystem options
        path = MPath.from_inp(params).absolute_path(input_params.get("conf_dir"))
        if "extension" in params:
            data_type = data_type_from_extension(params["extension"])
        else:
            try:
                tiledir_metadata_json = read_output_metadata(path / "metadata.json")
            except FileNotFoundError:
                # in case no metadata.json is available, try to guess data type via the
                # format file extension
                raise MapcheteConfigError(
                    f"data type not defined and cannot find metadata.json in {path}"
                )
            data_type = tiledir_metadata_json["driver"].get(
                "data_type",
                driver_metadata(tiledir_metadata_json["driver"]["format"])["data_type"],
            )
        if data_type == DataType.raster:
            params["count"] = params.get("count", params.get("bands", None))
            validate_values(params, [("dtype", str), ("count", int)])
            return RasterTileDirectory(
                params=params,
                nodata=params.get("nodata", 0),
                dtype=params["dtype"],
                count=params["count"],
            )
        else:
            return VectorTileDirectory(params=params)
