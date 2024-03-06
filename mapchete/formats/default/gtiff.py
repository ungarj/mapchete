"""
Handles writing process output into a pyramid of GeoTIFF files or a single GeoTIFF file.

output configuration parameters
-------------------------------

mandatory
~~~~~~~~~

bands: integer
    number of output bands to be written
path: string
    output directory
dtype: string
    numpy datatype

optional
~~~~~~~~

tiled: bool
    internal TIFF tiling (default: True)
blockxsize: integer
    internal tile width (default: 256)
blockysize:
    internal tile height (default: 256)
nodata: integer or float
    nodata value used for writing
compress: string
    compression method (default: lzw): lzw, jpeg, packbits, deflate, CCITTRLE,
    CCITTFAX3, CCITTFAX4, lzma
"""
from __future__ import annotations

import logging
import os

from mapchete.formats.raster.single_file import RasterSingleFileOutputWriter
from mapchete.formats.raster.tile_directory import (
    RasterTileDirectoryOutputReader,
    RasterTileDirectoryOutputWriter,
)

logger = logging.getLogger(__name__)


METADATA = {"driver_name": "GTiff", "data_type": "raster", "mode": "rw"}
IN_MEMORY_THRESHOLD = int(os.environ.get("MP_IN_MEMORY_THRESHOLD", 20000 * 20000))


class OutputDataReader:
    """
    Constructor class which returns GTiffTileDirectoryOutputReader.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    path : string
        path to output directory
    file_extension : string
        file extension for output files (.tif)
    output_params : dictionary
        output parameters from Mapchete file
    nodata : integer or float
        nodata value used when writing GeoTIFFs
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    srid : string
        spatial reference ID of CRS (e.g. "{'init': 'epsg:4326'}")
    """

    def __new__(cls, output_params, **kwargs):
        """Initialize."""
        return RasterTileDirectoryOutputReader(
            output_params, driver="GTiff", file_extension=".tif", **kwargs
        )


class OutputDataWriter:
    """
    Constructor class which either returns GTiffSingleFileOutputWriter or
    GTiffTileDirectoryOutputWriter.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    path : string
        path to output directory
    file_extension : string
        file extension for output files (.tif)
    output_params : dictionary
        output parameters from Mapchete file
    nodata : integer or float
        nodata value used when writing GeoTIFFs
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    srid : string
        spatial reference ID of CRS (e.g. "{'init': 'epsg:4326'}")
    """

    def __new__(cls, output_params, **kwargs):
        """Initialize."""
        path = output_params["path"]
        file_extension = ".tif"
        if path.suffix == file_extension:
            return RasterSingleFileOutputWriter(output_params, driver="COG", **kwargs)
        else:
            return RasterTileDirectoryOutputWriter(
                output_params, file_extension=file_extension, **kwargs
            )
