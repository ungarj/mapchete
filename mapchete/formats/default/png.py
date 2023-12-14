"""
PNG process output.

output configuration parameters
-------------------------------

mandatory
~~~~~~~~~

bands: integer
    number of output bands to be written
path: string
    output directory

optional
~~~~~~~~

nodata: integer or float
    nodata value used for writing
"""

import logging

import numpy as np
import numpy.ma as ma

from mapchete.formats import base
from mapchete.io.raster import (
    memory_file,
    prepare_array,
    read_raster_no_crs,
    write_raster_window,
)
from mapchete.path import MPath
from mapchete.tile import BufferedTile
from mapchete.validate import validate_values

logger = logging.getLogger(__name__)
METADATA = {"driver_name": "PNG", "data_type": "raster", "mode": "w"}
PNG_DEFAULT_PROFILE = {"dtype": "uint8", "driver": "PNG", "count": 4, "nodata": 0}


class OutputDataReader(base.TileDirectoryOutputReader):
    """
    PNG output class.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    path : string
        path to output directory
    file_extension : string
        file extension for output files (.png)
    output_params : dictionary
        output parameters from Mapchete file
    nodata : integer or float
        nodata value used when writing PNGs
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    srid : string
        spatial reference ID of CRS (e.g. "{'init': 'epsg:4326'}")
    """

    METADATA = METADATA

    def __init__(self, output_params, **kwargs):
        """Initialize."""
        super().__init__(output_params)
        self.path = output_params["path"]
        self.file_extension = ".png"
        self.output_params = dict(
            output_params,
            nodata=output_params.get("nodata", PNG_DEFAULT_PROFILE["nodata"]),
            dtype=PNG_DEFAULT_PROFILE["dtype"],
        )

    def read(self, output_tile, **kwargs):
        """
        Read existing process output.

        Parameters
        ----------
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        process output : ``BufferedTile`` with appended data
        """
        try:
            return read_raster_no_crs(self.get_path(output_tile))
        except FileNotFoundError:
            return self.empty(output_tile)

    def is_valid_with_config(self, config):
        """
        Check if output format is valid with other process parameters.

        Parameters
        ----------
        config : dictionary
            output configuration parameters

        Returns
        -------
        is_valid : bool
        """
        return validate_values(config, [("path", (str, MPath))])

    def profile(self, tile=None):
        """
        Create a metadata dictionary for rasterio.

        Parameters
        ----------
        tile : ``BufferedTile``

        Returns
        -------
        metadata : dictionary
            output profile dictionary used for rasterio.
        """
        dst_metadata = PNG_DEFAULT_PROFILE
        dst_metadata.pop("transform", None)
        if tile is not None:
            dst_metadata.update(
                width=tile.width, height=tile.height, affine=tile.affine, crs=tile.crs
            )
        try:
            dst_metadata.update(count=self.output_params["count"])
        except KeyError:
            pass
        return dst_metadata

    def for_web(self, data):
        """
        Convert data to web output.

        Parameters
        ----------
        data : array

        Returns
        -------
        web data : array
        """
        rgba = self._prepare_array_for_png(data)
        data = ma.masked_where(rgba == self.output_params["nodata"], rgba)
        return memory_file(data, self.profile()), "image/png"

    def empty(self, process_tile):
        """
        Return empty data.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        empty data : array
            empty array with data type given in output parameters
        """
        bands = (
            self.output_params["bands"]
            if "bands" in self.output_params
            else PNG_DEFAULT_PROFILE["count"]
        )
        return ma.masked_array(
            data=ma.zeros((bands,) + process_tile.shape),
            mask=ma.zeros((bands,) + process_tile.shape),
            dtype=PNG_DEFAULT_PROFILE["dtype"],
        )

    def _prepare_array_for_png(self, data):
        data = prepare_array(data, dtype=np.uint8)
        # Create 3D NumPy array with alpha channel.
        if len(data) == 1:
            rgba = np.stack(
                (
                    data[0],
                    data[0],
                    data[0],
                    np.where(
                        data[0].data == self.output_params["nodata"], 0, 255
                    ).astype("uint8"),
                )
            )
        elif len(data) == 2:
            rgba = np.stack((data[0], data[0], data[0], data[1]))
        elif len(data) == 3:
            rgba = np.stack(
                (
                    data[0],
                    data[1],
                    data[2],
                    np.where(
                        data[0].data == self.output_params["nodata"], 0, 255
                    ).astype("uint8", copy=False),
                )
            )
        elif len(data) == 4:
            rgba = np.array(data).astype("uint8", copy=False)
        else:
            raise TypeError("invalid number of bands: %s" % len(data))
        return rgba


class OutputDataWriter(base.OutputDataWriter, OutputDataReader):
    METADATA = METADATA

    def write(self, process_tile, data):
        """
        Write data from one or more process tiles.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        rgba = self._prepare_array_for_png(data)
        data = ma.masked_where(rgba == self.output_params["nodata"], rgba)

        if data.mask.all():
            logger.debug("data empty, nothing to write")
        else:
            # Convert from process_tile to output_tiles and write
            for tile in self.pyramid.intersecting(process_tile):
                out_path = self.get_path(tile)
                self.prepare_path(tile)
                out_tile = BufferedTile(tile, self.pixelbuffer)
                write_raster_window(
                    in_grid=process_tile,
                    in_data=data,
                    out_profile=self.profile(out_tile),
                    out_grid=out_tile,
                    out_path=out_path,
                )
