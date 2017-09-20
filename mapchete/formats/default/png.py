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

import os
import io
import rasterio
from rasterio.errors import RasterioIOError
import numpy as np
import numpy.ma as ma
from PIL import Image
from flask import send_file

from mapchete.formats import base
from mapchete.tile import BufferedTile
from mapchete.io.raster import write_raster_window, prepare_array


METADATA = {
    "driver_name": "PNG",
    "data_type": "raster",
    "mode": "w"
}


class OutputData(base.OutputData):
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

    METADATA = {
        "driver_name": "PNG",
        "data_type": "raster",
        "mode": "w"
    }

    def __init__(self, output_params):
        """Initialize."""
        super(OutputData, self).__init__(output_params)
        self.path = output_params["path"]
        self.file_extension = ".png"
        self.output_params = output_params
        self.output_params["dtype"] = PNG_PROFILE["dtype"]
        try:
            self.nodata = output_params["nodata"]
        except KeyError:
            self.nodata = PNG_PROFILE["nodata"]

    def write(self, process_tile):
        """
        Write data from one or more process tiles.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        rgba = self._prepare_array_for_png(process_tile.data)
        process_tile.data = ma.masked_where(rgba == self.nodata, rgba)
        # Convert from process_tile to output_tiles
        for tile in self.pyramid.intersecting(process_tile):
            # skip if file exists and overwrite is not set
            self.prepare_path(tile)
            out_tile = BufferedTile(tile, self.pixelbuffer)
            write_raster_window(
                in_tile=process_tile,
                out_tile=BufferedTile(tile, self.pixelbuffer),
                out_profile=self.profile(out_tile),
                out_path=self.get_path(tile)
            )

    def read(self, output_tile):
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
            with rasterio.open(self.get_path(output_tile)) as src:
                output_tile.data = src.read(masked=True)
        except RasterioIOError:
            output_tile.data = self.empty(output_tile)
        return output_tile

    def tiles_exist(self, process_tile):
        """
        Check whether output tiles of a process tile exist.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        exists : bool
        """
        return any(
            os.path.exists(self.get_path(tile))
            for tile in self.pyramid.intersecting(process_tile)
        )

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
        assert isinstance(config, dict)
        assert "path" in config
        assert isinstance(config["path"], str)
        return True

    def get_path(self, tile):
        """
        Determine target file path.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        path : string
        """
        return os.path.join(*[
            self.path, str(tile.zoom), str(tile.row),
            str(tile.col)+self.file_extension])

    def prepare_path(self, tile):
        """
        Create directory and subdirectory if necessary.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``
        """
        try:
            os.makedirs(os.path.dirname(self.get_path(tile)))
        except OSError:
            pass

    def profile(self, tile):
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
        dst_metadata = PNG_PROFILE
        dst_metadata.pop("transform", None)
        dst_metadata.update(
            width=tile.width, height=tile.height, affine=tile.affine,
            driver="PNG", crs=tile.crs)
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
        empty_image = Image.fromarray(rgba.transpose(1, 2, 0), mode='RGBA')
        out_img = io.BytesIO()
        empty_image.save(out_img, 'PNG')
        out_img.seek(0)
        return send_file(out_img, mimetype='image/png')

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
            else PNG_PROFILE["count"]
        )
        return ma.masked_array(
            data=ma.zeros((bands, ) + process_tile.shape),
            mask=ma.zeros((bands, ) + process_tile.shape),
            dtype=PNG_PROFILE["dtype"]
        )

    def _prepare_array_for_png(self, data):
        data = prepare_array(data, dtype=np.uint8)
        # Create 3D NumPy array with alpha channel.
        if len(data) == 1:
            rgba = np.stack((
                data[0], data[0], data[0],
                np.where(
                    data[0].data == self.nodata, 0, 255)
                .astype("uint8")
            ))
        elif len(data) == 2:
            rgba = np.stack((data[0], data[0], data[0], data[1]))
        elif len(data) == 3:
            rgba = np.stack((
                data[0], data[1], data[2], np.where(
                    data[0].data == self.nodata, 0, 255
                ).astype("uint8")
            ))
        elif len(data) == 4:
            rgba = np.array(data).astype("uint8")
        else:
            raise TypeError("invalid number of bands: %s" % len(data))
        return rgba


PNG_PROFILE = {
    "dtype": "uint8",
    "driver": "PNG",
    "count": 4,
    "nodata": 0
}
