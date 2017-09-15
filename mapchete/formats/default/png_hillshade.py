"""
Special PNG process output for hillshades.

Writes inverted hillshade into alpha channel of black image, so that hillshade
can be used as overlay over other data.

output configuration parameters
-------------------------------

mandatory
~~~~~~~~~

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
    "driver_name": "PNG_hillshade",
    "data_type": "raster",
    "mode": "w"
}


class OutputData(base.OutputData):
    """
    PNG_hillshade output class.

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
    old_band_num : bool
        in prior versions, 4 channels (3x gray 1x alpha) were written, now
        2 channels (1x gray, 1x alpha)
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
        "driver_name": "PNG_hillshade",
        "data_type": "raster",
        "mode": "w"
    }

    def __init__(self, output_params):
        """Initialize."""
        super(OutputData, self).__init__(output_params)
        self.path = output_params["path"]
        self.file_extension = ".png"
        self.output_params = output_params
        self.nodata = PNG_PROFILE["nodata"]
        try:
            self.old_band_num = output_params["old_band_num"]
            PNG_PROFILE.update(count=4)
        except KeyError:
            self.old_band_num = False

    def write(self, process_tile):
        """
        Write data from process tiles into PNG file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        data = prepare_array(
            process_tile.data, dtype="uint8", masked=False, nodata=0)
        if self.old_band_num:
            data = np.stack((
                np.zeros(process_tile.shape), np.zeros(process_tile.shape),
                np.zeros(process_tile.shape), data[0]))
        else:
            data = np.stack((
                np.zeros(process_tile.shape), data[0]))
        process_tile.data = prepare_array(
            data, dtype="uint8", masked=True, nodata=255)
        # Convert from process_tile to output_tiles
        for tile in self.pyramid.intersecting(process_tile):
            # skip if file exists and overwrite is not set
            out_path = self.get_path(tile)
            self.prepare_path(tile)
            out_tile = BufferedTile(tile, self.pixelbuffer)
            write_raster_window(
                in_tile=process_tile, out_profile=self.profile(out_tile),
                out_tile=out_tile, out_path=out_path)

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
        if self.old_band_num:
            band_num = 4
        else:
            band_num = 2
        try:
            with rasterio.open(self.get_path(output_tile)) as src:
                output_tile.data = ma.masked_values(src.read(band_num), 0)
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
            width=tile.width,
            height=tile.height,
            affine=tile.affine, driver="PNG",
            crs=tile.crs
        )
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
        data = prepare_array(data, masked=False, dtype="uint8")[0]
        zeros = np.zeros(data.shape)
        out_rgb = (zeros, zeros, zeros, data)
        reshaped = np.stack(out_rgb).transpose(1, 2, 0).astype("uint8")
        empty_image = Image.fromarray(reshaped, mode='RGBA')
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
        empty data : array or list
            empty array with correct data type for raster data or empty list
            for vector data
        """
        return ma.masked_values(np.zeros(process_tile.shape), 0)


PNG_PROFILE = {
    "dtype": "uint8",
    "driver": "PNG",
    "count": 2,
    "nodata": 255
}
