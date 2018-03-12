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
import six
import rasterio
from rasterio.errors import RasterioIOError
import numpy as np
import numpy.ma as ma

from mapchete.formats import base
from mapchete.tile import BufferedTile
from mapchete.io.raster import write_raster_window, prepare_array, memory_file
from mapchete.config import validate_values


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
        self.output_params.update(dtype=PNG_PROFILE["dtype"])

    def write(self, process_tile, data):
        """
        Write data from process tiles into PNG file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        data = self._prepare_array(data)
        # Convert from process_tile to output_tiles
        for tile in self.pyramid.intersecting(process_tile):
            # skip if file exists and overwrite is not set
            out_path = self.get_path(tile)
            self.prepare_path(tile)
            out_tile = BufferedTile(tile, self.pixelbuffer)
            write_raster_window(
                in_tile=process_tile, in_data=data,
                out_profile=self.profile(out_tile), out_tile=out_tile,
                out_path=out_path)

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
                return ma.masked_values(
                    src.read(4 if self.old_band_num else 2), 0)
        except RasterioIOError:
            return self.empty(output_tile)
        return output_tile

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
        return validate_values(config, [("path", six.string_types)])

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
            str(tile.col) + self.file_extension])

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
        dst_metadata = PNG_PROFILE
        dst_metadata.pop("transform", None)
        if tile is not None:
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
        MemoryFile(), MIME type
        """
        return (
            memory_file(self._prepare_array(data), self.profile()), "image/png"
        )

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

    def _prepare_array(self, data):
        data = prepare_array(
            -(data - 255), dtype="uint8", masked=False, nodata=0)
        if self.old_band_num:
            data = np.stack((
                np.zeros(data[0].shape), np.zeros(data[0].shape),
                np.zeros(data[0].shape), data[0]))
        else:
            data = np.stack((np.zeros(data[0].shape), data[0]))
        return prepare_array(data, dtype="uint8", masked=True, nodata=255)


PNG_PROFILE = {
    "dtype": "uint8",
    "driver": "PNG",
    "count": 2,
    "nodata": 255
}
