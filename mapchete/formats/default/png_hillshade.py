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

import logging
import numpy as np
import numpy.ma as ma

from mapchete.config import validate_values
from mapchete.formats import base
from mapchete.io import get_boto3_bucket
from mapchete.io.raster import (
    write_raster_window, prepare_array, memory_file, read_raster_no_crs
)
from mapchete.tile import BufferedTile


logger = logging.getLogger(__name__)
METADATA = {
    "driver_name": "PNG_hillshade",
    "data_type": "raster",
    "mode": "w"
}
PNG_DEFAULT_PROFILE = {
    "dtype": "uint8",
    "driver": "PNG",
    "count": 2,
    "nodata": 255
}


class OutputDataReader(base.TileDirectoryOutputReader):
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

    METADATA = METADATA

    def __init__(self, output_params, **kwargs):
        """Initialize."""
        super().__init__(output_params)
        self.path = output_params["path"]
        self.file_extension = ".png"
        self.output_params = dict(
            output_params,
            nodata=output_params.get("nodata", PNG_DEFAULT_PROFILE["nodata"]),
            dtype=PNG_DEFAULT_PROFILE["dtype"]
        )
        self._profile = dict(PNG_DEFAULT_PROFILE)
        try:
            self.old_band_num = output_params["old_band_num"]
            self._profile.update(count=4)
        except KeyError:
            self.old_band_num = False
        self.output_params.update(dtype=self._profile["dtype"])
        self._bucket = self.path.split("/")[2] if self.path.startswith("s3://") else None

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
            return ma.masked_values(
                read_raster_no_crs(
                    self.get_path(output_tile), indexes=(4 if self.old_band_num else 2)
                ),
                0
            )
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
        return validate_values(config, [("path", str)])

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
        dst_metadata = dict(self._profile)
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
        )  # pragma: no cover

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
        data = prepare_array(-(data - 255), dtype="uint8", masked=False, nodata=0)[0]
        zeros = np.zeros(data.shape)
        if self.old_band_num:
            data = np.stack([zeros, zeros, zeros, data])
        else:
            data = np.stack([zeros, data])
        return prepare_array(data, dtype="uint8", masked=True, nodata=255)


class OutputDataWriter(base.OutputDataWriter, OutputDataReader):

    METADATA = METADATA

    def write(self, process_tile, data):
        """
        Write data from process tiles into PNG file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        data = self._prepare_array(data)

        if data.mask.all():  # pragma: no cover
            logger.debug("data empty, nothing to write")
        else:
            # in case of S3 output, create an boto3 resource
            bucket_resource = get_boto3_bucket(self._bucket) if self._bucket else None

            # Convert from process_tile to output_tiles and write
            for tile in self.pyramid.intersecting(process_tile):
                out_path = self.get_path(tile)
                self.prepare_path(tile)
                out_tile = BufferedTile(tile, self.pixelbuffer)
                write_raster_window(
                    in_tile=process_tile,
                    in_data=data,
                    out_profile=self.profile(out_tile),
                    out_tile=out_tile,
                    out_path=out_path,
                    bucket_resource=bucket_resource
                )
