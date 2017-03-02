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
import numpy as np
import numpy.ma as ma
from PIL import Image
from flask import send_file

from mapchete.formats import base
from mapchete.tile import BufferedTile
from mapchete.io.raster import write_raster_window


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
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        self.verify_data(process_tile)
        data = self.prepare_data(process_tile.data)
        if len(data) == 1:
            r = data[0]
            g = data[0]
            b = data[0]
        elif len(data) == 3:
            r, g, b = data
        else:
            raise TypeError("invalid number of bands: %s" % len(data))
        # Generate alpha channel out of mask or nodata values.
        a = np.where(r.mask, 0, 255).astype("uint8")
        # Create 3D NumPy array with alpha channel.
        stacked = np.stack((r, g, b, a))
        process_tile.data = ma.masked_array(
            data=stacked,
            mask=np.where(stacked == self.nodata, True, False))
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
        process output : array
        """
        try:
            with rasterio.open(self.get_path(output_tile)) as src:
                data = src.read([1, 2, 3])
                mask = np.where(src.read(4) == 255, False, True)
                output_tile.data = ma.MaskedArray(
                    data=data,
                    mask=np.stack((mask, mask, mask, ))
                )
        except:
            output_tile.data = self.empty(output_tile)
        return output_tile

    def tiles_exist(self, process_tile):
        """
        Check whether all output tiles of a process tile exist.

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
        zoomdir = os.path.join(self.path, str(tile.zoom))
        rowdir = os.path.join(zoomdir, str(tile.row))
        return os.path.join(rowdir, str(tile.col) + self.file_extension)

    def prepare_path(self, tile):
        """
        Create directory and subdirectory if necessary.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``
        """
        zoomdir = os.path.join(self.path, str(tile.zoom))
        if not os.path.exists(zoomdir):
            os.makedirs(zoomdir)
        rowdir = os.path.join(zoomdir, str(tile.row))
        if not os.path.exists(rowdir):
            os.makedirs(rowdir)

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
        return dst_metadata

    def verify_data(self, tile):
        """
        Verify array data and move array into tuple if necessary.

        Parameters
        ----------
        tile : ``BufferedTile``

        Returns
        -------
        valid : bool
        """
        try:
            assert isinstance(
                tile.data, (np.ndarray, ma.MaskedArray, tuple, list))
        except AssertionError:
            raise ValueError(
                "process output must be 2D NumPy array, masked array or a tuple"
                )
        if isinstance(tile.data, (tuple, list)):
            try:
                assert len(tile.data) == 3
            except AssertionError:
                raise ValueError(
                    """only three bands (red, green, blue) allowed for """
                    """process output, not %s""" %
                    len(tile.data))
        for band in tile.data:
            try:
                assert band.ndim == 2
            except AssertionError:
                raise ValueError("each output band must be a 2D NumPy array")

    def prepare_data(self, data):
        """
        Convert data into correct output.

        Parameters
        ----------
        data : array
        profile : dictionary

        Returns
        -------
        prepared_data : array
            a 3D masked NumPy array as 8 bit unsigned integer
        """
        if isinstance(data, (list, tuple)):
            out_data = ()
            out_mask = ()
            for band in data:
                if isinstance(band, ma.MaskedArray):
                    try:
                        assert band.shape == band.mask.shape
                        out_data += (band, )
                        out_mask += (band.mask, )
                    except:
                        out_data += (band.data, )
                        out_mask += (
                            np.where(band.data == self.nodata, True, False), )
                elif isinstance(band, np.ndarray):
                    out_data += (band)
                    out_mask += (np.where(band == self.nodata, True, False))
                else:
                    raise ValueError("input data bands must be NumPy arrays")
            assert len(out_data) == len(out_mask) == 3
            return ma.MaskedArray(
                data=np.stack(out_data).astype("uint8"),
                mask=np.stack(out_mask))
        elif isinstance(data, ma.MaskedArray):
            if data.ndim == 2:
                data = data[np.newaxis, :]
            assert len(data) <= 3
            try:
                assert data.shape == data.mask.shape
                return data.astype("uint8")
            except:
                return ma.MaskedArray(
                    data=data.astype("uint8"),
                    mask=np.where(data == self.nodata, True, False),
                    fill_value=self.nodata)
        elif isinstance(data, np.ndarray):
            if data.ndim == 2:
                data = data[np.newaxis, :]
            assert len(data) <= 3
            return ma.MaskedArray(
                data=data.astype("uint8"),
                mask=np.where(data == self.nodata, True, False),
                fill_value=self.nodata)

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
        data = self.prepare_data(data)
        if len(data) == 1:
            r = data[0]
            g = data[0]
            b = data[0]
        elif len(data) == 3:
            r, g, b = data
        else:
            raise TypeError("invalid number of bands: %s" % len(data))
        # Generate alpha channel out of mask or nodata values.
        a = np.where(r.mask, 0, 255).astype("uint8")
        # Create 3D NumPy array with alpha channel.
        rgba = np.stack((r, g, b, a))
        reshaped = rgba.transpose(1, 2, 0)
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
        empty data : array
            empty array with data type given in output parameters
        """
        return ma.masked_array(
            data=ma.zeros((3, ) + process_tile.shape),
            mask=ma.ones((3, ) + process_tile.shape),
            dtype=PNG_PROFILE["dtype"]
        )


PNG_PROFILE = {
    "dtype": "uint8",
    "driver": "PNG",
    "count": 4,
    "nodata": 0
}
