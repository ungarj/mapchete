"""PNG process output."""

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
    """Main output class."""

    METADATA = {
        "driver_name": "PNG",
        "data_type": "raster",
        "mode": "w"
    }

    def __init__(self, output_params):
        """Initialize."""
        super(OutputData, self).__init__(output_params)
        self.path = output_params["path"]
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        self.file_extension = ".png"
        self.output_params = output_params
        try:
            self.nodata = output_params["nodata"]
        except KeyError:
            self.nodata = PNG_PROFILE["nodata"]

    def write(self, process_tile, overwrite=False):
        """Write process output into PNGs."""
        self.verify_data(process_tile)
        r, g, b = self.prepare_data(process_tile.data)
        # Generate alpha channel out of mask or nodata values.
        a = np.where(r.mask, 0, 255).astype("uint8")
        # Create 3D NumPy array with alpha channel.
        process_tile.data = ma.masked_array(
            data=np.stack((r, g, b, a)),
            mask=np.zeros((4, ) + process_tile.shape))
        # Convert from process_tile to output_tiles
        for tile in self.pyramid.intersecting(process_tile):
            # skip if file exists and overwrite is not set
            out_path = self.get_path(tile)
            if os.path.exists(out_path) and not overwrite:
                return
            self.prepare_path(tile)
            out_tile = BufferedTile(tile, self.pixelbuffer)
            write_raster_window(
                in_tile=process_tile, out_profile=self.profile(out_tile),
                out_tile=out_tile, out_path=out_path)

    def read(self, output_tile):
        """Read process output tile into numpy array."""
        try:
            with rasterio.open(self.get_path(output_tile)) as src:
                data = src.read([1, 2, 3])
                mask = src.read(4)
                mask = np.where(src.read(4) == 255, False, True)
                output_tile.data = ma.MaskedArray(
                    data=data,
                    mask=np.stack((mask, mask, mask, ))
                )
        except:
            output_tile.data = self.empty(output_tile)
        return output_tile

    def tiles_exist(self, process_tile):
        """Check whether all output tiles of a process tile exist."""
        return any(
            os.path.exists(self.get_path(tile))
            for tile in self.pyramid.intersecting(process_tile)
        )

    def is_valid_with_config(self, config):
        """Check if output format is valid with other process parameters."""
        assert isinstance(config, dict)
        assert "path" in config
        assert isinstance(config["path"], str)
        return True

    def get_path(self, tile):
        """Determine target file path."""
        zoomdir = os.path.join(self.path, str(tile.zoom))
        rowdir = os.path.join(zoomdir, str(tile.row))
        return os.path.join(rowdir, str(tile.col) + self.file_extension)

    def prepare_path(self, tile):
        """Create directory and subdirectory if necessary."""
        zoomdir = os.path.join(self.path, str(tile.zoom))
        if not os.path.exists(zoomdir):
            os.makedirs(zoomdir)
        rowdir = os.path.join(zoomdir, str(tile.row))
        if not os.path.exists(rowdir):
            os.makedirs(rowdir)

    def profile(self, tile):
        """Create a metadata dictionary for rasterio."""
        dst_metadata = PNG_PROFILE
        dst_metadata.pop("transform", None)
        dst_metadata.update(
            width=tile.width, height=tile.height, affine=tile.affine,
            driver="PNG", crs=tile.crs)
        return dst_metadata

    def verify_data(self, tile):
        """Verify array data and move array into tuple if necessary."""
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

        Returns a 3D masked NumPy array as 8 bit unsigned integer.
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
            assert len(data) == 3
            try:
                assert data.shape == data.mask.shape
                return data.astype("uint8")
            except:
                return ma.MaskedArray(
                    data=data.astype("uint8"),
                    mask=np.where(data == self.nodata, True, False))
        elif isinstance(data, np.ndarray):
            assert len(data) == 3
            masked = ma.MaskedArray(
                data=data.astype("uint8"),
                mask=np.where(data == self.nodata, True, False))
            return masked

    def for_web(self, data):
        """Return tiles for web usage (as file object)."""
        # Convert to 8 bit.
        r, g, b = self.prepare_data(data)
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
        """Return empty data."""
        return ma.masked_array(
            data=ma.zeros((3, ) + process_tile.shape),
            mask=ma.ones((3, ) + process_tile.shape),
            dtype=PNG_PROFILE["dtype"]
        )


PNG_PROFILE = {
    "dtype": "uint8",
    "driver": "PNG",
    "count": 4,
    "nodata": 255
}
