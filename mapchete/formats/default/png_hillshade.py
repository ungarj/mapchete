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
        try:
            self.old_band_num = output_params["old_band_num"]
            PNG_PROFILE.update(count=4)
        except KeyError:
            self.old_band_num = False

    def write(self, process_tile):
        """Write process output into GeoTIFFs."""
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        self.verify_data(process_tile)
        # assert process_tile data complies with output properties like band
        # number, data type.
        data = process_tile.data
        if isinstance(data, ma.MaskedArray):
            data = np.where(data.mask, 0, data).astype("uint8")
        if self.old_band_num:
            process_tile.data = np.stack((
                np.zeros(process_tile.shape), np.zeros(process_tile.shape),
                np.zeros(process_tile.shape), data))
        else:
            process_tile.data = np.stack((np.zeros(process_tile.shape), data))
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
        """Read process output tile into numpy array."""
        try:
            if self.old_band_num:
                band_num = 4
            else:
                band_num = 2
            with rasterio.open(self.get_path(output_tile)) as src:
                output_tile.data = src.read(band_num, masked=True)
                return output_tile
        except:
            raise

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
            width=tile.width,
            height=tile.height,
            affine=tile.affine, driver="PNG",
            crs=tile.crs
        )
        return dst_metadata

    def verify_data(self, tile):
        """Verify array data and move array into tuple if necessary."""
        try:
            assert isinstance(tile.data, (np.ndarray, ma.MaskedArray, tuple))
        except AssertionError:
            raise ValueError(
                "process output must be 2D NumPy array, masked array or a tuple"
                )
        if isinstance(tile.data, (tuple, list)):
            try:
                assert len(tile.data) == 1
            except AssertionError:
                raise ValueError(
                    "only one band allowed for process output, not %s" %
                    len(tile.data))
            tile.data = tile.data[0]
        try:
            assert tile.data.ndim == 2
        except AssertionError:
            raise ValueError("output band must be a 2D NumPy array")

    def for_web(self, data):
        """Return tiles for web usage (as file object)."""
        if isinstance(data, ma.MaskedArray):
            data = np.where(data.mask, 0, data).astype("uint8")
        zeros = np.zeros(data.shape)
        out_rgb = (zeros, zeros, zeros, data)
        reshaped = np.stack(out_rgb).transpose(1, 2, 0).astype("uint8")
        empty_image = Image.fromarray(reshaped, mode='RGBA')
        out_img = io.BytesIO()
        empty_image.save(out_img, 'PNG')
        out_img.seek(0)
        return send_file(out_img, mimetype='image/png')

    def empty(self, process_tile):
        """Return empty data."""
        return ma.zeros(process_tile.shape)


PNG_PROFILE = {
    "dtype": "uint8",
    "driver": "PNG",
    "count": 2
}
