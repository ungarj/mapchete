"""Handles output pyramids using GeoTIFFS."""

import os
import numpy as np
import numpy.ma as ma

from mapchete.formats import base
from mapchete.tile import BufferedTile
from mapchete.io.raster import write_raster_window


class OutputData(base.OutputData):
    """Main output class."""

    METADATA = {
        "driver_name": "GTiff",
        "data_type": "raster",
        "mode": "w"
    }

    def __init__(self, output_params):
        """Initialize."""
        super(OutputData, self).__init__(output_params)
        self.path = output_params["path"]
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        self.file_extension = ".tif"
        self.output_params = output_params

    def write(self, process_tile, overwrite=False):
        """Write process output into GeoTIFFs."""
        self.verify_data(process_tile)
        # assert process_tile data complies with output properties like band
        # number, data type.
        process_tile.data = tuple(
            band.astype(self.output_params["dtype"])
            for band in process_tile.data
        )
        # Convert from process_tile to output_tiles
        for tile in self.pyramid.intersecting(process_tile):
            # skip if file exists and overwrite is not set
            out_path = self.get_path(tile)
            if os.path.exists(out_path) and not overwrite:
                return
            self.prepare_path(tile)
            out_tile = BufferedTile(tile, self.pixelbuffer)
            # write_from_tile(buffered_tile, profile, out_tile, out_path)
            write_raster_window(
                in_tile=process_tile, out_profile=self.profile(out_tile),
                out_tile=out_tile, out_path=out_path)

    def tiles_exist(self, process_tile):
        """Check whether all output tiles of a process tile exist."""
        return all(
            os.path.exists(self.get_path(tile))
            for tile in self.pyramid.intersecting(process_tile)
        )

    def is_valid_with_config(self, config):
        """Check if output format is valid with other process parameters."""
        assert isinstance(config, dict)
        assert "bands" in config
        assert isinstance(config["bands"], int)
        assert "path" in config
        assert isinstance(config["path"], str)
        assert "dtype" in config
        assert isinstance(config["dtype"], str)
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
        dst_metadata = GTIFF_PROFILE
        dst_metadata.pop("transform", None)
        dst_metadata.update(
            crs=tile.crs, width=tile.width, height=tile.height,
            affine=tile.affine, driver="GTiff",
            count=self.output_params["bands"],
            dtype=self.output_params["dtype"]
        )
        return dst_metadata

    def verify_data(self, tile):
        """Verify array data and move array into tuple if necessary."""
        if isinstance(tile.data, (np.ndarray, ma.MaskedArray)):
            tile.data = (tile.data, )
        if isinstance(tile.data, tuple):
            for band in tile.data:
                try:
                    assert isinstance(band, (np.ndarray, ma.MaskedArray))
                    assert band.ndim == 2
                except AssertionError:
                    raise ValueError("output bands must be 2D NumPy arrays")
        else:
            raise ValueError(
                "output data must be a 2D NumPy array or a tuple containing \
                2D NumPy arrays.")

    def empty(self, process_tile):
        """Empty data."""
        empty_band = ma.masked_array(
            data=np.full(
                process_tile.shape, self.profile["nodata"],
                dtype=self.profile["dtype"]),
            mask=np.ones(process_tile.shape)
        )
        return (
            empty_band
            for band in range(self.profile["count"])
        )

GTIFF_PROFILE = {
    "blockysize": 256,
    "blockxsize": 256,
    "tiled": True,
    "dtype": "uint8",
    "compress": "lzw",
    "interleave": "band",
    "nodata": 0
}
