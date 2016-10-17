"""
Raster file input which can be read by rasterio.

Currently limited by extensions .tif, .vrt., .png and .jp2 but could be
extended easily.
"""

import fiona
from shapely.geometry import box
from rasterio.crs import CRS

from mapchete.formats import base
from mapchete.io.vector import reproject_geometry, read_vector_window


class InputData(base.InputData):
    """Main input class."""

    METADATA = {
        "driver_name": "vector_file",
        "data_type": "vector",
        "mode": "r",
        "file_extensions": ["shp", "geojson"]
    }

    def __init__(self, input_params):
        """Initialize."""
        super(InputData, self).__init__(input_params)
        self.path = input_params["path"]

    def open(self, tile, **kwargs):
        """Return InputTile."""
        return InputTile(tile, self, **kwargs)

    def bbox(self, out_crs=None):
        """Return data bounding box."""
        assert self.path
        assert self.pyramid
        if out_crs is None:
            out_crs = self.pyramid.crs
        with fiona.open(self.path) as inp:
            inp_crs = CRS(inp.crs)
            try:
                assert inp_crs.is_valid
            except AssertionError:
                raise IOError("CRS could not be read from %s" % self.path)
            bbox = bbox = box(*inp.bounds)
        # If soucre and target CRSes differ, segmentize and reproject
        if inp_crs != out_crs:
            return reproject_geometry(bbox, src_crs=inp_crs, dst_crs=out_crs)
        else:
            return bbox


class InputTile(base.InputTile):
    """Target Tile representation of input data."""

    def __init__(self, tile, vector_file):
        """Initialize."""
        self.tile = tile
        self.vector_file = vector_file

    def read(self, validity_check=True):
        """Read reprojected and resampled numpy array for current Tile."""
        return read_vector_window(
            self.vector_file.path, self.tile, validity_check=validity_check)

    def is_empty(self):
        """Check if there is data within this tile."""
        src_bbox = self.vector_file.bbox()
        tile_geom = self.tile.bbox
        if not tile_geom.intersects(src_bbox):
            return True
        try:
            self.read().next()
            return False
        except StopIteration:
            return True
        except:
            raise
