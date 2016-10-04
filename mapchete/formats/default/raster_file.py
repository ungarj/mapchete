"""Raster file input which can be read by rasterio."""

import rasterio
from shapely.geometry import box
from shapely.wkt import loads
import ogr

from mapchete.formats.base import InputData
from mapchete.io.vector import reproject_geometry


class InputData(InputData):
    """Main input class."""

    def __init__(self, input_file=None, pyramid=None):
        """Initialize."""
        self.driver_name = "raster_file"
        self.data_type = "raster"
        self.mode = "r"
        self.file_extensions = ["tif", "vrt", "png", "jp2"]
        self.file = input_file
        self.pyramid = pyramid

    def open(self):
        """Return InputTile."""
        raise NotImplementedError

    def bbox(self, out_crs=None):
        """Return data bounding box."""
        assert self.file
        assert self.pyramid
        with rasterio.open(self.file) as inp:
            inp_crs = inp.crs
            try:
                assert inp_crs.is_valid
            except AssertionError:
                raise IOError("CRS could not be read from %s" % self.file)
        out_bbox = bbox = box(
            inp.bounds.left, inp.bounds.bottom, inp.bounds.right,
            inp.bounds.top)
        # If soucre and target CRSes differ, segmentize and reproject
        if inp_crs != out_crs:
            segmentize = _get_segmentize_value(self.file, self.pyramid)
            try:
                ogr_bbox = ogr.CreateGeometryFromWkb(bbox.wkb)
                ogr_bbox.Segmentize(segmentize)
                segmentized_bbox = loads(ogr_bbox.ExportToWkt())
                bbox = segmentized_bbox
            except:
                raise
            try:
                return reproject_geometry(
                    bbox,
                    src_crs=inp_crs,
                    dst_crs=out_crs
                    )
            except:
                raise
        else:
            return out_bbox


class InputTile(InputData):
    """Target Tile representation of input data."""

    def __init__(self):
        """Initialize."""
        raise NotImplementedError
        self.pixelbuffer = None

    def read(self, bands=None):
        """Read reprojected and resampled numpy array for current Tile."""
        raise NotImplementedError


def _get_segmentize_value(input_file, tile_pyramid):
    """Return the recommended segmentation value in input file units."""
    with rasterio.open(input_file, "r") as input_raster:
        pixelsize = input_raster.transform[0]
    return pixelsize * tile_pyramid.tile_size
