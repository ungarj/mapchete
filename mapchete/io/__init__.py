"""Functions for reading and writing data."""

import boto3
import logging
import os
import rasterio
from shapely.geometry import box
from tilematrix import TilePyramid
from urllib.request import urlopen
from urllib.error import HTTPError

from mapchete.io.vector import reproject_geometry, segmentize_geometry


logger = logging.getLogger(__name__)


GDAL_HTTP_OPTS = dict(
    GDAL_DISABLE_READDIR_ON_OPEN=True,
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif, .ovr, .jp2, .png",
    GDAL_HTTP_TIMEOUT=30
)


def get_best_zoom_level(input_file, tile_pyramid_type):
    """
    Determine the best base zoom level for a raster.

    "Best" means the maximum zoom level where no oversampling has to be done.

    Parameters
    ----------
    input_file : path to raster file
    tile_pyramid_type : ``TilePyramid`` projection (``geodetic`` or
        ``mercator``)

    Returns
    -------
    zoom : integer
    """
    tile_pyramid = TilePyramid(tile_pyramid_type)
    with rasterio.open(input_file, "r") as src:
        xmin, ymin, xmax, ymax = reproject_geometry(
            segmentize_geometry(
                box(
                    src.bounds.left, src.bounds.bottom, src.bounds.right,
                    src.bounds.top
                ),
                get_segmentize_value(input_file, tile_pyramid)
            ),
            src_crs=src.crs, dst_crs=tile_pyramid.crs
        ).bounds
        x_dif = xmax - xmin
        y_dif = ymax - ymin
        size = float(src.width + src.height)
        avg_resolution = (
            (x_dif / float(src.width)) * (float(src.width) / size) +
            (y_dif / float(src.height)) * (float(src.height) / size)
        )

    for zoom in range(0, 40):
        if tile_pyramid.pixel_x_size(zoom) <= avg_resolution:
            return zoom-1


def get_segmentize_value(input_file=None, tile_pyramid=None):
    """
    Return the recommended segmentation value in input file units.

    It is calculated by multiplyling raster pixel size with tile shape in
    pixels.

    Parameters
    ----------
    input_file : str
        location of a file readable by rasterio
    tile_pyramied : ``TilePyramid`` or ``BufferedTilePyramid``
        tile pyramid to estimate target tile size

    Returns
    -------
    segmenize value : float
        length suggested of line segmentation to reproject file bounds
    """
    with rasterio.open(input_file, "r") as input_raster:
        pixelsize = input_raster.transform[0]
    return pixelsize * tile_pyramid.tile_size


def path_is_remote(path, s3=True):
    """
    Determine whether file path is remote or local.

    Parameters
    ----------
    path : path to file

    Returns
    -------
    is_remote : bool
    """
    prefixes = ("http://", "https://")
    if s3:
        prefixes += ("s3://", )
    return path.startswith(prefixes)


def path_exists(path):
    """
    Check if file exists either remote or local.

    Parameters:
    -----------
    path : path to file

    Returns:
    --------
    exists : bool
    """
    if path.startswith(("http://", "https://")):
        try:
            urlopen(path).info()
            return True
        except HTTPError as e:
            if e.code == 404:
                return False
            else:
                raise
    elif path.startswith("s3://"):
        bucket_name = path.split("/")[2]
        bucket = boto3.resource('s3').Bucket(bucket_name)
        key = "/".join(path.split("/")[3:])
        for obj in bucket.objects.filter(Prefix=key):
            if obj.key == key:
                return True
        else:
            return False
    else:
        return os.path.exists(path)


def absolute_path(directory, path):
    """Return absolute path if local."""
    if path_is_remote(path):
        return path
    else:
        return os.path.abspath(os.path.join(directory, path))


def makedirs(path):
    if not path_is_remote(path):
        try:
            os.makedirs(path)
        except OSError:
            pass
