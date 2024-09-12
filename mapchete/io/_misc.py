import logging
from enum import Enum

import rasterio
from rasterio.warp import calculate_default_transform
from shapely.errors import TopologicalError
from shapely.geometry import box

from mapchete.geometry import reproject_geometry, segmentize_geometry
from mapchete.path import MPath
from mapchete.tile import BufferedTile, BufferedTilePyramid

logger = logging.getLogger(__name__)


def get_best_zoom_level(input_file, tile_pyramid_type):
    """
    Determine the best base zoom level for a raster.

    "Best" means the maximum zoom level where no oversampling has to be done.

    Parameters
    ----------
    input_file : path to raster file
    tile_pyramid_type : ``TilePyramid`` projection (``geodetic`` or``mercator``)

    Returns
    -------
    zoom : integer
    """
    tile_pyramid = BufferedTilePyramid(tile_pyramid_type)
    with rasterio.open(input_file, "r") as src:
        xmin, ymin, xmax, ymax = reproject_geometry(
            segmentize_geometry(
                box(
                    src.bounds.left, src.bounds.bottom, src.bounds.right, src.bounds.top
                ),
                get_segmentize_value(input_file, tile_pyramid),
            ),
            src_crs=src.crs,
            dst_crs=tile_pyramid.crs,
        ).bounds
        x_dif = xmax - xmin
        y_dif = ymax - ymin
        size = float(src.width + src.height)
        avg_resolution = (x_dif / float(src.width)) * (float(src.width) / size) + (
            y_dif / float(src.height)
        ) * (float(src.height) / size)

    for zoom in range(0, 40):
        if tile_pyramid.pixel_x_size(zoom) <= avg_resolution:
            return max([0, zoom - 1])


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


class MatchingMethod(str, Enum):
    gdal = "gdal"
    min = "min"


def tile_to_zoom_level(
    tile: BufferedTile,
    dst_pyramid: BufferedTilePyramid,
    matching_method: MatchingMethod = MatchingMethod.gdal,
    precision: int = 8,
):
    """
    Determine the best zoom level in target TilePyramid from given Tile.

    Parameters
    ----------
    tile : BufferedTile
    dst_pyramid : BufferedTilePyramid
    matching_method : MatchingMethod ('gdal' or 'min')
        gdal: Uses GDAL's standard method. Here, the target resolution is calculated by
            averaging the extent's pixel sizes over both x and y axes. This approach
            returns a zoom level which may not have the best quality but will speed up
            reading significantly.
        min: Returns the zoom level which matches the minimum resolution of the extent's
            four corner pixels. This approach returns the zoom level with the best
            possible quality but with low performance. If the tile extent is outside of
            the destination pyramid, a TopologicalError will be raised.
    precision : int
        Round resolutions to n digits before comparing.

    Returns
    -------
    zoom : int
    """

    def width_height(bounds):
        """
        Determine with and height in destination pyramid CRS.

        Raises a TopologicalError if bounds cannot be reprojected.
        """
        try:
            geom = reproject_geometry(
                box(*bounds), src_crs=tile.crs, dst_crs=dst_pyramid.crs
            )
            if geom.is_empty:  # Shapely>=2.0
                raise ValueError("geometry empty after reprojection")
            left, bottom, right, top = geom.bounds
        except ValueError:  # pragma: no cover
            raise TopologicalError("bounds cannot be translated into target CRS")
        return right - left, top - bottom

    if tile.tp.crs == dst_pyramid.crs:
        return tile.zoom
    else:
        if matching_method == MatchingMethod.gdal:
            # use rasterio/GDAL method to calculate default warp target properties
            # enabling CHECK_WITH_INVERT_PROJ fixes #269, otherwise this function would
            # return a non-optimal zoom level for reprojection
            with rasterio.Env(CHECK_WITH_INVERT_PROJ=True):
                transform, width, height = calculate_default_transform(
                    tile.tp.crs, dst_pyramid.crs, tile.width, tile.height, *tile.bounds
                )
                # this is the resolution the tile would have in destination CRS
                tile_resolution = round(transform[0], precision)
        elif matching_method == MatchingMethod.min:
            # calculate the minimum pixel size from the four tile corner pixels
            left, bottom, right, top = tile.bounds
            x_size = tile.pixel_x_size
            y_size = tile.pixel_y_size
            res = []
            for bounds in [
                (left, top - y_size, left + x_size, top),  # left top
                (left, bottom, left + x_size, bottom + y_size),  # left bottom
                (right - x_size, bottom, right, bottom + y_size),  # right bottom
                (right - x_size, top - y_size, right, top),  # right top
            ]:
                try:
                    width, height = width_height(bounds)
                    res.extend([width, height])
                except TopologicalError:
                    logger.debug("pixel outside of destination pyramid")
            if res:
                tile_resolution = round(min(res), precision)
            else:
                raise TopologicalError("tile outside of destination pyramid")
        else:
            raise ValueError("invalid method given: %s", matching_method)
        logger.debug(
            "we are looking for a zoom level interpolating to %s resolution",
            tile_resolution,
        )
        zoom = 0
        while True:
            td_resolution = round(dst_pyramid.pixel_x_size(zoom), precision)
            if td_resolution <= tile_resolution:
                break
            zoom += 1
        logger.debug(
            "target zoom for %s: %s (%s)", tile_resolution, zoom, td_resolution
        )
        return zoom


def get_boto3_bucket(bucket_name):  # pragma: no cover
    """Return boto3.Bucket object from bucket name."""
    raise DeprecationWarning("get_boto3_bucket() is deprecated")


def copy(src_path, dst_path, src_fs=None, dst_fs=None, overwrite=False):
    """Copy path from one place to the other."""
    src_path = MPath.from_inp(src_path, fs=src_fs)
    dst_path = MPath.from_inp(dst_path, fs=dst_fs)

    if not overwrite and dst_path.fs.exists(dst_path):
        raise IOError(f"{dst_path} already exists")

    # create parent directories on local filesystems
    dst_path.parent.makedirs()

    # copy either within a filesystem or between filesystems
    if src_path.fs == dst_path.fs:
        src_path.fs.copy(str(src_path), str(dst_path))
    else:
        # read source data first
        with src_path.open("rb") as src:
            content = src.read()
        # only write to destination if reading source data didn't raise errors,
        # otherwise we can end up with empty objects on an object store
        with dst_path.open("wb") as dst:
            dst.write(content)
