import logging
import os
import rasterio
from rasterio.warp import calculate_default_transform
from shapely.errors import TopologicalError
from shapely.geometry import box

from mapchete.io._geometry_operations import reproject_geometry, segmentize_geometry
from mapchete.tile import BufferedTilePyramid


logger = logging.getLogger(__name__)


GDAL_HTTP_OPTS = dict(
    GDAL_DISABLE_READDIR_ON_OPEN=True,
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif, .ovr, .jp2, .png, .xml",
    GDAL_HTTP_TIMEOUT=30,
    GDAL_HTTP_MAX_RETRY=3,
    GDAL_HTTP_RETRY_DELAY=5
)


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
            return zoom - 1


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


def tile_to_zoom_level(tile, dst_pyramid=None, matching_method="gdal", precision=8):
    """
    Determine the best zoom level in target TilePyramid from given Tile.

    Parameters
    ----------
    tile : BufferedTile
    dst_pyramid : BufferedTilePyramid
    matching_method : str ('gdal' or 'min')
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
        try:
            l, b, r, t = reproject_geometry(
                box(*bounds), src_crs=tile.crs, dst_crs=dst_pyramid.crs
            ).bounds
        except ValueError:
            raise TopologicalError("bounds cannot be translated into target CRS")
        return r - l, t - b

    if tile.tp.crs == dst_pyramid.crs:
        return tile.zoom
    else:
        if matching_method == "gdal":
            # use rasterio/GDAL method to calculate default warp target properties
            transform, width, height = calculate_default_transform(
                tile.tp.crs,
                dst_pyramid.crs,
                tile.width,
                tile.height,
                *tile.bounds
            )
            # this is the resolution the tile would have in destination TilePyramid CRS
            tile_resolution = round(transform[0], precision)
        elif matching_method == "min":
            # calculate the minimum pixel size from the four tile corner pixels
            l, b, r, t = tile.bounds
            x = tile.pixel_x_size
            y = tile.pixel_y_size
            res = []
            for bounds in [
                (l, t - y, l + x, t),  # left top
                (l, b, l + x, b + y),  # left bottom
                (r - x, b, r, b + y),  # right bottom
                (r - x, t - y, r, t)   # right top
            ]:
                try:
                    w, h = width_height(bounds)
                    res.extend([w, h])
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
            tile_resolution
        )
        zoom = 0
        while True:
            td_resolution = round(dst_pyramid.pixel_x_size(zoom), precision)
            if td_resolution <= tile_resolution:
                break
            zoom += 1
        logger.debug("target zoom for %s: %s (%s)", tile_resolution, zoom, td_resolution)
        return zoom


def get_boto3_bucket(bucket_name):
    """Return boto3.Bucket object from bucket name."""
    import boto3
    url = os.environ.get("AWS_S3_ENDPOINT")
    return boto3.resource(
        's3',
        endpoint_url=(
            "https://" + url
            if url and not url.startswith(("http://", "https://"))
            else url
        )
    ).Bucket(bucket_name)


def get_gdal_options(opts, is_remote=False):
    """
    Return a merged set of custom and default GDAL/rasterio Env options.

    If is_remote is set to True, the default GDAL_HTTP_OPTS are appended.

    Parameters
    ----------
    opts : dict or None
        Explicit GDAL options.
    is_remote : bool
        Indicate whether Env is for a remote file.

    Returns
    -------
    dictionary
    """
    user_opts = {} if opts is None else dict(**opts)
    if is_remote:
        return dict(GDAL_HTTP_OPTS, **user_opts)
    else:
        return user_opts
