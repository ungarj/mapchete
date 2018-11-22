"""Functions for reading and writing data."""

import json
import logging
import os
import rasterio
from shapely.geometry import box
from tilematrix import TilePyramid
from urllib.request import urlopen
from urllib.error import HTTPError
import warnings

from mapchete.errors import MapcheteConfigError
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
    prefixes = ("http://", "https://", "/vsicurl/")
    if s3:
        prefixes += ("s3://", "/vsis3/")
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
        bucket = get_boto3_bucket(path.split("/")[2])
        key = "/".join(path.split("/")[3:])
        for obj in bucket.objects.filter(Prefix=key):
            if obj.key == key:
                return True
        else:
            return False
    else:
        return os.path.exists(path)


def absolute_path(path=None, base_dir=None):
    """
    Return absolute path if path is local.

    Parameters:
    -----------
    path : path to file
    base_dir : base directory used for absolute path

    Returns:
    --------
    absolute path
    """
    if path_is_remote(path):
        return path
    else:
        if os.path.isabs(path):
            return path
        else:
            if base_dir is None or not os.path.isabs(base_dir):
                raise TypeError("base_dir must be an absolute path.")
            return os.path.abspath(os.path.join(base_dir, path))


def relative_path(path=None, base_dir=None):
    """
    Return relative path if path is local.

    Parameters:
    -----------
    path : path to file
    base_dir : directory where path sould be relative to

    Returns:
    --------
    relative path
    """
    if path_is_remote(path) or not os.path.isabs(path):
        return path
    else:
        return os.path.relpath(path, base_dir)


def makedirs(path):
    """
    Silently create all subdirectories of path if path is local.

    Parameters:
    -----------
    path : path
    """
    if not path_is_remote(path):
        try:
            os.makedirs(path)
        except OSError:
            pass


def write_output_metadata(output_params):
    """Dump output JSON and verify parameters if output metadata exist."""
    logger.debug(output_params)
    if "path" in output_params:
        metadata_path = os.path.join(output_params["path"], "metadata.json")
        logger.debug("check for output %s", metadata_path)
        try:
            existing_params = read_json(metadata_path)
            logger.debug("%s exists", metadata_path)
            logger.debug("existing output parameters: %s", existing_params)
            current_params = params_to_dump(output_params)
            grid = existing_params["pyramid"]["grid"]
            if grid["type"] == "geodetic" and grid["shape"] == [2, 1]:
                warnings.warn(
                    """Deprecated grid shape ordering found. """
                    """Please change grid shape from [2, 1] to [1, 2] in %s."""
                    % metadata_path
                )
                existing_params["pyramid"]["grid"]["shape"] = [1, 2]
            if (
                existing_params["pyramid"] != current_params["pyramid"] or
                existing_params["driver"]["format"] != current_params["driver"]["format"]
            ):
                raise MapcheteConfigError(
                    "process output definition differs from existing output: %s != %s" % (
                        existing_params, current_params
                    )
                )
        except FileNotFoundError:
            logger.debug("%s does not exist", metadata_path)
            dump_params = params_to_dump(output_params)
            # dump output metadata
            try:
                write_json(metadata_path, dump_params)
            except Exception as e:
                logger.warning("failed to write %s: %s", metadata_path, e)
    else:
        logger.debug("no path parameter found")


def write_json(path, params):
    """Write local or remote."""
    logger.debug("write %s to %s", params, path)
    if path.startswith("s3://"):
        bucket = get_boto3_bucket(path.split("/")[2])
        key = "/".join(path.split("/")[3:])
        logger.debug("upload %s", key)
        bucket.put_object(
            Key=key,
            Body=json.dumps(params, sort_keys=True, indent=4)
        )
    else:
        makedirs(os.path.dirname(path))
        with open(path, 'w') as dst:
            json.dump(params, dst, sort_keys=True, indent=4)


def read_json(path):
    """Read local or remote."""
    if path.startswith(("http://", "https://")):
        try:
            return json.loads(urlopen(path).read().decode())
        except HTTPError:
            raise FileNotFoundError("%s not found", path)
    elif path.startswith("s3://"):
        bucket = get_boto3_bucket(path.split("/")[2])
        key = "/".join(path.split("/")[3:])
        for obj in bucket.objects.filter(Prefix=key):
            if obj.key == key:
                return json.loads(obj.get()['Body'].read().decode())
        raise FileNotFoundError("%s not found", path)
    else:
        try:
            with open(path, "r") as src:
                return json.loads(src.read())
        except:
            raise FileNotFoundError("%s not found", path)


def params_to_dump(params):
    # in case GridDefinition was not yet initialized
    if isinstance(params["type"], str):
        tp = TilePyramid(params["type"])
        params.update(type=tp.grid)
    return dict(
        pyramid=dict(
            grid=dict(
                type=params["type"].type,
                shape=list(params["type"].shape),
                bounds=list(params["type"].bounds),
                left=params["type"].left,
                bottom=params["type"].bottom,
                right=params["type"].right,
                top=params["type"].top,
                is_global=params["type"].is_global,
                srid=params["type"].srid,
                crs=params["type"].crs.to_string(),
            ),
            metatiling=params.get("metatiling", 1),
            pixelbuffer=params.get("pixelbuffer", 0),
        ),
        driver={
           k: v
           for k, v in params.items()
           if k not in ["path", "type", "pixelbuffer", "metatiling"]
        }
    )


def get_boto3_bucket(bucket_name):
    import boto3
    url = os.environ.get("AWS_S3_ENDPOINT")
    return boto3.resource(
        's3',
        endpoint_url=(
            "https://" + url if url and not url.startswith(("http://", "https://"))
            else url
        )
    ).Bucket(bucket_name)
