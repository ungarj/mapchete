"""Functions for reading and writing data."""

from mapchete.io._json import (
    write_json,
    read_json,
)
from mapchete.io._misc import (
    get_best_zoom_level,
    get_segmentize_value,
    tile_to_zoom_level,
    get_boto3_bucket,
    copy,
)
from mapchete.io.settings import GDAL_HTTP_OPTS, get_gdal_options
from mapchete.path import (
    fs_from_path,
    path_is_remote,
    path_exists,
    tiles_exist,
    absolute_path,
    relative_path,
    makedirs,
    MPath,
)


__all__ = [
    "copy",
    "fs_from_path",
    "GDAL_HTTP_OPTS",
    "get_best_zoom_level",
    "get_segmentize_value",
    "tile_to_zoom_level",
    "path_is_remote",
    "path_exists",
    "tiles_exist",
    "absolute_path",
    "relative_path",
    "makedirs",
    "Path",
    "write_json",
    "read_json",
    "get_boto3_bucket",
    "get_gdal_options",
    "MPath",
]
