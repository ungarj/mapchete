"""This module is deprecated and only here for backwards compatibility"""

from mapchete.path import absolute_path  # pragma: no cover
from mapchete.path import (
    MPath,
    fs_from_path,
    makedirs,
    path_exists,
    path_is_remote,
    relative_path,
    tiles_exist,
)  # pragma: no cover

__all__ = [
    "fs_from_path",
    "path_is_remote",
    "path_exists",
    "tiles_exist",
    "absolute_path",
    "relative_path",
    "makedirs",
    "MPath",
]  # pragma: no cover
