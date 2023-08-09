"""This module is deprecated and only here for backwards compatibility"""

from mapchete.path import (  # pragma: no cover
    MPath,
    absolute_path,
    fs_from_path,
    makedirs,
    path_exists,
    path_is_remote,
    relative_path,
    tiles_exist,
)

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
