import fsspec
import json
import logging
import os

from mapchete.io._path import fs_from_path, makedirs, path_exists
from mapchete.io._misc import get_boto3_bucket


logger = logging.getLogger(__name__)


def write_json(path, params, fs=None, **kwargs):
    """Write local or remote."""
    logger.debug(f"write {params} to {path}")
    with fsspec.open(path, "w") as dst:
        json.dump(params, dst, sort_keys=True, indent=4)


def read_json(path, fs=None, **kwargs):
    """Read local or remote."""
    fs = fs or fs_from_path(path, **kwargs)
    try:
        with fs.open(path) as src:
            return json.loads(src.read())
    except Exception as e:
        if path_exists(path, fs=fs):  # pragma: no cover
            raise e
        else:
            raise FileNotFoundError(f"{path} not found")
