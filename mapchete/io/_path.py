from collections import defaultdict
import concurrent.futures
import logging
import os
from urllib.request import urlopen
from urllib.error import HTTPError

from mapchete.io._misc import get_boto3_bucket

logger = logging.getLogger(__name__)


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

    Parameters
    ----------
    path : path to file

    Returns
    -------
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

    Parameters
    ----------
    path : path to file
    base_dir : base directory used for absolute path

    Returns
    -------
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

    Parameters
    ----------
    path : path to file
    base_dir : directory where path sould be relative to

    Returns
    -------
    relative path
    """
    if path_is_remote(path) or not os.path.isabs(path):
        return path
    else:
        return os.path.relpath(path, base_dir)


def makedirs(path):
    """
    Silently create all subdirectories of path if path is local.

    Parameters
    ----------
    path : path
    """
    if not path_is_remote(path):
        try:
            os.makedirs(path)
        except OSError:
            pass


def process_tiles_exist(config=None, process_tiles=None):
    """
    Yield process tiles and whether their output already exists or not.

    The S3 part of the function are loosely inspired by
    https://alexwlchan.net/2019/07/listing-s3-keys/

    Parameters
    ----------
    config : mapchete.config.MapcheteConfig
    process_tiles : iterator

    Yields
    ------
    tuple : (process_tile, exists)
    """
    basepath = config.output_reader.path

    # only on TileDirectories on S3
    if (
        not config.output_reader.path.endswith(config.output_reader.file_extension) and
        basepath.startswith("s3://")
    ):
        basekey = "/".join(basepath.split("/")[3:])
        bucket = basepath.split("/")[2]
        import boto3
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")

        # make process tiles unique
        process_tiles = set(process_tiles)
        # determine current zoom
        zoom = next(iter(process_tiles)).zoom
        # get all output tiles for process tiles
        output_tiles = set(
            [
                t
                for process_tile in process_tiles
                for t in config.output_pyramid.intersecting(process_tile)
            ]
        )
        # create a mapping between paths and process_tiles
        paths = dict()
        # group process_tiles by row
        rowgroups = defaultdict(list)
        # remember already yielded process_tiles
        yielded = set()
        for output_tile in output_tiles:
            if output_tile.zoom != zoom:
                raise ValueError("tiles of different zoom levels cannot be mixed")
            path = config.output_reader.get_path(output_tile)
            process_tile = config.process_pyramid.intersecting(output_tile)[0]
            paths[path] = process_tile
            rowgroups[process_tile.row].append(process_tile)
        # use prefix until row, page through api results
        for row, tiles in rowgroups.items():
            logger.debug("check existing tiles in row %s" % row)
            prefix = os.path.join(*[basekey, str(zoom), str(row)])
            logger.debug(
                "read keys %s*" % os.path.join("s3://" + bucket, prefix)
            )

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                logger.debug("read next page")
                try:
                    contents = page["Contents"]
                except KeyError:
                    break
                for obj in contents:
                    path = obj["Key"]
                    # get matching process_tile
                    process_tile = paths[os.path.join("s3://" + bucket, path)]
                    # store and yield process tile if it was not already yielded
                    if process_tile not in yielded:
                        yielded.add(process_tile)
                        yield (process_tile, True)

        # finally, yield all process tiles which were not yet yielded as False
        for process_tile in process_tiles.difference(yielded):
            yield (process_tile, False)

    else:
        def _exists(tile):
            return (tile, config.output_reader.tiles_exist(tile))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for future in concurrent.futures.as_completed(
                (executor.submit(_exists, tile) for tile in process_tiles)
            ):
                yield future.result()
