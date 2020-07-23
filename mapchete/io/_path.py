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
            else:  # pragma: no cover
                raise
    elif path.startswith("s3://"):
        bucket = get_boto3_bucket(path.split("/")[2])
        key = "/".join(path.split("/")[3:])
        for obj in bucket.objects.filter(Prefix=key, RequestPayer='requester'):
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


def tiles_exist(config=None, output_tiles=None, process_tiles=None):
    """
    Yield tiles and whether their output already exists or not.

    Either "output_tiles" or "process_tiles" have to be provided.

    The S3 part of the function are loosely inspired by
    https://alexwlchan.net/2019/07/listing-s3-keys/

    Parameters
    ----------
    config : mapchete.config.MapcheteConfig
    process_tiles : iterator

    Yields
    ------
    tuple : (tile, exists)
    """
    if process_tiles is not None and output_tiles is not None:  # pragma: no cover
        raise ValueError("just one of 'process_tiles' and 'output_tiles' allowed")
    elif process_tiles is None and output_tiles is None:  # pragma: no cover
        raise ValueError("one of 'process_tiles' and 'output_tiles' has to be provided")

    basepath = config.output_reader.path
    # make tiles unique
    tiles = set(process_tiles) if process_tiles is not None else set(output_tiles)

    # in case no tiles are provided
    if not tiles:
        return

    # only on TileDirectories on S3
    if (
        not config.output_reader.path.endswith(config.output_reader.file_extension) and
        basepath.startswith("s3://")
    ):
        import boto3
        basekey = "/".join(basepath.split("/")[3:])
        bucket = basepath.split("/")[2]
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")

        # determine zoom
        zoom = next(iter(tiles)).zoom

        # get all output tiles
        if process_tiles:
            output_tiles = set(
                [
                    t
                    for process_tile in tiles
                    for t in config.output_pyramid.intersecting(process_tile)
                ]
            )
        else:
            output_tiles = tiles

        # create a mapping between paths and tiles
        paths = dict()
        # remember rows
        rows = set()
        for output_tile in output_tiles:
            if output_tile.zoom != zoom:  # pragma: no cover
                raise ValueError("tiles of different zoom levels cannot be mixed")
            path = config.output_reader.get_path(output_tile)

            if process_tiles:
                paths[path] = config.process_pyramid.intersecting(output_tile)[0]
            else:
                paths[path] = output_tile

            rows.add(output_tile.row)

        # remember already yielded tiles
        yielded = set()
        for row in rows:
            # use prefix until row, page through api results
            logger.debug("check existing tiles in row %s" % row)
            prefix = os.path.join(*[basekey, str(zoom), str(row)])
            logger.debug("read keys %s*" % os.path.join("s3://" + bucket, prefix))

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                logger.debug("read next page")
                try:
                    contents = page["Contents"]
                except KeyError:
                    break

                for obj in contents:
                    # get matching tile
                    try:
                        tile = paths[os.path.join("s3://" + bucket, obj["Key"])]
                    except KeyError:  # pragma: no cover
                        continue
                    # store and yield process tile if it was not already yielded
                    if tile not in yielded:
                        yielded.add(tile)
                        yield (tile, True)

        # finally, yield all tiles which were not yet yielded as False
        for tile in tiles.difference(yielded):
            yield (tile, False)

    else:
        def _exists(tile):
            if process_tiles:
                return (tile, config.output_reader.tiles_exist(process_tile=tile))
            else:
                return (tile, config.output_reader.tiles_exist(output_tile=tile))

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for future in concurrent.futures.as_completed(
                (executor.submit(_exists, tile) for tile in tiles)
            ):
                yield future.result()
