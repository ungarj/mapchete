import concurrent.futures
import fsspec
from itertools import chain
import logging
import os
from urllib.error import HTTPError
from urllib.request import urlopen

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


def path_exists(path, fs=None, **kwargs):
    """
    Check if file exists either remote or local.

    Parameters
    ----------
    path : path to file

    Returns
    -------
    exists : bool
    """
    fs = fs or fs_from_path(path, **kwargs)
    fs.invalidate_cache(path=path)
    return fs.exists(path)


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


def tiles_exist(
    config=None,
    output_tiles=None,
    process_tiles=None,
    basepath=None,
    file_extension=None,
    output_pyramid=None,
    fs=None,
    multi=None
):
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
    elif config is None and output_pyramid is None and output_tiles is None: # pragma: no cover
        raise ValueError(
            "output_pyramid is required when no MapcheteConfig and process_tiles given"
        )
    elif config is None and file_extension is None:  # pragma: no cover
        raise ValueError("file_extension is required when no MapcheteConfig is given")

    # get first tile and in case no tiles are provided return
    try:
        tiles_iter = iter(process_tiles) if output_tiles is None else iter(output_tiles)
        first_tile = next(tiles_iter)
        all_tiles_iter = chain([first_tile], tiles_iter)
    except StopIteration:
        return

    if config:
        basepath = config.output_reader.path
        file_extension = config.output_reader.file_extension
        process_pyramid = config.process_pyramid
        output_pyramid = config.output_pyramid
    else:
        basepath = basepath
        file_extension = file_extension
        if output_pyramid is None and output_tiles:
            output_pyramid = first_tile.tp
        process_pyramid =  first_tile.tp if process_tiles else None
    fs = fs or fs_from_path(basepath)

    # only on TileDirectories on S3
    # This implementation queries multiple keys at once by using paging and therefore
    # requires less S3 requests and is in most cases faster.
    if (
        not basepath.endswith(file_extension) and
        basepath.startswith("s3://")
    ):
        import boto3
        tiles = set(all_tiles_iter)
        basekey = "/".join(basepath.split("/")[3:])
        bucket = basepath.split("/")[2]
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")

        # determine zoom
        zoom = first_tile.zoom

        # get all output tiles
        if process_tiles:
            output_tiles = set(
                [
                    t
                    for process_tile in tiles
                    for t in output_pyramid.intersecting(process_tile)
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

            path = (
                config.output_reader.get_path(output_tile)
                if config
                else _tile_path(basepath, output_tile, file_extension)
            )

            if process_tiles:
                paths[path] = process_pyramid.intersecting(output_tile)[0]
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

    # This implementation is for all other storage backends.
    else:
        def _exists(
            tile,
            config=None,
            basepath=None,
            file_extension=None,
            fs=None,
            output_pyramid=None
        ):
            if process_tiles:
                if config:
                    print(config.output_reader.get_path(tile))
                    return (tile, config.output_reader.tiles_exist(process_tile=tile))
                else:
                    for output_tile in output_pyramid.intersecting(tile):
                        print(_tile_path(basepath, tile, file_extension))
                        if fs.exists(_tile_path(basepath, tile, file_extension)):
                            return tile, True
                    else:
                        return tile, False
            else:
                if config:
                    return (tile, config.output_reader.tiles_exist(output_tile=tile))
                else:
                    return (tile, fs.exists(_tile_path(basepath, tile, file_extension)))

        if multi == 1:
            for tile in all_tiles_iter:
                yield _exists(
                    tile=tile,
                    config=config,
                    basepath=basepath,
                    file_extension=file_extension,
                    fs=fs,
                    output_pyramid=output_pyramid
                )
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=multi) as executor:
                for future in concurrent.futures.as_completed(
                    (
                        executor.submit(
                            _exists,
                            tile=tile,
                            config=config,
                            basepath=basepath,
                            file_extension=file_extension,
                            fs=fs,
                            output_pyramid=output_pyramid
                        )
                        for tile in all_tiles_iter
                    )
                ):
                    yield future.result()


def _tile_path(basepath, tile, file_extension):
    return os.path.join(
        basepath, f"{tile.zoom}/{tile.row}/{tile.col}{file_extension}"
    )


def fs_from_path(path, timeout=5, session=None, username=None, password=None, **kwargs):
    """Guess fsspec FileSystem from path and initialize using the desired options."""
    if path.startswith("s3://"):
        logger.debug("use s3 filesystem")
        return fsspec.filesystem(
            "s3",
            requester_pays=os.environ.get("AWS_REQUEST_PAYER") == "requester",
            config_kwargs=dict(connect_timeout=timeout, read_timeout=timeout),
            session=session,
        )
    elif path.startswith(("http://", "https://")):
        if username:  # pragma: no cover
            from aiohttp import BasicAuth
            auth = BasicAuth(username, password)
        else:
            auth = None
        logger.debug("use http filesystem")
        return fsspec.filesystem(
            "https",
            auth=auth,
            asynchronous=False, 
        )
    else:
        logger.debug("use local filesystem")
        return fsspec.filesystem("file")
