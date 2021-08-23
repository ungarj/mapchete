import concurrent.futures
import fsspec
from itertools import chain
import logging
import os

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


def tiles_exist(config=None, output_tiles=None, process_tiles=None, multi=None):
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

    try:
        tiles_iter = iter(process_tiles) if output_tiles is None else iter(output_tiles)
        first_tile = next(tiles_iter)
        all_tiles_iter = chain([first_tile], tiles_iter)
    except StopIteration:
        return

    # only on TileDirectories on S3
    if not basepath.endswith(
        config.output_reader.file_extension
    ) and basepath.startswith("s3://"):
        yield from _s3_tiledirectories(
            basepath=basepath,
            first_tile=first_tile,
            all_tiles_iter=all_tiles_iter,
            process_tiles=process_tiles,
            output_tiles=output_tiles,
            config=config,
        )
    else:

        def _exists(tile):
            return (
                tile,
                config.output_reader.tiles_exist(
                    **{"process_tile" if process_tiles else "output_tile": tile}
                ),
            )

        if multi == 1:
            for tile in all_tiles_iter:
                yield _exists(tile=tile)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=multi) as executor:
                for future in concurrent.futures.as_completed(
                    (executor.submit(_exists, tile) for tile in all_tiles_iter)
                ):
                    yield future.result()


def _s3_tiledirectories(
    basepath=None,
    first_tile=None,
    all_tiles_iter=None,
    process_tiles=None,
    output_tiles=None,
    config=None,
):
    import boto3

    basekey = "/".join(basepath.split("/")[3:])
    bucket = basepath.split("/")[2]
    s3 = boto3.client("s3", **config.output_reader._fs_kwargs)
    paginator = s3.get_paginator("list_objects_v2")

    # determine zoom
    zoom = first_tile.zoom

    # get all output tiles
    all_tiles = set(all_tiles_iter)
    if process_tiles:
        output_tiles = (
            t
            for process_tile in all_tiles
            for t in config.output_pyramid.intersecting(process_tile)
        )
    else:
        output_tiles = all_tiles

    # create a mapping between paths and tiles
    paths = dict()
    # remember rows
    output_rows = set()
    for output_tile in output_tiles:
        if output_tile.zoom != zoom:  # pragma: no cover
            raise ValueError("tiles of different zoom levels cannot be mixed")
        path = config.output_reader.get_path(output_tile)

        if process_tiles:
            paths[path] = config.process_pyramid.intersecting(output_tile)[0]
        else:
            paths[path] = output_tile

        output_rows.add(output_tile.row)

    # remember already yielded tiles
    yielded = set()
    for row in output_rows:
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
                    # in case of an existing tile which was not passed on to this
                    # function
                    continue
                # store and yield process tile if it was not already yielded
                if tile not in yielded:
                    yielded.add(tile)
                    yield (tile, True)

    # finally, yield all tiles which were not yet yielded as False
    for tile in all_tiles.difference(yielded):
        yield (tile, False)


def fs_from_path(path, timeout=5, session=None, username=None, password=None, **kwargs):
    """Guess fsspec FileSystem from path and initialize using the desired options."""
    if path.startswith("s3://"):
        return fsspec.filesystem(
            "s3",
            requester_pays=os.environ.get("AWS_REQUEST_PAYER") == "requester",
            config_kwargs=dict(connect_timeout=timeout, read_timeout=timeout),
            session=session,
            client_kwargs=kwargs,
        )
    elif path.startswith(("http://", "https://")):
        if username:  # pragma: no cover
            from aiohttp import BasicAuth

            auth = BasicAuth(username, password)
        else:
            auth = None
        return fsspec.filesystem("https", auth=auth, asynchronous=False, **kwargs)
    else:
        return fsspec.filesystem("file", asynchronous=False, **kwargs)
