"""Functions handling paths and file systems."""

from collections import defaultdict
import logging
import os

import fsspec

from mapchete._executor import Executor

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
    logger.debug("check if path exists: %s", path)
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


def makedirs(path, fs=None):
    """
    Silently create all subdirectories of path if path is local.

    Parameters
    ----------
    path : path
    """
    fs = fs or fs_from_path(path)
    # create parent directories on local filesystems
    if fs.protocol == "file":
        fs.makedirs(path, exist_ok=True)


def tiles_exist(
    config=None,
    output_tiles=None,
    output_tiles_batches=None,
    process_tiles=None,
    process_tiles_batches=None,
    **kwargs,
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
    if (
        sum(
            [
                0 if t is None else 1
                for t in [
                    output_tiles,
                    output_tiles_batches,
                    process_tiles,
                    process_tiles_batches,
                ]
            ]
        )
        != 1
    ):  # pragma: no cover
        raise ValueError(
            "exactly one of 'output_tiles', 'output_tiles_batches', 'process_tiles' or 'process_tiles_batches' is allowed"
        )

    basepath = config.output_reader.path

    # for single file outputs:
    if basepath.endswith(config.output_reader.file_extension):

        def _exists(tile):
            return (
                tile,
                config.output_reader.tiles_exist(
                    **{"process_tile" if process_tiles else "output_tile": tile}
                ),
            )

        def _tiles():
            if process_tiles or output_tiles:  # pragma: no cover
                yield from process_tiles or output_tiles
            else:
                for batch in process_tiles_batches or output_tiles_batches:
                    yield from batch

        for tile in _tiles():
            yield _exists(tile=tile)

    # for tile directory outputs:
    elif process_tiles:
        logger.debug("sort process tiles by row, this could take a while")
        process_tiles_batches = _batch_tiles_by_row(process_tiles)
    elif output_tiles:
        logger.debug("sort output tiles by row, this could take a while")
        output_tiles_batches = _batch_tiles_by_row(output_tiles)

    if process_tiles_batches:
        yield from _process_tiles_batches_exist(process_tiles_batches, config)
    elif output_tiles_batches:
        yield from _output_tiles_batches_exist(output_tiles_batches, config)


def _batch_tiles_by_row(tiles):
    ordered = defaultdict(set)
    for tile in tiles:
        ordered[tile.row].add(tile)
    return ((t for t in ordered[row]) for row in sorted(list(ordered.keys())))


def _crop_path(path, elements=-3):
    return "/".join(path.split("/")[elements:])


def _output_tiles_batches_exist(output_tiles_batches, config):
    with Executor(concurrency="threads") as executor:
        for batch in executor.as_completed(
            _output_tiles_batch_exists,
            (list(b) for b in output_tiles_batches),
            fargs=(config,),
        ):
            yield from batch.result()


def _output_tiles_batch_exists(tiles, config):
    if tiles:
        zoom = tiles[0].zoom
        # determine output paths
        output_paths = {
            _crop_path(config.output_reader.get_path(output_tile)): output_tile
            for output_tile in tiles
        }
        # iterate through output tile rows and determine existing output tiles
        existing_tiles = set()
        row = tiles[0].row
        logger.debug("check existing tiles in row %s", row)
        rowpath = os.path.join(config.output_reader.path, str(zoom), str(row))
        logger.debug("rowpath: %s", rowpath)
        try:
            for path in config.output_reader.fs.ls(rowpath, detail=False):
                path = _crop_path(path)
                if path in output_paths:
                    existing_tiles.add(output_paths[path])
        # this happens when the row directory does not even exist
        except FileNotFoundError:
            pass
        return [(tile, tile in existing_tiles) for tile in tiles]
    else:  # pragma: no cover
        return []


def _process_tiles_batches_exist(process_tiles_batches, config):
    with Executor(concurrency="threads") as executor:
        for batch in executor.as_completed(
            _process_tiles_batch_exists,
            (list(b) for b in process_tiles_batches),
            fargs=(config,),
        ):
            yield from batch.result()


def _process_tiles_batch_exists(tiles, config):
    if tiles:
        zoom = tiles[0].zoom
        # determine output tile rows
        output_rows = sorted(
            list(set(t.row for t in config.output_pyramid.intersecting(tiles[0])))
        )
        # determine output paths
        output_paths = {
            _crop_path(config.output_reader.get_path(output_tile)): process_tile
            for process_tile in tiles
            for output_tile in config.output_pyramid.intersecting(process_tile)
        }
        # iterate through output tile rows and determine existing process tiles
        existing_tiles = set()
        for row in output_rows:
            logger.debug("check existing tiles in row %s", row)
            rowpath = os.path.join(config.output_reader.path, str(zoom), str(row))
            logger.debug("rowpath: %s", rowpath)
            try:
                for path in config.output_reader.fs.ls(rowpath, detail=False):
                    path = _crop_path(path)
                    if path in output_paths:
                        existing_tiles.add(output_paths[path])
            # this happens when the row directory does not even exist
            except FileNotFoundError:
                pass
        return [(tile, tile in existing_tiles) for tile in tiles]
    else:  # pragma: no cover
        return []


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


def copy(src_path, dst_path, src_fs=None, dst_fs=None, overwrite=False):
    """Copy path from one place to the other."""
    src_fs = src_fs or fs_from_path(src_path)
    dst_fs = dst_fs or fs_from_path(dst_path)

    if not overwrite and dst_fs.exists(dst_path):
        raise IOError(f"{dst_path} already exists")

    # create parent directories on local filesystems
    makedirs(os.path.dirname(dst_path))

    # copy either within a filesystem or between filesystems
    if src_fs == dst_fs:
        src_fs.copy(src_path, dst_path)
    else:
        with src_fs.open(src_path, "rb") as src:
            with dst_fs.open(dst_path, "wb") as dst:
                dst.write(src.read())
