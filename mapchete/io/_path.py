"""Functions handling paths and file systems."""

from collections import defaultdict
from functools import cached_property
import logging
import os
from typing import Union

import fsspec

from mapchete._executor import Executor

logger = logging.getLogger(__name__)


class MPath(os.PathLike):
    """Partially replicates pathlib.Path but with remote file support."""

    def __init__(self, path, **kwargs):
        self._kwargs = kwargs
        if isinstance(path, MPath):
            path_str = str(path)
            self._kwargs.update(path._kwargs)
        else:
            path_str = path
        if path_str.startswith("/vsicurl/"):
            self._path_str = path_str.lstrip("/vsicurl/")
            if not self._path_str.startswith(
                ("http://", "https://")
            ):  # pragma: no cover
                raise ValueError(f"wrong usage of GDAL VSI paths: {path_str}")
        else:
            self._path_str = path_str
        self._fs = self._kwargs.get("fs")
        default_fs_options = {"asynchronous": False, "timeout": 5}
        self._fs_options = dict(
            default_fs_options, **self._kwargs.get("fs_options") or {}
        )
        self._fs_session = self._kwargs.get("fs_session")

    def __str__(self):
        return self._path_str

    def __fspath__(self):
        return self._path_str

    @property
    def name(self) -> str:
        return os.path.basename(self._path_str.rstrip("/"))

    @property
    def stem(self) -> str:
        return self.name.split(".")[0]

    @property
    def suffix(self) -> str:
        return os.path.splitext(self._path_str)[1]

    @property
    def dirname(self) -> str:
        return os.path.dirname(self._path_str)

    @property
    def parent(self) -> "MPath":
        return self.new(self.dirname)

    @property
    def elements(self) -> list:
        return self._path_str.split("/")

    @cached_property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Return path filesystem."""
        if self._fs is not None:
            return self._fs
        elif self._path_str.startswith("s3"):
            return fsspec.filesystem(
                "s3",
                requester_pays=os.environ.get("AWS_REQUEST_PAYER") == "requester",
                config_kwargs=dict(
                    connect_timeout=self._fs_options.get("timeout"),
                    read_timeout=self._fs_options.get("timeout"),
                ),
                session=self._fs_session,
                client_kwargs={
                    k: v
                    for k, v in self._kwargs.items()
                    if k not in ["fs", "fs_options", "fs_session"]
                },
            )
        elif self._path_str.startswith(("http://", "https://")):
            if self._kwargs.get("username") or self._kwargs.get(
                "password"
            ):  # pragma: no cover
                from aiohttp import BasicAuth

                auth = BasicAuth(
                    self._kwargs.get("username"), self._kwargs.get("password")
                )
            else:
                auth = None
            return fsspec.filesystem("https", auth=auth, **self._fs_options)
        else:
            return fsspec.filesystem("file", **self._fs_options)

    @cached_property
    def protocols(self) -> set:
        """Return set of filesystem protocols."""
        if isinstance(self.fs.protocol, str):
            return set([self.fs.protocol])
        else:
            return set(self.fs.protocol)

    def startswith(self, string):
        return self._path_str.startswith(string)

    def endswith(self, string):
        return self._path_str.endswith(string)

    def split(self, by):
        return self._path_str.split(by)

    def new(self, path) -> "MPath":
        """Create a new MPath instance with given path."""
        return MPath(path, **self._kwargs)

    def exists(self) -> bool:
        """Check if path exists."""
        return self.fs.exists(self._path_str)

    def is_remote(self) -> bool:
        """Check whether path is remote or not."""
        remote_protocols = {"http", "https", "s3", "s3a"}
        return bool(self.protocols.intersection(remote_protocols))

    def is_absolute(self) -> bool:
        """Return whether path is absolute."""
        return os.path.isabs(self._path_str)

    def absolute_path(self, base_dir=None) -> "MPath":
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
        if self.is_remote() or self.is_absolute():
            return self
        else:
            if base_dir is None or not os.path.isabs(base_dir):
                raise TypeError("base_dir must be an absolute path.")
            return self.new(os.path.abspath(os.path.join(base_dir, self._path_str)))

    def relative_path(self, start=None, base_dir=None) -> "MPath":
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
        start = start or base_dir
        if self.is_remote() or self.is_absolute():
            return self
        else:
            return self.new(os.path.relpath(self._path_str, start=start))

    def open(self, mode="r"):
        """Open file."""
        return self.fs.open(self._path_str, mode)

    def read_text(self) -> str:
        """Open and return file content as text."""
        with self.open() as src:
            return src.read()

    def makedirs(self) -> None:
        """Create all parent directories for path."""
        # create parent directories on local filesystems
        if self.fs.protocol == "file":
            self.fs.makedirs(self.dirname, exist_ok=True)

    def ls(self, detail=False):
        return self.fs.ls(self._path_str, detail=detail)

    def joinpath(self, *other) -> "MPath":
        """Join path with other."""
        return self.new(os.path.join(self._path_str, *other))

    def as_gdal_str(self) -> str:
        """Return path as GDAL VSI string."""
        if self._path_str.startswith(("http://", "https://")):
            return "/vsicurl/" + self._path_str
        elif self._path_str.startswith("s3://"):
            return self._path_str.replace("s3://", "/vsis3/")
        else:
            return self._path_str

    def __truediv__(self, other) -> "MPath":
        """Short for self.joinpath()."""
        return self.joinpath(other)

    def __eq__(self, other):
        return hash(self) == hash(MPath(other))

    def __repr__(self):
        return f"<mapchete.io.MPath object: {self._path_str}>"

    def __hash__(self):
        return hash(repr(self))


def path_is_remote(path, **kwargs):
    """
    Determine whether file path is remote or local.

    Parameters
    ----------
    path : path to file

    Returns
    -------
    is_remote : bool
    """
    path = path if isinstance(path, MPath) else MPath(path)
    return path.is_remote()


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
    path = path if isinstance(path, MPath) else MPath(path)
    return path.exists()


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
    path = path if isinstance(path, MPath) else MPath(path)
    return path.absolute_path(base_dir=base_dir)


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
    path = path if isinstance(path, MPath) else MPath(path)
    return path.relative_path(base_dir=base_dir)


def makedirs(path, fs=None):
    """
    Silently create all subdirectories of path if path is local.

    Parameters
    ----------
    path : path
    """
    path = path if isinstance(path, MPath) else MPath(path, fs=fs)
    path.makedirs()


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
    if basepath.suffix == config.output_reader.file_extension:

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
    return "/".join(MPath(path).elements[elements:])


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
        rowpath = config.output_reader.path.joinpath(str(zoom), str(row))
        logger.debug("rowpath: %s", rowpath)
        try:
            for path in rowpath.ls(detail=False):
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
            rowpath = config.output_reader.path.joinpath(str(zoom), str(row))
            logger.debug("rowpath: %s", rowpath)
            try:
                for path in rowpath.ls(detail=False):
                    path = _crop_path(path)
                    if path in output_paths:
                        existing_tiles.add(output_paths[path])
            # this happens when the row directory does not even exist
            except FileNotFoundError:
                pass
        return [(tile, tile in existing_tiles) for tile in tiles]
    else:  # pragma: no cover
        return []


def fs_from_path(path, **kwargs):
    """Guess fsspec FileSystem from path and initialize using the desired options."""
    path = path if isinstance(path, MPath) else MPath(path, **kwargs)
    return path.fs


def copy(src_path, dst_path, src_fs=None, dst_fs=None, overwrite=False):
    """Copy path from one place to the other."""
    src_path = MPath(src_path, fs=src_fs)
    dst_path = MPath(dst_path, fs=dst_fs)

    if not overwrite and dst_path.fs.exists(dst_path):
        raise IOError(f"{dst_path} already exists")

    # create parent directories on local filesystems
    dst_path.makedirs()

    # copy either within a filesystem or between filesystems
    if src_path.fs == dst_path.fs:
        src_path.fs.copy(str(src_path), str(dst_path))
    else:
        with src_path.open("rb") as src:
            with dst_path.open("wb") as dst:
                dst.write(src.read())
