"""Functions handling paths and file systems."""
from __future__ import annotations

import logging
import os
from collections import defaultdict
from functools import cached_property
from typing import IO, List, Set, TextIO, Union

import fiona
import fsspec
import rasterio
from fiona.session import Session as FioSession
from fsspec import AbstractFileSystem
from rasterio.session import Session as RioSession

from mapchete._executor import Executor
from mapchete.io.settings import GDALHTTPOptions
from mapchete.tile import BufferedTile

logger = logging.getLogger(__name__)

DEFAULT_STORAGE_OPTIONS = {"asynchronous": False, "timeout": None}
UNALLOWED_S3_KWARGS = ["timeout"]
UNALLOWED_HTTP_KWARGS = ["username", "password"]

MPathLike = Union[str, os.PathLike]


class MPath(os.PathLike):
    """
    Partially replicates pathlib.Path but with remote file support.
    """

    def __init__(
        self,
        path: Union[str, os.PathLike, MPath],
        fs: AbstractFileSystem = None,
        storage_options: Union[dict, None] = None,
        **kwargs,
    ):
        self._kwargs = {}
        if isinstance(path, MPath):
            path_str = str(path)
            self._kwargs.update(path._kwargs)
        elif isinstance(path, str):
            path_str = path
        else:
            raise TypeError(
                f"MPath has to be initialized with either a string or another MPath instance, not {path}"
            )
        if path_str.startswith("/vsicurl/"):
            self._path_str = path_str.lstrip("/vsicurl/")
            if not self._path_str.startswith(
                ("http://", "https://")
            ):  # pragma: no cover
                raise ValueError(f"wrong usage of GDAL VSI paths: {path_str}")
        else:
            self._path_str = path_str
        if fs:
            self._kwargs.update(fs=fs)
        if "fs_options" in kwargs:
            storage_options = kwargs.get("fs_options")
        if storage_options:
            self._kwargs.update(storage_options=storage_options)
        self._fs = fs
        self._storage_options = dict(
            DEFAULT_STORAGE_OPTIONS, **self._kwargs.get("storage_options") or {}
        )

    @staticmethod
    def from_dict(dictionary: dict) -> MPath:
        path_str = dictionary.get("path")
        if not path_str:
            raise ValueError(
                f"dictionary representation requires at least a 'path' item: {dictionary}"
            )
        return MPath(
            path_str,
            storage_options=dictionary.get("storage_options", {}),
            fs=dictionary.get("fs"),
        )

    @staticmethod
    def from_inp(inp: Union[dict, MPathLike], **kwargs) -> MPath:
        if isinstance(inp, dict):
            return MPath.from_dict(inp)
        elif isinstance(inp, str):
            return MPath(inp, **kwargs)
        elif isinstance(inp, MPath):
            if kwargs:
                return MPath(inp, **kwargs)
            return inp
        elif hasattr(inp, "__fspath__"):  # pragma: no cover
            return MPath(inp.__fspath__(), **kwargs)
        else:  # pragma: no cover
            raise TypeError(f"cannot construct MPath object from {inp}")

    def to_dict(self) -> dict:
        return dict(
            path=self._path_str,
            storage_options=self._storage_options,
            fs=self.fs,
        )

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
    def parent(self) -> MPath:
        return self.new(self.dirname)

    @property
    def elements(self) -> List[str]:
        return self._path_str.split("/")

    @cached_property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Return path filesystem."""
        if self._fs is not None:
            return self._fs
        elif self._path_str.startswith("s3"):
            return fsspec.filesystem(
                "s3",
                requester_pays=self._storage_options.get(
                    "requester_pays", os.environ.get("AWS_REQUEST_PAYER") == "requester"
                ),
                config_kwargs=dict(
                    connect_timeout=self._storage_options.get("timeout"),
                    read_timeout=self._storage_options.get("timeout"),
                ),
                **{
                    k: v
                    for k, v in self._storage_options.items()
                    if k not in UNALLOWED_S3_KWARGS
                },
            )
        elif self._path_str.startswith(("http://", "https://")):
            if self._storage_options.get("username") or self._storage_options.get(
                "password"
            ):  # pragma: no cover
                from aiohttp import BasicAuth

                auth = BasicAuth(
                    self._storage_options.get("username"),
                    self._storage_options.get("password"),
                )
            else:
                auth = None
            return fsspec.filesystem(
                "https",
                auth=auth,
                **{
                    k: v
                    for k, v in self._storage_options.items()
                    if k not in UNALLOWED_HTTP_KWARGS
                },
            )
        else:
            return fsspec.filesystem("file", **self._storage_options)

    def fs_session(self):
        if hasattr(self.fs, "session"):
            return self.fs.session
        else:
            return None

    @cached_property
    def protocols(self) -> Set[str]:
        """Return set of filesystem protocols."""
        if isinstance(self.fs.protocol, str):
            return set([self.fs.protocol])
        else:
            return set(self.fs.protocol)

    def without_suffix(self) -> MPath:
        return self.new(os.path.splitext(self._path_str)[0])

    def with_suffix(self, suffix: str) -> MPath:
        suffix = suffix.lstrip(".")
        return self.new(self.without_suffix() + f".{suffix}")

    def without_protocol(self) -> MPath:
        # Split the input string on "://"
        parts = self._path_str.split("://", 1)

        # Check if "://" was found in the string
        if len(parts) == 2:
            return self.new(parts[1])

        # If "://" was not found, return the input string as-is
        return self

    def with_protocol(self, protocol: str) -> MPath:
        return self.new(f"{protocol}://") / self.without_protocol()

    def startswith(self, string: str) -> bool:
        return self._path_str.startswith(string)

    def endswith(self, string: str) -> bool:
        return self._path_str.endswith(string)

    def split(self, by: str) -> List[str]:  # pragma: no cover
        return self._path_str.split(by)

    def crop(self, elements: int) -> MPath:
        return self.new("/".join(self.elements[elements:]))

    def new(self, path: MPathLike) -> "MPath":
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
        if self.is_remote():
            return True
        return os.path.isabs(self._path_str)

    def absolute_path(self, base_dir: Union[MPathLike, None] = None) -> MPath:
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
        if self.is_absolute():
            return self
        else:
            if base_dir:
                if not MPath.from_inp(base_dir).is_absolute():
                    raise TypeError("base_dir must be an absolute path.")
                return self.new(os.path.abspath(os.path.join(base_dir, self._path_str)))
            return self.new(os.path.abspath(self._path_str))

    def relative_path(
        self,
        start: Union[MPathLike, None] = None,
        base_dir: Union[MPathLike, None] = None,
    ) -> MPath:
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

    def open(self, mode: str = "r") -> Union[IO, TextIO]:
        """Open file."""
        return self.fs.open(self._path_str, mode)

    def read_text(self) -> str:
        """Open and return file content as text."""
        with self.open() as src:
            return src.read()

    def makedirs(self, exist_ok: bool = True) -> None:
        """Create all parent directories for path."""
        # create parent directories on local filesystems
        if self.fs.protocol == "file":
            # if path has no suffix, assume a file path and only create parent directories
            logger.debug("create directory %s", str(self))
            self.fs.makedirs(self, exist_ok=exist_ok)

    def ls(
        self, detail: bool = False, absolute_paths: bool = True
    ) -> List[Union[MPath, dict]]:
        def _create_full_path(path):
            # s3fs returns paths without protocol ("s3://"), therefore we have to append it again:
            if absolute_paths and "s3" in self.protocols:
                return self.new(path).with_protocol("s3")
            return self.new(path)

        if detail:
            # return as is but convert "name" from string to MPath instead
            return [
                {
                    k: (_create_full_path(v) if k == "name" else v)
                    for k, v in path_info.items()
                }
                for path_info in self.fs.ls(self._path_str, detail=detail)
            ]
        else:
            return [
                _create_full_path(path)
                for path in self.fs.ls(self._path_str, detail=detail)
            ]

    def rm(self, recursive: bool = False, ignore_errors: bool = False) -> None:
        try:
            self.fs.rm(str(self), recursive=recursive)
        except FileNotFoundError:
            if ignore_errors:
                pass
            else:  # pragma: no cover
                raise

    def size(self) -> int:
        return self.fs.size(self._path_str)

    def joinpath(self, *other: List[MPathLike]) -> MPath:
        """Join path with other."""
        return self.new(os.path.join(self._path_str, *list(map(str, other))))

    def as_gdal_str(self) -> str:
        """Return path as GDAL VSI string."""
        if self._path_str.startswith(("http://", "https://")):
            return "/vsicurl/" + self._path_str
        elif self._path_str.startswith("s3://"):
            return self._path_str.replace("s3://", "/vsis3/")
        else:
            return self._path_str

    def gdal_env_params(
        self,
        opts: Union[dict, None] = None,
        allowed_remote_extensions: Union[List[str], None] = None,
    ) -> dict:
        """
        Return a merged set of custom and default GDAL/rasterio Env options.

        If is_remote is set to True, the default GDAL_HTTP_OPTS are appended.

        Parameters
        ----------
        opts : dict or None
            Explicit GDAL options.
        is_remote : bool
            Indicate whether Env is for a remote file.

        Returns
        -------
        dictionary
        """
        user_opts = {} if opts is None else dict(**opts)

        # for remote paths, we need some special settings
        if self.is_remote():
            gdal_opts = dict(GDALHTTPOptions())

            # we cannot know at this point which file types the VRT or STACTA JSON
            # is pointing to, so in order to play safe, we remove the extensions constraint here
            if self.suffix in (".vrt", ".json"):
                try:
                    gdal_opts.pop("CPL_VSIL_CURL_ALLOWED_EXTENSIONS")
                except KeyError:  # pragma: no cover
                    pass

            # limit requests only to allowed extensions
            else:
                default_remote_extensions = gdal_opts.get(
                    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ""
                ).split(", ")
                if allowed_remote_extensions:
                    extensions = allowed_remote_extensions.split(",") + (
                        default_remote_extensions
                    )
                extensions = default_remote_extensions + [self.suffix]
                # make sure current path extension is added to allowed_remote_extensions
                gdal_opts.update(
                    CPL_VSIL_CURL_ALLOWED_EXTENSIONS=", ".join(
                        set([ext for ext in extensions if ext != ""])
                    )
                )

            # secure HTTP credentials
            if self.fs.kwargs.get("auth"):
                gdal_opts.update(
                    GDAL_HTTP_USERPWD=f"{self.fs.kwargs['auth'].login}:{self.fs.kwargs['auth'].password}"
                )

            # if a custom S3 endpoint is used, we need to deactivate these AWS options
            if self._endpoint_url:
                gdal_opts.update(AWS_VIRTUAL_HOSTING=False, AWS_HTTPS=False)

            # merge everything with user options
            gdal_opts.update(user_opts)

        # reading only locally, go with user options
        else:
            gdal_opts = user_opts

        logger.debug("using GDAL options: %s", gdal_opts)
        return gdal_opts

    @cached_property
    def _endpoint_url(self) -> Union[str, None]:
        # GDAL parses the paths in a weird way, so we have to be careful with a custom
        # endpoint
        if hasattr(self.fs, "endpoint_url") and isinstance(self.fs.endpoint_url, str):
            return self.fs.endpoint_url.lstrip("http://").lstrip("https://")
        else:
            return None

    def rio_session(self) -> "rasterio.session.Session":
        if self.fs_session():
            # rasterio accepts a Session object but only a boto3.session.Session
            # object and not a aiobotocore.session.AioSession which we get from fsspec
            return RioSession.from_path(
                self._path_str,
                aws_access_key_id=self.fs.key,
                aws_secret_access_key=self.fs.secret,
                endpoint_url=self._endpoint_url,
                requester_pays=self.fs.storage_options.get("requester_pays", False),
            )
        else:
            return RioSession.from_path(self._path_str)

    def rio_env_config(
        self,
        opts: Union[dict, None] = None,
        allowed_remote_extensions: Union[List[str], None] = None,
    ) -> dict:
        """Return configuration parameters for rasterio.Env()."""
        out = self.gdal_env_params(
            opts=opts,
            allowed_remote_extensions=allowed_remote_extensions,
        )
        if self.is_remote():
            out.update(session=self.rio_session())
        return out

    def rio_env(
        self,
        opts: Union[dict, None] = None,
        allowed_remote_extensions: Union[List[str], None] = None,
    ) -> rasterio.Env:
        """Return preconfigured rasterio.Env context manager for path."""
        return rasterio.Env(
            **self.rio_env_config(
                opts=opts, allowed_remote_extensions=allowed_remote_extensions
            )
        )

    def fio_session(self) -> "fiona.session.Session":
        if self.fs_session():
            # fiona accepts a Session object but only a boto3.session.Session
            # object and not a aiobotocore.session.AioSession which we get from fsspec
            return FioSession.from_path(
                self._path_str,
                aws_access_key_id=self.fs.key,
                aws_secret_access_key=self.fs.secret,
                endpoint_url=self._endpoint_url,
                requester_pays=self.fs.storage_options.get("requester_pays", False),
            )
        else:
            return FioSession.from_path(self._path_str)

    def fio_env_config(
        self,
        opts: Union[dict, None] = None,
        allowed_remote_extensions: Union[List[str], None] = None,
    ) -> dict:
        """Return configuration parameters for fiona.Env()."""
        out = self.gdal_env_params(
            opts=opts,
            allowed_remote_extensions=allowed_remote_extensions,
        )
        if self.is_remote():
            out.update(session=self.fio_session())
        return out

    def fio_env(
        self,
        opts: Union[dict, None] = None,
        allowed_remote_extensions: Union[List[str], None] = None,
    ) -> fiona.Env:
        """Return preconfigured fiona.Env context manager for path."""
        return fiona.Env(
            **self.fio_env_config(
                opts=opts, allowed_remote_extensions=allowed_remote_extensions
            )
        )

    def __truediv__(self, other: MPathLike) -> MPath:
        """Short for self.joinpath()."""
        return self.joinpath(other)

    def __add__(self, other: MPathLike) -> "MPath":
        """Short for self.joinpath()."""
        return self.new(str(self) + other)

    def __eq__(self, other: MPathLike):
        if isinstance(other, str):
            return str(self) == other
        else:
            return hash(self) == hash(MPath(other))

    def __gt__(self, other: MPathLike):  # pragma: no cover
        return str(self) > str(MPath(other))

    def __ge__(self, other: MPathLike):  # pragma: no cover
        return str(self) >= str(MPath(other))

    def __lt__(self, other: MPathLike):  # pragma: no cover
        return str(self) < str(MPath(other))

    def __le__(self, other: MPathLike):  # pragma: no cover
        return str(self) <= str(MPath(other))

    def __repr__(self):
        return f"<mapchete.io.MPath object: {self._path_str}, storage_options={self._storage_options}>"

    def __hash__(self):
        return hash(repr(self))


def path_is_remote(path, **kwargs) -> bool:
    """
    Determine whether file path is remote or local.

    Parameters
    ----------
    path : path to file

    Returns
    -------
    is_remote : bool
    """
    return MPath.from_inp(path, **kwargs).is_remote()


def path_exists(
    path, fs: Union[fsspec.AbstractFileSystem, None] = None, **kwargs
) -> bool:
    """
    Check if file exists either remote or local.

    Parameters
    ----------
    path : path to file

    Returns
    -------
    exists : bool
    """
    return MPath.from_inp(path, fs=fs, **kwargs).exists()


def absolute_path(
    path: Union[MPathLike, None] = None,
    base_dir: Union[MPathLike, None] = None,
    **kwargs,
) -> MPath:
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
    return MPath.from_inp(path, **kwargs).absolute_path(base_dir=base_dir)


def relative_path(
    path: Union[MPathLike, None] = None,
    base_dir: Union[MPathLike, None] = None,
    **kwargs,
) -> MPath:
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
    return MPath.from_inp(path, **kwargs).relative_path(base_dir=base_dir)


def makedirs(
    path, fs: Union[fsspec.AbstractFileSystem, None] = None, **kwargs
) -> None:  # pragma: no cover
    """
    Silently create all subdirectories of path if path is local.

    Parameters
    ----------
    path : path
    """
    MPath.from_inp(path, fs=fs, **kwargs).makedirs()


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
        sort_attribute = batch_sort_property(config.output_reader.tile_path_schema)
        logger.debug(
            "sort process tiles by %s, this could take a while", sort_attribute
        )
        process_tiles_batches = _batch_tiles_by_attribute(process_tiles, sort_attribute)
    elif output_tiles:
        sort_attribute = batch_sort_property(config.output_reader.tile_path_schema)
        logger.debug("sort output tiles by %s, this could take a while", sort_attribute)
        output_tiles_batches = _batch_tiles_by_attribute(output_tiles, sort_attribute)

    # Some HTTP endpoints won't allow ls() on them, so we will have to
    # request tile by tile in order to determine whether they exist or not.
    # This flag will trigger this further down.
    is_https_without_ls = False
    if "https" in config.output_reader.path.protocols:
        try:
            config.output_reader.path.ls()
        except FileNotFoundError:  # pragma: no cover
            metadata_json = config.output_reader.path / "metadata.json"
            if not metadata_json.exists():
                raise FileNotFoundError(
                    f"TileDirectory does not seem to exist or metadata.json is not available: {config.output_reader.path}"
                )
            is_https_without_ls = True

    if process_tiles_batches:
        yield from _process_tiles_batches_exist(
            process_tiles_batches, config, is_https_without_ls
        )
    elif output_tiles_batches:
        yield from _output_tiles_batches_exist(
            output_tiles_batches, config, is_https_without_ls
        )


def _batch_tiles_by_attribute(tiles: List[BufferedTile], attribute: str = "row"):
    ordered = defaultdict(set)
    for tile in tiles:
        ordered[getattr(tile, attribute)].add(tile)
    return ((t for t in ordered[key]) for key in sorted(list(ordered.keys())))


def _output_tiles_batches_exist(output_tiles_batches, config, is_https_without_ls):
    with Executor(concurrency="threads") as executor:
        for batch in executor.as_completed(
            _output_tiles_batch_exists,
            (list(b) for b in output_tiles_batches),
            fargs=(config, is_https_without_ls),
        ):
            yield from batch.result()


def _output_tiles_batch_exists(tiles, config, is_https_without_ls):
    if tiles:
        zoom = tiles[0].zoom
        # determine output paths
        output_paths = {
            config.output_reader.get_path(output_tile).crop(-3): output_tile
            for output_tile in tiles
        }
        # iterate through output tile rows and determine existing output tiles
        existing_tiles = _existing_tiles(
            output_rows=[tiles[0].row],
            output_paths=output_paths,
            config=config,
            zoom=zoom,
            is_https_without_ls=is_https_without_ls,
        )
        return [(tile, tile in existing_tiles) for tile in tiles]
    else:  # pragma: no cover
        return []


def _process_tiles_batches_exist(process_tiles_batches, config, is_https_without_ls):
    with Executor(concurrency="threads") as executor:
        for batch in executor.as_completed(
            _process_tiles_batch_exists,
            (list(b) for b in process_tiles_batches),
            fargs=(config, is_https_without_ls),
        ):
            yield from batch.result()


def _process_tiles_batch_exists(tiles, config, is_https_without_ls):
    if tiles:
        zoom = tiles[0].zoom
        # determine output tile rows
        output_rows = sorted(
            list(set(t.row for t in config.output_pyramid.intersecting(tiles[0])))
        )
        # determine output paths
        output_paths = {
            config.output_reader.get_path(output_tile).crop(-3): process_tile
            for process_tile in tiles
            for output_tile in config.output_pyramid.intersecting(process_tile)
        }
        # iterate through output tile rows and determine existing process tiles
        existing_tiles = _existing_tiles(
            output_rows=output_rows,
            output_paths=output_paths,
            config=config,
            zoom=zoom,
            is_https_without_ls=is_https_without_ls,
        )
        return [(tile, tile in existing_tiles) for tile in tiles]
    else:  # pragma: no cover
        return []


def _existing_tiles(
    output_rows=None,
    output_paths=None,
    config=None,
    zoom=None,
    is_https_without_ls=False,
):
    existing_tiles = set()
    for row in output_rows:
        logger.debug("check existing tiles in row %s", row)
        rowpath = config.output_reader.path.joinpath(zoom, row)
        logger.debug("rowpath: %s", rowpath)

        if is_https_without_ls:  # pragma: no cover
            for path, tile in output_paths.items():
                full_path = rowpath / path.elements[-1]
                if full_path.exists():
                    existing_tiles.add(tile)

        else:
            try:
                for path in rowpath.ls(detail=False):
                    path = path.crop(-3)
                    if path in output_paths:
                        existing_tiles.add(output_paths[path])
            # this happens when the row directory does not even exist
            except FileNotFoundError:
                pass

    return existing_tiles


def fs_from_path(path: MPathLike, **kwargs) -> fsspec.AbstractFileSystem:
    """Guess fsspec FileSystem from path and initialize using the desired options."""
    path = path if isinstance(path, MPath) else MPath(path, **kwargs)
    return path.fs


def batch_sort_property(tile_path_schema: str) -> str:
    """
    Determine by which property according to the schema batches should be sorted.

    In order to reduce S3 requests this function determines the root directory name in a
    TileDirectory structure on which MPath.ls() is to be called.

    "{zoom}/{row}/{col}.{extension}" -> "row": batches should be collected by tile rows
    "{zoom}/{col}/{row}.{extension}" -> "col": batches should be collected by tile columns
    """
    # split into path elements
    elements = tile_path_schema.split("/")
    # reverse so we can start from the end
    elements.reverse()
    out = "row"
    # start from the end and take the last (i.e. first from the original schema) element
    for element in elements:
        if element in ["{row}", "{col}"]:
            out = element
    return out.lstrip("{").rstrip("}")
