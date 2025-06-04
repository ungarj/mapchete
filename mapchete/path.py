"""Functions handling paths and file systems."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import warnings
from collections import defaultdict
from datetime import datetime
from functools import cached_property
from io import TextIOWrapper
from typing import (
    IO,
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Set,
    TextIO,
    NamedTuple,
    Union,
)

from aiohttp import BasicAuth
import fiona
import fsspec
import oyaml as yaml
import rasterio
from fiona.session import Session as FioSession
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from rasterio.session import Session as RioSession
from retry.api import retry_call

from mapchete.executor import Executor
from mapchete.pretty import pretty_bytes
from mapchete.protocols import ObserverProtocol
from mapchete.settings import GDALHTTPOptions, IORetrySettings, mapchete_options
from mapchete.tile import BatchBy, BufferedTile
from mapchete.timer import Timer
from mapchete.types import MPathLike, Progress

logger = logging.getLogger(__name__)

UNALLOWED_S3_KWARGS = ["timeout"]
UNALLOWED_HTTP_KWARGS = ["username", "password"]


def _retry(func):
    """Custom retry decorator for MPath methods."""

    def _call_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exception:  # pragma: no cover
            # This is a hack because some tool using aiohttp does not raise a
            # ClientResponseError directly but masks it as a generic Exception and thus
            # preventing our retry mechanism to kick in.
            # Also, s3fs sometimes throws a generic OSError which we need to catch and convert here.
            if repr(exception).startswith('Exception("ClientResponseError') or (
                isinstance(exception, OSError)
                and "An error occurred (BadRequest) when calling the PutObject operation: N/A"
                in repr(exception)
            ):  # pragma: no cover
                raise ConnectionError(repr(exception)).with_traceback(
                    exception.__traceback__
                )
            raise exception

    def wrapper(*args, **kwargs):
        return retry_call(
            _call_func, args, kwargs, logger=logger, **IORetrySettings().model_dump()
        )

    return wrapper


class DirectoryContent(NamedTuple):
    root: MPath
    subdirs: List[MPath]
    files: List[MPath]


class MPath(os.PathLike):
    """
    Partially replicates pathlib.Path but with remote file support.
    """

    storage_options: dict = {"asynchronous": False, "timeout": None}
    _gdal_options: dict

    def __init__(
        self,
        path: Union[str, os.PathLike, MPath],
        fs: Optional[AbstractFileSystem] = None,
        storage_options: Union[dict, None] = None,
        info_dict: Union[dict, None] = None,
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
        for option in ["fs_options", "protocol"]:
            if option in kwargs:
                storage_options = kwargs.get(option)
        if storage_options:
            self._kwargs.update(storage_options=storage_options)
        self.storage_options = dict(
            self.storage_options, **self._kwargs.get("storage_options") or {}
        )
        self._fs = fs
        self._info = info_dict
        self._gdal_options = dict()

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

    @staticmethod
    def cwd() -> MPath:
        return MPath(os.getcwd())

    @_retry
    def info(self, refresh: bool = False) -> dict:
        if refresh or self._info is None:
            logger.debug("%s: make self.fs.info() call ...", str(self))
            self._info = self.fs.info(self._path_str)
        return self._info

    @_retry
    def checksum(self, algo: str = "sha256", block_size: int = 1024 * 1024) -> str:
        """Stream a file and compute its checksum."""
        hasher = hashlib.new(algo)

        with self.open("rb") as src:
            for chunk in iter(lambda: src.read(block_size), b""):
                hasher.update(chunk)  # type: ignore

        return hasher.hexdigest()

    def to_dict(self) -> dict:
        return dict(
            path=self._path_str,
            storage_options=self.storage_options,
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
        elif self._path_str.startswith("s3://"):
            # move 'region_name' up to client_kwargs in order to have effect
            if self.storage_options.get("region_name"):
                client_kwargs = self.storage_options.get("client_kwargs", {})
                client_kwargs.update(
                    region_name=self.storage_options.pop("region_name")
                )
                self.storage_options.update(client_kwargs=client_kwargs)
            return fsspec.filesystem(
                "s3",
                requester_pays=self.storage_options.get(
                    "requester_pays", os.environ.get("AWS_REQUEST_PAYER") == "requester"
                ),
                config_kwargs=dict(
                    connect_timeout=self.storage_options.get("timeout"),
                    read_timeout=self.storage_options.get("timeout"),
                ),
                **{
                    k: v
                    for k, v in self.storage_options.items()
                    if k not in UNALLOWED_S3_KWARGS
                },
            )
        elif self._path_str.startswith(("http://", "https://")):
            username = self.storage_options.get("username")
            if username:
                auth = BasicAuth(
                    login=username,
                    password=self.storage_options.get("password", ""),
                )
            else:
                auth = None
            return fsspec.filesystem(
                "https",
                auth=auth,
                **{
                    k: v
                    for k, v in self.storage_options.items()
                    if k not in UNALLOWED_HTTP_KWARGS
                },
            )
        else:
            return fsspec.filesystem("file", **self.storage_options)

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

    def new(
        self,
        path: Union[MPathLike, Dict[str, Any]],
        relative_to_self: bool = False,
        info_dict: Optional[Dict[str, Any]] = None,
    ) -> MPath:
        """Create a new MPath instance with given path."""
        if isinstance(path, str):
            path_info = info_dict
            path_str = path
        elif isinstance(path, dict):
            # this is for S3 object dictionaries
            path_info = path
            path_str = path_info.get("name", path_info.get("Key"))
            if path_str is None:  # pragma: no cover
                raise ValueError(f"cannot create MPath from dictionary: {path_info}")
            # S3 json does not return path information with protocol, so let's add it manually
            if "s3" in self.protocols:
                path_str = f"s3://{path_str}"
        elif isinstance(path, MPath):
            path_info = info_dict or path._info
            path_str = path._path_str
        else:  # pragma: no cover
            raise TypeError(f"invalid path type: {path}")

        if relative_to_self:
            path_str = os.path.relpath(path_str, start=self)

        return MPath(path_str, info_dict=path_info, **self._kwargs)

    @_retry
    def exists(self, weak_check: bool = False) -> bool:
        """Check if path exists."""
        # avoid HEAD call if _info object is already there
        if weak_check and self._info is not None:  # pragma: no cover
            return True
        logger.debug("%s: make self.fs.exists() call ...", str(self))
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

    def relative_to(self, other: MPathLike) -> str:
        return str(
            self.without_protocol().relative_path(
                MPath.from_inp(other).without_protocol()
            )
        )

    def open(
        self, mode: str = "r", **kwargs
    ) -> Union[IO, TextIO, TextIOWrapper, AbstractBufferedFile]:
        """Open file."""
        logger.debug("%s: make self.fs.open() call ...", str(self))
        return self.fs.open(self._path_str, mode, **kwargs)

    @_retry
    def read_text(self) -> str:
        """Open and return file content as text."""
        try:
            with self.open() as src:
                return str(src.read())
        except FileNotFoundError:
            raise FileNotFoundError(f"{str(self)} not found")
        except Exception as exception:  # pragma: no cover
            if self.exists():
                logger.exception(exception)
                raise exception
            else:
                raise FileNotFoundError(f"{str(self)} not found")

    def read_json(self) -> dict:
        """Read local or remote."""
        return json.loads(self.read_text())

    @_retry
    def write_json(self, params: dict, sort_keys=True, indent=4) -> None:
        """Write local or remote."""
        logger.debug(f"write {params} to {self}")
        self.parent.makedirs()
        with self.open(mode="w") as dst:
            json.dump(params, dst, sort_keys=sort_keys, indent=indent)

    def read_yaml(self) -> dict:
        """Read local or remote."""
        return yaml.safe_load(self.read_text())

    @_retry
    def write_yaml(self, params: dict) -> None:
        """Write local or remote."""
        logger.debug(f"write {params} to {self}")
        self.parent.makedirs()
        with self.open(mode="w") as dst:
            yaml.dump(params, dst)

    def makedirs(self, exist_ok: bool = True) -> None:
        """Create all parent directories for path."""
        # create parent directories on local filesystems
        if "file" in self.fs.protocol:
            # if path has no suffix, assume a file path and only create parent directories
            logger.debug("create directory %s", str(self))
            self.fs.makedirs(self, exist_ok=exist_ok)

    @_retry
    def ls(
        self, absolute_paths: bool = True, detail: Optional[bool] = None
    ) -> List[MPath]:
        if detail is not None:  # pragma: no cover
            warnings.warn(DeprecationWarning("'detail' kwarg is deprecated."))
        logger.debug("%s: make self.fs.ls() call ...", str(self))
        return [
            self.new(path_info, relative_to_self=not absolute_paths)
            for path_info in self.fs.ls(self._path_str, detail=True)
        ]

    def walk(
        self,
        maxdepth: Optional[int] = None,
        topdown: bool = True,
        absolute_paths: bool = True,
        **kwargs,
    ) -> Generator[
        DirectoryContent,
        None,
        None,
    ]:
        logger.debug("%s: make self.fs.walk() call ...", str(self))
        for root, subdirs, files in self.fs.walk(
            str(self), maxdepth=maxdepth, topdown=topdown, detail=True, **kwargs
        ):
            if isinstance(root, str):
                mpath_root = self.new(root, relative_to_self=not absolute_paths)
                if "s3" in self.protocols:
                    mpath_root = mpath_root.with_protocol("s3")
                yield DirectoryContent(
                    root=mpath_root,
                    subdirs=[
                        self.new(mpath_root / subdir_str, info_dict=subdir_info)
                        for subdir_str, subdir_info in subdirs.items()  # type: ignore
                    ],
                    files=[
                        self.new(mpath_root / file_str, info_dict=file_info)
                        for file_str, file_info in files.items()  # type: ignore
                    ],
                )

    def paginate(
        self, items_per_page: int = 1000
    ) -> Generator[List[MPath], None, None]:
        """
        List all files in directory and all subdirectories.

        On S3 paths, this uses the 'list_objects_v2' paginator from boto3.

        On other file systems, it replicates the behavior of the S3 paginator
        """
        if "s3" in self.protocols:
            import boto3

            bucket = self.without_protocol().elements[0]
            prefix = "/".join(self.without_protocol().elements[1:])
            s3_client = boto3.client(
                "s3",
                region_name=self.storage_options.get("region_name"),
                endpoint_url=self.storage_options.get("endpoint_url"),
                aws_access_key_id=self.storage_options.get("key"),
                aws_secret_access_key=self.storage_options.get("secret"),
            )
            for page in s3_client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket,
                Prefix=prefix,
                PaginationConfig={"PageSize": items_per_page},
            ):
                yield [self.new(obj_dict) for obj_dict in page.get("Contents", [])]
        else:
            page = []
            for directory_content in self.walk():
                for file in directory_content.files:
                    page.append(file)
                    if len(page) == items_per_page:
                        yield page
                        page = []
            # yield remaining files
            if page:
                yield page

    @_retry
    def rm(self, recursive: bool = False, ignore_errors: bool = False) -> None:
        if (
            not recursive and not ignore_errors and self.is_directory()
        ):  # pragma: no cover
            warnings.warn(f"{self} is a directory, use 'recursive' flag to delete")
        try:
            logger.debug("%s: make self.fs.rm() call ...", str(self))
            self.fs.rm(str(self), recursive=recursive)
        except FileNotFoundError:
            if ignore_errors:
                pass
            else:  # pragma: no cover
                raise
        finally:
            self._info = None

    @_retry
    def cp(
        self,
        destination: MPathLike,
        overwrite: bool = False,
        exists_ok: bool = False,
        read_block_size: int = 0,
        chunksize: int = 1024 * 1024,  # 1MB
        observers: Optional[List[ObserverProtocol]] = None,
    ) -> None:
        """
        Copy file contents to destination.
        """
        from mapchete.commands.observer import Observers

        all_observers = Observers(observers)
        dst_path = MPath.from_inp(destination)

        if overwrite:
            pass
        elif dst_path.exists(weak_check=True):
            if exists_ok:
                msg = f"{str(dst_path)} already exists"
                all_observers.notify(message=msg)
                logger.debug(msg)
                return
            else:
                raise IOError(f"{dst_path} already exists")

        # create parent directories on local filesystems
        dst_path.parent.makedirs()

        try:
            # copy either within a filesystem or between filesystems
            msg = f"copy {self} to {dst_path} {'(overwrite)' if overwrite else ''}..."
            all_observers.notify(message=msg)
            logger.debug(msg)
            with Timer() as duration:
                if self.fs == dst_path.fs:
                    self.fs.copy(str(self), str(dst_path))
                else:
                    with self.open("rb", block_size=read_block_size) as src:
                        with dst_path.open("wb") as dst:
                            total_size = self.size()
                            transferred = 0
                            all_observers.notify(
                                progress=Progress(current=transferred, total=total_size)
                            )
                            for chunk in iter(lambda: src.read(chunksize), b""):
                                transferred += chunksize
                                dst.write(chunk)  # type: ignore
                                all_observers.notify(
                                    progress=Progress(
                                        current=min([transferred, total_size]),
                                        total=total_size,
                                    )
                                )
            all_observers.notify(message=f"copied in {duration}")
        except Exception:  # pragma: no cover
            # delete file if something failed
            # dst_path should either not even exist and if, the overwrite flag is active anyways
            dst_path.rm(ignore_errors=True)
            raise

    def size(self) -> int:
        return self.info().get("size", self.info().get("Size"))  # type: ignore

    def pretty_size(self) -> str:
        return pretty_bytes(self.size())

    def last_modified(self) -> datetime:
        # for S3 objects
        last_modified = self.info().get("LastModified")
        mtime = self.info().get("mtime")
        if last_modified:
            return last_modified
        # for local files
        elif mtime:
            return datetime.fromtimestamp(mtime)
        else:  # pragma: no cover
            raise ValueError("Object timestamp could not be determined.")

    def is_directory(self) -> bool:
        try:
            # for S3 objects use the possible cached info directory
            if "StorageClass" in self.info():  # pragma: no cover
                return self.info().get("StorageClass") == "DIRECTORY"
        except FileNotFoundError:  # pragma: no cover
            pass
        logger.debug("%s: make self.fs.isdir() call ...", str(self))
        return self.fs.isdir(self._path_str)

    def joinpath(self, *other: Union[MPathLike, List[MPathLike]]) -> MPath:
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
                gdal_opts.update(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR")

            # limit requests only to allowed extensions
            else:
                default_remote_extensions = gdal_opts.get(
                    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ""
                ).split(", ")
                extensions = default_remote_extensions + [self.suffix]
                if allowed_remote_extensions:
                    extensions += allowed_remote_extensions
                # make sure current path extension is added to allowed_remote_extensions
                gdal_opts.update(
                    CPL_VSIL_CURL_ALLOWED_EXTENSIONS=", ".join(
                        set([ext for ext in extensions if ext != ""])
                    )
                )

            # secure HTTP credentials
            auth = getattr(self.fs, "kwargs", {}).get("auth")
            if auth:
                gdal_opts.update(GDAL_HTTP_USERPWD=f"{auth.login}:{auth.password}")

            # if a custom S3 endpoint is used, we need to deactivate these AWS options
            if self._endpoint_url:
                gdal_opts.update(
                    AWS_VIRTUAL_HOSTING=False,
                    AWS_HTTPS=self._gdal_options.get("aws_https", False),
                )

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
        endpoint_url = getattr(self.fs, "endpoint_url", None)
        if endpoint_url:
            self._gdal_options.update(aws_https=endpoint_url.startswith("https://"))
            return endpoint_url.replace("http://", "").replace("https://", "")
        else:
            return None

    def rio_session(self) -> RioSession:
        return RioSession.from_path(
            self._path_str,
            aws_access_key_id=self.storage_options.get("key", None),
            aws_secret_access_key=self.storage_options.get("secret", None),
            endpoint_url=self._endpoint_url,
            requester_pays=self.storage_options.get("requester_pays", False),
        )

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

    def fio_session(self) -> FioSession:
        return FioSession.from_path(
            self._path_str,
            aws_access_key_id=self.storage_options.get("key", None),
            aws_secret_access_key=self.storage_options.get("secret", None),
            endpoint_url=self._endpoint_url,
            requester_pays=self.storage_options.get("requester_pays", False),
        )

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
        return self.new(str(self) + str(other))

    def __eq__(self, other):
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
        return f"<mapchete.io.MPath object: {self._path_str}, storage_options={self.storage_options}>"

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
    path: MPathLike,
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
    path: MPathLike,
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
    config,
    output_tiles: Optional[Generator[BufferedTile, None, None]] = None,
    output_tiles_batches: Optional[
        Generator[Generator[BufferedTile, None, None], None, None]
    ] = None,
    process_tiles: Optional[Generator[BufferedTile, None, None]] = None,
    process_tiles_batches: Optional[
        Generator[Generator[BufferedTile, None, None], None, None]
    ] = None,
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
            if process_tiles:  # pragma: no cover
                yield from process_tiles
            elif output_tiles:  # pragma: no cover
                yield from output_tiles
            elif process_tiles_batches:
                for batch in process_tiles_batches:
                    yield from batch
            elif output_tiles_batches:  # pragma: no cover
                for batch in output_tiles_batches:
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

    if process_tiles_batches:
        yield from _process_tiles_batches_exist(
            process_tiles_batches,
            config,
            _is_https_without_ls(config.output_reader.path),
        )
    elif output_tiles_batches:
        yield from _output_tiles_batches_exist(
            output_tiles_batches,
            config,
            _is_https_without_ls(config.output_reader.path),
        )


def _is_https_without_ls(path: MPath, default_file: str = "metadata.json") -> bool:
    # Some HTTP endpoints won't allow ls() on them, so we will have to
    # request tile by tile in order to determine whether they exist or not.
    # This flag will trigger this further down.
    is_https_without_ls = False
    if "https" in path.protocols:
        try:
            path.ls()
        except FileNotFoundError:  # pragma: no cover
            metadata_json = path / default_file
            if not metadata_json.exists():
                raise FileNotFoundError(
                    f"TileDirectory does not seem to exist or {default_file} is not available: {path}"
                )
            is_https_without_ls = True
    return is_https_without_ls


def _batch_tiles_by_attribute(
    tiles: Generator[BufferedTile, None, None], attribute: str = "row"
) -> Generator[Generator[BufferedTile, None, None], None, None]:
    ordered = defaultdict(set)
    for tile in tiles:
        ordered[getattr(tile, attribute)].add(tile)
    return ((t for t in ordered[key]) for key in sorted(list(ordered.keys())))


def _output_tiles_batches_exist(
    output_tiles_batches: Generator[Generator[BufferedTile, None, None], None, None],
    config,
    is_https_without_ls,
):
    with Executor(concurrency=mapchete_options.tiles_exist_concurrency) as executor:
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
        existing_tiles = _existing_output_tiles(
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
    with Executor(concurrency=mapchete_options.tiles_exist_concurrency) as executor:
        for batch in executor.as_completed(
            _process_tiles_batch_exists,
            (list(b) for b in process_tiles_batches),
            fargs=(config, is_https_without_ls),
        ):
            yield from batch.result()


def _process_tiles_batch_exists(tiles, config, is_https_without_ls):
    def _all_output_tiles_exist(process_tile, existing_output_tiles):
        # a process tile only exists if all of its output tiles exist
        for output_tile in config.output_pyramid.intersecting(process_tile):
            if output_tile not in existing_output_tiles:
                return False
        else:
            return True

    if tiles:
        zoom = tiles[0].zoom
        # determine output tile rows
        output_rows = sorted(
            list(set(t.row for t in config.output_pyramid.intersecting(tiles[0])))
        )
        # determine all output paths
        output_paths = {
            config.output_reader.get_path(output_tile).crop(-3): output_tile
            for process_tile in tiles
            for output_tile in config.output_pyramid.intersecting(process_tile)
        }
        # iterate through output tile rows and determine existing process tiles
        existing_output_tiles = _existing_output_tiles(
            output_rows=output_rows,
            output_paths=output_paths,
            config=config,
            zoom=zoom,
            is_https_without_ls=is_https_without_ls,
        )
        return [
            (tile, _all_output_tiles_exist(tile, existing_output_tiles))
            for tile in tiles
        ]
    else:  # pragma: no cover
        return []


def _existing_output_tiles(
    output_rows: List[BufferedTile],
    output_paths: dict,
    config,
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
    return path.fs if isinstance(path, MPath) else MPath(path, **kwargs).fs


def batch_sort_property(tile_path_schema: str) -> BatchBy:
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
    return BatchBy[out.lstrip("{").rstrip("}")]
