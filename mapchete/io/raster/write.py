from __future__ import annotations

from importlib.util import find_spec
import logging
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from typing import Generator, Optional, Union

import numpy.ma as ma
import rasterio
from rasterio.io import DatasetWriter, MemoryFile, BufferedDatasetWriter
from rasterio.profiles import Profile

from mapchete.io.raster.array import extract_from_array
from mapchete.path import MPath, MPathLike
from mapchete.protocols import GridProtocol
from mapchete.validate import validate_write_window_params

logger = logging.getLogger(__name__)


@contextmanager
def rasterio_write(
    path: MPathLike, mode: str = "w", in_memory: bool = True, *args, **kwargs
) -> Generator[
    Union[BufferedDatasetWriter, DatasetWriter],
    None,
    None,
]:
    """
    Wrap rasterio.open() but handle bucket upload if path is remote.

    Returns
    -------
    RasterioRemoteWriter if target is remote, otherwise return rasterio.open().
    """
    path = MPath.from_inp(path)

    try:
        if path.is_remote():
            if "s3" in path.protocols and not find_spec("boto3"):  # pragma: no cover
                raise ImportError("please install [s3] extra to write remote files")
            with RasterioRemoteWriter(
                path, in_memory=in_memory, *args, **kwargs
            ) as dst:
                yield dst
        else:
            with path.rio_env() as env:
                logger.debug("writing %s with GDAL options %s", str(path), env.options)
                path.parent.makedirs(exist_ok=True)
                with rasterio.open(path, mode=mode, *args, **kwargs) as dst:
                    yield dst
    except Exception as exc:  # pragma: no cover
        logger.exception(exc)
        logger.debug("remove %s ...", str(path))
        path.rm(ignore_errors=True)
        raise


class RasterioRemoteMemoryWriter:
    path: MPath
    _sink: Union[BufferedDatasetWriter, DatasetWriter]

    def __init__(self, path: MPathLike, *args, **kwargs):
        logger.debug("open RasterioRemoteMemoryWriter for path %s", path)
        self.path = MPath.from_inp(path)
        self.fs = self.path.fs
        self._dst = MemoryFile()
        self._open_args = args
        self._open_kwargs = kwargs
        self._sink = None

    def __enter__(self):
        self._sink = self._dst.open(*self._open_args, **self._open_kwargs)
        return self._sink

    def __exit__(self, exc_type, exc_value, exc_traceback):
        try:
            self._sink.close()
            if exc_value is None:
                logger.debug("upload rasterio MemoryFile to %s", self.path)
                with self.fs.open(self.path, "wb") as dst:
                    dst.write(self._dst.getbuffer())
        finally:
            logger.debug("close rasterio MemoryFile")
            self._dst.close()


class RasterioRemoteTempFileWriter:
    path: MPath
    _sink: Union[BufferedDatasetWriter, DatasetWriter]

    def __init__(self, path: MPathLike, *args, **kwargs):
        logger.debug("open RasterioTempFileWriter for path %s", path)
        self.path = MPath.from_inp(path)
        self.fs = self.path.fs
        self._dst = NamedTemporaryFile(suffix=self.path.suffix)
        self._open_args = args
        self._open_kwargs = kwargs
        self._sink = None

    def __enter__(self):
        self._sink = rasterio.open(
            self._dst.name, "w+", *self._open_args, **self._open_kwargs
        )
        return self._sink

    def __exit__(self, exc_type, exc_value, exc_traceback):
        try:
            self._sink.close()
            if exc_value is None:
                logger.debug("upload TempFile %s to %s", self._dst.name, self.path)
                self.fs.put_file(self._dst.name, self.path)
        finally:
            logger.debug("close and remove tempfile")
            self._dst.close()


class RasterioRemoteWriter:
    path: MPath

    def __new__(
        cls, path: MPathLike, *args, in_memory: bool = True, **kwargs
    ) -> Union[RasterioRemoteMemoryWriter, RasterioRemoteTempFileWriter]:
        path = MPath.from_inp(path)
        if in_memory:
            return RasterioRemoteMemoryWriter(path, *args, **kwargs)
        else:
            return RasterioRemoteTempFileWriter(path, *args, **kwargs)


def write_raster_window(
    in_grid: GridProtocol,
    in_data: ma.MaskedArray,
    out_profile: Union[Profile, dict],
    out_path: MPathLike,
    out_grid: Optional[GridProtocol] = None,
    tags: Optional[dict] = None,
    write_empty: bool = False,
    **kwargs,
):
    """
    Write a window from a numpy array to an output file.
    """
    out_path = MPath.from_inp(out_path)
    logger.debug("write %s", out_path)

    if kwargs.get("out_tile"):  # pragma: no cover
        raise DeprecationWarning("'out_tile' is deprecated and should be 'grid'")

    out_grid = out_grid or in_grid

    validate_write_window_params(in_grid, out_grid, in_data, out_profile)

    # extract data
    window_data = extract_from_array(
        array=in_data, array_transform=in_grid.transform, out_grid=out_grid
    )

    # use transform instead of affine
    if "affine" in out_profile:
        out_profile["transform"] = out_profile.pop("affine")

    # write if there is any band with non-masked data
    if write_empty or (window_data.all() is not ma.masked):
        try:
            with rasterio_write(out_path, "w", **out_profile) as dst:
                logger.debug("write grid %s to %s", out_grid, out_path)
                dst.write(window_data.astype(out_profile["dtype"], copy=False))
                _write_tags(dst, tags)
        except Exception as e:  # pragma: no cover
            logger.exception("error while writing file %s: %s", out_path, e)
            raise
    else:
        logger.debug("array window empty, not writing %s", out_path)


def _write_tags(dst, tags):
    if tags:
        for k, v in tags.items():
            # for band specific tags
            if isinstance(k, int):
                dst.update_tags(k, **v)
            # for filewide tags
            else:
                dst.update_tags(**{k: v})
