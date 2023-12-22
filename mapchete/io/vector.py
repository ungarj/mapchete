"""Functions handling vector data."""

import logging
import warnings
from contextlib import ExitStack, contextmanager
from itertools import chain
from tempfile import NamedTemporaryFile
from typing import Any, Union

import fiona
from fiona.errors import DriverError, FionaError, FionaValueError
from fiona.io import MemoryFile
from rasterio.crs import CRS
from retry import retry
from shapely.errors import TopologicalError
from shapely.geometry import base, box, mapping, shape
from tilematrix import clip_geometry_to_srs_bounds

from mapchete.errors import MapcheteIOError, NoCRSError, NoGeoError
from mapchete.io import copy
from mapchete.io._geometry_operations import (
    _repair,
    clean_geometry_type,
    multipart_to_singleparts,
    reproject_geometry,
    segmentize_geometry,
    to_shape,
)
from mapchete.path import MPath, fs_from_path, path_exists
from mapchete.settings import IORetrySettings
from mapchete.types import Bounds
from mapchete.validate import validate_bounds

__all__ = [
    "reproject_geometry",
    "segmentize_geometry",
    "to_shape",
    "multipart_to_singleparts",
    "clean_geometry_type",
]

logger = logging.getLogger(__name__)


@contextmanager
def fiona_open(path, mode="r", **kwargs):
    """Call fiona.open but set environment correctly and return custom writer if needed."""
    path = MPath.from_inp(path)

    if "w" in mode:
        with fiona_write(path, mode=mode, **kwargs) as dst:
            yield dst

    else:
        with fiona_read(path, mode=mode, **kwargs) as src:
            yield src


@contextmanager
def fiona_read(path, mode="r", **kwargs):
    """
    Wrapper around fiona.open but fiona.Env is set according to path properties.
    """
    path = MPath.from_inp(path)

    with path.fio_env() as env:
        logger.debug("reading %s with GDAL options %s", str(path), env.options)
        with fiona.open(str(path), mode=mode, **kwargs) as src:
            yield src


@contextmanager
def fiona_write(path, mode="w", fs=None, in_memory=True, *args, **kwargs):
    """
    Wrap fiona.open() but handle bucket upload if path is remote.

    Parameters
    ----------
    path : str or MPath
        Path to write to.
    mode : str
        One of the fiona.open() modes.
    fs : fsspec.FileSystem
        Target filesystem.
    in_memory : bool
        On remote output store an in-memory file instead of writing to a tempfile.
    args : list
        Arguments to be passed on to fiona.open()
    kwargs : dict
        Keyword arguments to be passed on to fiona.open()

    Returns
    -------
    FionaRemoteWriter if target is remote, otherwise return fiona.open().
    """
    path = MPath.from_inp(path)

    try:
        if path.is_remote():
            if "s3" in path.protocols:  # pragma: no cover
                try:
                    import boto3
                except ImportError:
                    raise ImportError("please install [s3] extra to write remote files")
            with FionaRemoteWriter(
                path, fs=fs, in_memory=in_memory, *args, **kwargs
            ) as dst:
                yield dst
        else:
            with path.fio_env() as env:
                logger.debug("writing %s with GDAL options %s", str(path), env.options)
                path.parent.makedirs(exist_ok=True)
                with fiona.open(str(path), mode=mode, *args, **kwargs) as dst:
                    yield dst
    except Exception as exc:  # pragma: no cover
        logger.exception(exc)
        logger.debug("remove %s ...", str(path))
        path.rm(ignore_errors=True)
        raise


def read_vector_window(
    inp, tile, validity_check=True, clip_to_crs_bounds=False, skip_missing_files=False
):
    """
    Read a window of an input vector dataset.

    Also clips geometry.

    Parameters
    ----------
    inp : string or IndexedFeatures
        path to vector file or an IndexedFeatures instance
    tile : ``Tile``
        tile extent to read data from
    validity_check : bool
        checks if reprojected geometry is valid and throws ``RuntimeError`` if
        invalid (default: True)
    clip_to_crs_bounds : bool
        Always clip geometries to CRS bounds. (default: False)

    Returns
    -------
    features : list
      a list of reprojected GeoJSON-like features
    """

    def _gen_features():
        for path in inp if isinstance(inp, list) else [inp]:
            try:
                yield from _read_vector_window(
                    path,
                    tile,
                    validity_check=validity_check,
                    clip_to_crs_bounds=clip_to_crs_bounds,
                )
            except FileNotFoundError:
                if skip_missing_files:
                    logger.debug("skip missing file %s", path)
                else:
                    raise

    try:
        return list(_gen_features())
    except FileNotFoundError:  # pragma: no cover
        raise
    except Exception as e:  # pragma: no cover
        raise MapcheteIOError(e)


def _read_vector_window(inp, tile, validity_check=True, clip_to_crs_bounds=False):
    try:
        if tile.pixelbuffer and tile.is_on_edge():
            return chain.from_iterable(
                _get_reprojected_features(
                    inp=inp,
                    dst_bounds=bbox.bounds,
                    dst_crs=tile.crs,
                    validity_check=validity_check,
                    clip_to_crs_bounds=clip_to_crs_bounds,
                )
                for bbox in clip_geometry_to_srs_bounds(
                    tile.bbox, tile.tile_pyramid, multipart=True
                )
            )
        else:
            features = _get_reprojected_features(
                inp=inp,
                dst_bounds=tile.bounds,
                dst_crs=tile.crs,
                validity_check=validity_check,
                clip_to_crs_bounds=clip_to_crs_bounds,
            )
            return features
    except FileNotFoundError:  # pragma: no cover
        raise
    except Exception as exc:  # pragma: no cover
        raise IOError(f"failed to read {inp}") from exc


def write_vector_window(
    in_data=None,
    out_driver="GeoJSON",
    out_schema=None,
    out_tile=None,
    out_path=None,
    allow_multipart_geometries=True,
    **kwargs,
):
    """
    Write features to file.

    When the output driver is 'Geobuf', the geobuf library will be used otherwise the
    driver will be passed on to Fiona.

    Parameters
    ----------
    in_data : features
    out_driver : string
    out_schema : dictionary
        output schema for fiona
    out_tile : ``BufferedTile``
        tile used for output extent
    out_path : string
        output path for file
    """
    # Delete existing file.
    out_path = MPath.from_inp(out_path)
    out_path.rm(ignore_errors=True)
    out_features = []
    for feature in in_data:
        try:
            # clip feature geometry to tile bounding box and append for writing
            clipped = clean_geometry_type(
                to_shape(feature["geometry"]).intersection(out_tile.bbox),
                out_schema["geometry"],
            )
            if allow_multipart_geometries:
                cleaned_output_fetures = [clipped]
            else:
                cleaned_output_fetures = multipart_to_singleparts(clipped)
            for out_geom in cleaned_output_fetures:
                if out_geom.is_empty:  # pragma: no cover
                    continue

                out_features.append(
                    {"geometry": mapping(out_geom), "properties": feature["properties"]}
                )
        except Exception as e:
            logger.warning("failed to prepare geometry for writing: %s", e)
            continue

    # write if there are output features
    if out_features:
        try:
            if out_driver.lower() in ["geobuf"]:
                # write data to remote file
                with VectorWindowMemoryFile(
                    tile=out_tile,
                    features=out_features,
                    schema=out_schema,
                    driver=out_driver,
                ) as memfile:
                    logger.debug((out_tile.id, "write tile", out_path))
                    with out_path.open("wb") as dst:
                        dst.write(memfile)
            else:  # pragma: no cover
                with fiona_open(
                    out_path,
                    "w",
                    schema=out_schema,
                    driver=out_driver,
                    crs=out_tile.crs.to_dict(),
                ) as dst:
                    logger.debug((out_tile.id, "write tile", out_path))
                    dst.writerecords(out_features)
        except Exception as e:
            logger.error("error while writing file %s: %s", out_path, e)
            raise

    else:
        logger.debug((out_tile.id, "nothing to write", out_path))


class VectorWindowMemoryFile:
    """Context manager around fiona.io.MemoryFile."""

    def __init__(self, tile=None, features=None, schema=None, driver=None):
        """Prepare data & profile."""
        self.tile = tile
        self.schema = schema
        self.driver = driver
        self.features = features

    def __enter__(self):
        """Open MemoryFile, write data and return."""
        if self.driver.lower() == "geobuf":
            import geobuf

            return geobuf.encode(
                dict(
                    type="FeatureCollection",
                    features=[dict(f, type="Feature") for f in self.features],
                )
            )
        else:  # pragma: no cover
            # this part is excluded now for tests as we try to let fiona write directly
            # to S3
            self.fio_memfile = MemoryFile()
            with self.fio_memfile.open(
                schema=self.schema, driver=self.driver, crs=self.tile.crs
            ) as dst:
                dst.writerecords(self.features)
            return self.fio_memfile.getbuffer()

    def __exit__(self, *args):
        """Make sure MemoryFile is closed."""
        try:
            self.fio_memfile.close()
        except AttributeError:
            pass


@retry(
    logger=logger,
    **dict(IORetrySettings()),
)
def _get_reprojected_features(
    inp=None,
    dst_bounds=None,
    dst_crs=None,
    validity_check=False,
    clip_to_crs_bounds=False,
):
    logger.debug("reading %s", inp)
    with ExitStack() as exit_stack:
        if isinstance(inp, (str, MPath)):
            try:
                src = exit_stack.enter_context(fiona_open(inp, "r"))
                src_crs = CRS(src.crs)
            except Exception as e:
                # fiona errors which indicate file does not exist
                for i in (
                    "does not exist in the file system",
                    "No such file or directory",
                    "The specified key does not exist",
                ):
                    if i in str(e):
                        raise FileNotFoundError(
                            "%s not found and cannot be opened with Fiona" % inp
                        )
                else:
                    try:
                        # NOTE: this can cause addional S3 requests
                        exists = path_exists(inp)
                    except Exception:  # pragma: no cover
                        # in order not to mask the original fiona exception, raise it
                        raise e
                    if exists:
                        # raise fiona exception
                        raise e
                    else:  # pragma: no cover
                        # file does not exist
                        raise FileNotFoundError(
                            "%s not found and cannot be opened with Fiona" % inp
                        )
        else:
            src = inp
            src_crs = inp.crs
        # reproject tile bounding box to source file CRS for filter
        if src_crs == dst_crs:
            dst_bbox = box(*dst_bounds)
        else:
            dst_bbox = reproject_geometry(
                box(*dst_bounds),
                src_crs=dst_crs,
                dst_crs=src_crs,
                validity_check=True,
            )
        for feature in src.filter(bbox=dst_bbox.bounds):
            try:
                # check validity
                original_geom = _repair(to_shape(feature["geometry"]))

                # clip with bounds and omit if clipped geometry is empty
                clipped_geom = original_geom.intersection(dst_bbox)

                # reproject each feature to tile CRS
                g = reproject_geometry(
                    clean_geometry_type(
                        clipped_geom,
                        original_geom.geom_type,
                        raise_exception=False,
                    ),
                    src_crs=src_crs,
                    dst_crs=dst_crs,
                    validity_check=validity_check,
                    clip_to_crs_bounds=False,
                )
                if not g.is_empty:
                    yield {
                        "properties": feature["properties"],
                        "geometry": mapping(g),
                    }
            # this can be handled quietly
            except TopologicalError as e:  # pragma: no cover
                logger.warning("feature omitted: %s", e)


def bounds_intersect(bounds1, bounds2):
    return Bounds.from_inp(bounds1).intersects(bounds2)


class FakeIndex:
    """Provides a fake spatial index in case rtree is not installed."""

    def __init__(self):
        self._items = []

    def insert(self, id, bounds):
        self._items.append((id, bounds))

    def intersection(self, bounds):
        return [
            id for id, i_bounds in self._items if bounds_intersect(i_bounds, bounds)
        ]


class IndexedFeatures:
    """
    Behaves like a mapping of GeoJSON-like objects but has a filter() method.

    Parameters
    ----------
    features : iterable
        Features to be indexed
    index : string
        Spatial index to use. Can either be "rtree" (if installed) or None.
    """

    def __init__(self, features, index="rtree", allow_non_geo_objects=False, crs=None):
        if index == "rtree":
            try:
                from rtree import index

                self._index = index.Index()
            except ImportError:  # pragma: no cover
                warnings.warn(
                    "It is recommended to install rtree in order to significantly speed up spatial indexes."
                )
                self._index = FakeIndex()
        else:
            self._index = FakeIndex()

        self.crs = features.crs if hasattr(features, "crs") else crs
        self._items = {}
        self._non_geo_items = set()
        self.bounds = (None, None, None, None)
        for feature in features:
            if isinstance(feature, tuple):
                id_, feature = feature
            else:
                id_ = self._get_feature_id(feature)
            self._items[id_] = feature
            try:
                try:
                    bounds = object_bounds(feature, dst_crs=crs)
                except NoCRSError as exc:
                    logger.warning(str(exc))
                    bounds = object_bounds(feature)
            except NoGeoError:
                if allow_non_geo_objects:
                    bounds = None
                else:
                    raise
            if bounds is None:
                self._non_geo_items.add(id_)
            else:
                self._update_bounds(bounds)
                self._index.insert(id_, bounds)

    def __repr__(self):  # pragma: no cover
        return f"IndexedFeatures(features={len(self)}, index={self._index.__repr__()}, bounds={self.bounds})"

    def __len__(self):
        return len(self._items)

    def __str__(self):  # pragma: no cover
        return "IndexedFeatures([%s])" % (", ".join([str(f) for f in self]))

    def __getitem__(self, key):
        try:
            return self._items[hash(key)]
        except KeyError:
            raise KeyError(f"no feature with id {key} exists")

    def __iter__(self):
        return iter(self._items.values())

    def items(self):
        return self._items.items()

    def keys(self):
        return self._items.keys()

    def values(self):
        return self._items.values()

    def filter(self, bounds=None, bbox=None):
        """
        Return features intersecting with bounds.

        Parameters
        ----------
        bounds : list or tuple
            Bounding coordinates (left, bottom, right, top).

        Returns
        -------
        features : list
            List of features.
        """
        bounds = bounds or bbox
        return [
            self._items[id_]
            for id_ in chain(self._index.intersection(bounds), self._non_geo_items)
        ]

    def _update_bounds(self, bounds):
        left, bottom, right, top = self.bounds
        self.bounds = (
            bounds.left if left is None else min(left, bounds.left),
            bounds.bottom if bottom is None else min(bottom, bounds.bottom),
            bounds.right if right is None else max(right, bounds.right),
            bounds.top if top is None else max(top, bounds.top),
        )

    def _get_feature_id(self, feature):
        if hasattr(feature, "id"):
            return hash(feature.id)
        elif isinstance(feature, dict) and "id" in feature:
            return hash(feature["id"])
        else:
            try:
                return hash(feature)
            except TypeError:
                raise TypeError("features need to have an id or have to be hashable")


def object_geometry(obj) -> base.BaseGeometry:
    """
    Determine geometry from object if available.
    """
    try:
        if hasattr(obj, "__geo_interface__"):
            return to_shape(obj)
        elif hasattr(obj, "geometry"):
            return to_shape(obj.geometry)
        elif hasattr(obj, "get") and obj.get("geometry"):
            return to_shape(obj["geometry"])
        else:
            raise TypeError("no geometry")
    except Exception as exc:
        logger.exception(exc)
        raise NoGeoError(f"cannot determine geometry from object: {obj}") from exc


def object_bounds(obj: Any, dst_crs: Union[CRS, None] = None) -> Bounds:
    """
    Determine geographic bounds from object if available.

    If dst_crs is defined, bounds will be reprojected in case the object holds CRS information.
    """
    try:
        if hasattr(obj, "bounds"):
            bounds = validate_bounds(obj.bounds)
        elif hasattr(obj, "bbox"):
            bounds = validate_bounds(obj.bbox)
        elif hasattr(obj, "get") and obj.get("bounds"):
            bounds = validate_bounds(obj["bounds"])
        else:
            bounds = validate_bounds(object_geometry(obj).bounds)
    except Exception as exc:
        logger.exception(exc)
        raise NoGeoError(f"cannot determine bounds from object: {obj}") from exc

    if dst_crs:
        return Bounds.from_inp(
            reproject_geometry(shape(bounds), src_crs=object_crs(obj), dst_crs=dst_crs)
        )

    return bounds


def object_crs(obj: Any) -> CRS:
    """Determine CRS from an object."""
    try:
        if hasattr(obj, "crs"):
            return CRS.from_user_input(obj.crs)
        elif hasattr(obj, "get") and obj.get("crs"):
            return CRS.from_user_input(obj["crs"])
        else:
            raise AttributeError(f"no crs attribute or key found in object: {obj}")
    except Exception as exc:
        logger.exception(exc)
        raise NoCRSError(f"cannot determine CRS from object: {obj}") from exc


def convert_vector(inp, out, overwrite=False, exists_ok=True, **kwargs):
    """
    Convert vector file to a differernt format.

    When kwargs are given, the operation will be conducted by Fiona, without kwargs,
    the file is simply copied to the destination using fsspec.

    Parameters
    ----------
    inp : str
        Path to input file.
    out : str
        Path to output file.
    overwrite : bool
        Overwrite output file. (default: False)
    skip_exists : bool
        Skip conversion if outpu already exists. (default: True)
    kwargs : mapping
        Creation parameters passed on to output file.
    """
    inp = MPath.from_inp(inp)
    out = MPath.from_inp(out)
    if out.exists():
        if not exists_ok:
            raise IOError(f"{out} already exists")
        elif not overwrite:
            logger.debug("output %s already exists and will not be overwritten")
            return
        else:
            fs_from_path(out).rm(out)
    kwargs = kwargs or {}
    if kwargs:
        logger.debug("convert vector file %s to %s using %s", str(inp), out, kwargs)
        with fiona_open(inp, "r") as src:
            with fiona_open(out, mode="w", **{**src.meta, **kwargs}) as dst:
                dst.writerecords(src)
    else:
        logger.debug("copy %s to %s", str(inp), str(out))
        out.parent.makedirs()
        copy(inp, out, overwrite=overwrite)


def read_vector(inp, index="rtree"):
    with fiona_open(inp, "r") as src:
        return IndexedFeatures(src, index=index)


class FionaRemoteMemoryWriter:
    def __init__(self, path, *args, **kwargs):
        logger.debug("open FionaRemoteMemoryWriter for path %s", path)
        self.path = path
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
                logger.debug("upload fiona MemoryFile to %s", self.path)
                with self.path.open("wb") as dst:
                    dst.write(self._dst.getbuffer())
        finally:
            logger.debug("close fiona MemoryFile")
            self._dst.close()


class FionaRemoteTempFileWriter:
    def __init__(self, path, *args, **kwargs):
        logger.debug("open FionaRemoteTempFileWriter for path %s", path)
        self.path = path
        self._dst = NamedTemporaryFile(suffix=self.path.suffix)
        self._open_args = args
        self._open_kwargs = kwargs
        self._sink = None

    def __enter__(self):
        self._sink = fiona.open(
            self._dst.name, "w", *self._open_args, **self._open_kwargs
        )
        return self._sink

    def __exit__(self, exc_type, exc_value, exc_traceback):
        try:
            self._sink.close()
            if exc_value is None:
                logger.debug("upload TempFile %s to %s", self._dst.name, self.path)
                self.path.fs.put_file(self._dst.name, self.path)
        finally:
            logger.debug("close and remove tempfile")
            self._dst.close()


class FionaRemoteWriter:
    def __new__(self, path, *args, in_memory=True, **kwargs):
        if in_memory:
            return FionaRemoteMemoryWriter(path, *args, **kwargs)
        else:
            return FionaRemoteTempFileWriter(path, *args, **kwargs)
