"""Functions handling vector data."""

from contextlib import ExitStack
import os
import logging
import fiona
from fiona.errors import DriverError, FionaError, FionaValueError
from fiona.io import MemoryFile
import json
from retry import retry
from rasterio.crs import CRS
from shapely.geometry import box, mapping, shape
from shapely.errors import TopologicalError
from tilematrix import clip_geometry_to_srs_bounds
from itertools import chain
import warnings

from mapchete.errors import GeometryTypeError, MapcheteIOError
from mapchete.io._misc import MAPCHETE_IO_RETRY_SETTINGS
from mapchete.io._path import fs_from_path, path_exists, makedirs, copy
from mapchete.io._geometry_operations import (
    reproject_geometry,
    segmentize_geometry,
    to_shape,
    multipart_to_singleparts,
    clean_geometry_type,
    _repair,
)
from mapchete.validate import validate_bounds

__all__ = [
    "reproject_geometry",
    "segmentize_geometry",
    "to_shape",
    "multipart_to_singleparts",
    "clean_geometry_type",
]

logger = logging.getLogger(__name__)


def read_vector_window(inp, tile, validity_check=True, clip_to_crs_bounds=False):
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
    try:
        return [
            feature
            for feature in chain.from_iterable(
                [
                    _read_vector_window(
                        path,
                        tile,
                        validity_check=validity_check,
                        clip_to_crs_bounds=clip_to_crs_bounds,
                    )
                    for path in (inp if isinstance(inp, list) else [inp])
                ]
            )
        ]
    except FileNotFoundError:  # pragma: no cover
        raise
    except Exception as e:  # pragma: no cover
        raise MapcheteIOError(e)


def _read_vector_window(inp, tile, validity_check=True, clip_to_crs_bounds=False):
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


def write_vector_window(
    in_data=None,
    out_driver="GeoJSON",
    out_schema=None,
    out_tile=None,
    out_path=None,
    bucket_resource=None,
    allow_multipart_geometries=True,
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
    try:
        os.remove(out_path)
    except OSError:
        pass

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
                    with fs_from_path(out_path).open(out_path, "wb") as dst:
                        dst.write(memfile)
            else:  # pragma: no cover
                # write data to local file
                # this part is not covered by tests as we now try to let fiona directly
                # write to S3
                with fiona.open(
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
    exceptions=(DriverError, FionaError, FionaValueError),
    **MAPCHETE_IO_RETRY_SETTINGS,
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
        if isinstance(inp, str):
            try:
                src = exit_stack.enter_context(fiona.open(inp, "r"))
                src_crs = CRS(src.crs)
            except Exception as e:
                if path_exists(inp):
                    logger.error("error while reading file %s: %s", inp, e)
                    raise e
                else:
                    raise FileNotFoundError("%s not found" % inp)
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
    bounds1 = validate_bounds(bounds1)
    bounds2 = validate_bounds(bounds2)
    horizontal = (
        # partial overlap
        bounds1.left <= bounds2.left <= bounds1.right
        or bounds1.left <= bounds2.right <= bounds1.right
        # bounds 1 within bounds 2
        or bounds2.left <= bounds1.left < bounds1.right <= bounds2.right
        # bounds 2 within bounds 1
        or bounds1.left <= bounds2.left < bounds2.right <= bounds1.right
    )
    vertical = (
        # partial overlap
        bounds1.bottom <= bounds2.bottom <= bounds1.top
        or bounds1.bottom <= bounds2.top <= bounds1.top
        # bounds 1 within bounds 2
        or bounds2.bottom <= bounds1.bottom < bounds1.top <= bounds2.top
        # bounds 2 within bounds 1
        or bounds1.bottom <= bounds2.bottom < bounds2.top <= bounds1.top
    )
    return horizontal and vertical


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
            bounds = self._get_feature_bounds(
                feature, allow_non_geo_objects=allow_non_geo_objects
            )
            if bounds is None:
                self._non_geo_items.add(id_)
            else:
                self._update_bounds(bounds)
                self._index.insert(id_, bounds)

    def __repr__(self):  # pragma: no cover
        return f"IndexedFeatures(features={len(self)}, index={self._index.__repr__()})"

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

    def _get_feature_bounds(self, feature, allow_non_geo_objects=False):
        try:
            if hasattr(feature, "bounds"):
                return validate_bounds(feature.bounds)
            elif hasattr(feature, "__geo_interface__"):
                return validate_bounds(shape(feature).bounds)
            elif feature.get("bounds"):
                return validate_bounds(feature["bounds"])
            elif feature.get("geometry"):
                return validate_bounds(to_shape(feature["geometry"]).bounds)
            else:
                raise TypeError("no bounds")
        except Exception as exc:
            if allow_non_geo_objects:
                return None
            else:
                logger.exception(exc)
                raise TypeError(f"cannot determine bounds from feature: {feature}")


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
    if path_exists(out):
        if not exists_ok:
            raise IOError(f"{out} already exists")
        elif not overwrite:
            logger.debug("output %s already exists and will not be overwritten")
            return
        else:
            fs_from_path(out).rm(out)
    kwargs = kwargs or {}
    if kwargs:
        logger.debug("convert raster file %s to %s using %s", inp, out, kwargs)
        with fiona.open(inp, "r") as src:
            makedirs(os.path.dirname(out))
            with fiona.open(out, mode="w", **{**src.meta, **kwargs}) as dst:
                dst.writerecords(src)
    else:
        logger.debug("copy %s to %s", inp, out)
        copy(inp, out, overwrite=overwrite)


def read_vector(inp, index="rtree"):
    with fiona.open(inp, "r") as src:
        return IndexedFeatures(src, index=index)
