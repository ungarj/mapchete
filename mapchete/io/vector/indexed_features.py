from itertools import chain
import logging
from typing import Any, Literal, Optional, Union
import warnings

from rasterio.crs import CRS
from mapchete.errors import NoCRSError, NoGeoError
from mapchete.geometry.reproject import reproject_geometry
from mapchete.geometry.shape import to_shape
from mapchete.io.vector.read import bounds_intersect
from mapchete.geometry.types import Geometry
from mapchete.types import Bounds, CRSLike


logger = logging.getLogger(__name__)


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

    def __init__(
        self,
        features,
        index: Optional[Literal["rtree"]] = "rtree",
        allow_non_geo_objects: bool = False,
        crs: Optional[CRSLike] = None,
    ):
        if index == "rtree":
            try:
                import rtree

                self._index = rtree.index.Index()
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


def object_geometry(obj) -> Geometry:
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


def object_bounds(obj: Any, dst_crs: Union[CRSLike, None] = None) -> Bounds:
    """
    Determine geographic bounds from object if available.

    If dst_crs is defined, bounds will be reprojected in case the object holds CRS information.
    """
    try:
        if hasattr(obj, "bounds"):
            bounds = Bounds.from_inp(obj.bounds)
        elif hasattr(obj, "bbox"):
            bounds = Bounds.from_inp(obj.bbox)
        elif hasattr(obj, "get") and obj.get("bounds"):
            bounds = Bounds.from_inp(obj["bounds"])
        else:
            bounds = Bounds.from_inp(object_geometry(obj).bounds)
    except Exception as exc:
        logger.exception(exc)
        raise NoGeoError(f"cannot determine bounds from object: {obj}") from exc

    if dst_crs:
        return Bounds.from_inp(
            reproject_geometry(
                to_shape(bounds), src_crs=object_crs(obj), dst_crs=dst_crs
            ).bounds
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
