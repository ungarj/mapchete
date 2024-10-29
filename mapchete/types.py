from __future__ import annotations

import os
from typing import (
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    TypedDict,
    Union,
    runtime_checkable,
)

from mapchete._deprecated import deprecated

from fiona.crs import CRS as FionaCRS  # type: ignore
from geojson_pydantic import Feature, FeatureCollection as GeoJSONGeometryType
from pydantic import BaseModel
from rasterio.crs import CRS as RasterioCRS
from rasterio.enums import Resampling
from shapely.geometry import (
    GeometryCollection,
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.geometry.base import BaseGeometry
from tilematrix import Shape, Tile

SinglepartGeometry = Union[
    Point,
    LineString,
    LinearRing,
    Polygon,
]

MultipartGeometry = Union[
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
]

Geometry = Union[SinglepartGeometry, MultipartGeometry, BaseGeometry]


@runtime_checkable
class GeoInterface(Protocol):
    __geo_interface__: Union[
        GeoJSONGeometryType, Dict[Literal["geometry"], GeoJSONGeometryType]
    ]


GeometryLike = Union[Geometry, GeoJSONGeometryType, GeoInterface, Feature, Dict]

CoordArrays = Tuple[Iterable[float], Iterable[float]]


GeoJSONLikeFeature = TypedDict(
    "GeoJSONLikeFeature", {"geometry": dict, "properties": dict}
)
MPathLike = Union[str, os.PathLike]
BoundsLike = Union[List[float], Tuple[float, float, float, float], dict, Polygon]
ShapeLike = Union[Shape, List[int], Tuple[int, int]]
ZoomLevelsLike = Union[List[int], int, dict]
TileLike = Union[Tile, Tuple[int, int, int]]
CRSLike = Union[FionaCRS, RasterioCRS]
NodataVal = Optional[float]
NodataVals = Union[List[NodataVal], NodataVal]
ResamplingLike = Union[Resampling, str]
BandIndex = int
BandIndexes = Union[BandIndex, List[BandIndex]]


def to_resampling(resampling: ResamplingLike) -> Resampling:
    if isinstance(resampling, Resampling):
        return resampling
    return Resampling[resampling]


class Progress(BaseModel):
    current: int = 0
    total: Optional[int] = None


# below are deprecated classes once sitting in this module:
@deprecated("mapchete.types.Bounds has been moved to mapchete.bounds.Bounds")
class Bounds:  # pragma: no cover
    def __new__(cls, *args, **kwargs):
        from mapchete.bounds import Bounds

        # Redirect instantiation to the new class
        return Bounds(*args, **kwargs)

    @staticmethod
    def from_inp(
        inp: BoundsLike, strict: bool = True, crs: Optional[CRSLike] = None
    ):  # pragma: no cover
        from mapchete.bounds import Bounds

        return Bounds.from_inp(inp=inp, strict=strict, crs=crs)

    @staticmethod
    def from_dict(
        inp: dict, strict: bool = True, crs: Optional[CRSLike] = None
    ):  # pragma: no cover
        return Bounds(**inp, strict=strict, crs=crs)


@deprecated("mapchete.types.Grid has been moved to mapchete.grid.Grid")
class Grid:  # pragma: no cover
    def __new__(cls, *args, **kwargs):
        from mapchete.grid import Grid

        return Grid(*args, **kwargs)

    @staticmethod
    def from_obj(obj):  # pragma: no cover
        from mapchete.grid import Grid

        return Grid.from_obj(obj)

    @staticmethod
    def from_bounds(
        bounds: BoundsLike, shape: ShapeLike, crs: CRSLike
    ):  # pragma: no cover
        from mapchete.grid import Grid

        return Grid.from_bounds(bounds=bounds, shape=shape, crs=crs)


@deprecated(
    "mapchete.types.ZoomLevels has been moved to mapchete.zoom_levels.ZoomLevels"
)
class ZoomLevels:  # pragma: no cover
    def __new__(cls, *args, **kwargs):  # pragma: no cover
        from mapchete.zoom_levels import ZoomLevels

        return ZoomLevels(*args, **kwargs)

    @staticmethod
    def from_inp(
        min: ZoomLevelsLike, max: Optional[int] = None, descending: bool = False
    ):  # pragma: no cover
        from mapchete.zoom_levels import ZoomLevels

        return ZoomLevels.from_inp(min=min, max=max, descending=descending)

    @staticmethod
    def from_int(inp: int, **kwargs):  # pragma: no cover
        from mapchete.zoom_levels import ZoomLevels

        return ZoomLevels.from_int(inp=inp, **kwargs)

    @staticmethod
    def from_list(inp: List[int], **kwargs):  # pragma: no cover
        from mapchete.zoom_levels import ZoomLevels

        return ZoomLevels.from_list(inp=inp, **kwargs)

    @staticmethod
    def from_dict(inp: dict, **kwargs):  # pragma: no cover
        from mapchete.zoom_levels import ZoomLevels

        return ZoomLevels.from_dict(inp=inp, **kwargs)
