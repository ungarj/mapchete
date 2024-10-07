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
