from typing import Iterator, List, Optional, Protocol, TypedDict

from mapchete.types import GeoJSONLikeFeature
from mapchete.types import BoundsLike, CRSLike

VectorFileSchema = TypedDict("VectorFileSchema", {"geometry": str, "properties": dict})


class FeatureCollectionProtocol(Protocol):
    crs: CRSLike

    def filter(
        self, bounds: Optional[BoundsLike] = None, bbox: Optional[BoundsLike] = None
    ) -> List[GeoJSONLikeFeature]: ...

    def __iter__(self) -> Iterator[GeoJSONLikeFeature]: ...
