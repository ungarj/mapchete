from dataclasses import dataclass
from typing import Protocol

from affine import Affine
from rasterio.crs import CRS


class GeoGridProtocol(Protocol):
    transform: Affine
    crs: CRS
    width: int
    height: int


@dataclass
class GeoGrid:
    transform: Affine
    crs: CRS
    width: int
    height: int



"""
* rasterio Dataset
* BufferedTile
* ReferencedRaster
* Grid
"""