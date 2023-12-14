import logging
import warnings
from typing import List, Optional, Tuple, Union

import numpy as np
import numpy.ma as ma
from affine import Affine
from shapely.geometry import box, mapping

from mapchete.io.raster.array import resample_from_array
from mapchete.io.raster.open import rasterio_open
from mapchete.io.raster.read import read_raster_window
from mapchete.path import MPath
from mapchete.protocols import GridProtocol
from mapchete.tile import BufferedTile
from mapchete.types import Bounds, CRSLike, MPathLike, NodataVal

logger = logging.getLogger(__name__)


class ReferencedRaster:
    """
    A loose in-memory representation of a rasterio dataset.

    Useful to ship cached raster files between dask worker nodes.
    """

    data: Union[np.ndarray, ma.masked_array]
    transform: Affine
    bounds: Union[List[float], Tuple[float], Bounds]
    crs: CRSLike
    nodata: Optional[NodataVal] = None
    driver: Optional[str] = None

    def __init__(
        self,
        data: Union[np.ndarray, ma.masked_array],
        transform: Affine,
        bounds: Union[List[float], Tuple[float], Bounds],
        crs: CRSLike,
        nodata: Optional[NodataVal] = None,
        driver: Optional[str] = None,
        **kwargs,
    ):
        if data.ndim == 1:  # pragma: no cover
            raise TypeError("input array must have at least 2 dimensions")
        elif data.ndim == 2:
            self.count = 1
            self.height, self.width = data.shape
        elif data.ndim == 3:
            self.count, self.height, self.width = data.shape
        else:  # pragma: no cover
            # for arrays with more dimensions, the first axis is assumed to be
            # the bands and the last two the width and height
            self.count = data.shape[0]
            self.height, self.width = data.shape[-2:]
        transform = transform or kwargs.get("affine")
        if transform is None:  # pragma: no cover
            raise ValueError("georeference given")
        self.data = data
        self.driver = driver
        self.dtype = self.data.dtype
        self.nodata = nodata
        self.crs = crs
        self.transform = self.affine = transform
        self.bounds = bounds
        self.__geo_interface__ = mapping(box(*self.bounds))

    @property
    def meta(self) -> dict:
        return {
            "driver": self.driver,
            "dtype": self.dtype,
            "nodata": self.nodata,
            "width": self.width,
            "height": self.height,
            "count": self.count,
            "crs": self.crs,
            "transform": self.transform,
        }

    def read(
        self,
        indexes: Union[int, List[int]] = None,
        tile: Optional[BufferedTile] = None,
        grid: Optional[GridProtocol] = None,
        resampling: str = "nearest",
    ) -> np.ndarray:
        """Either read full array or resampled to grid."""
        if tile:  # pragma: no cover
            warnings.warn(
                DeprecationWarning("'tile' is deprecated and should be 'grid'")
            )
            grid = grid or tile
        # select bands using band indexes
        if indexes is None or self.data.ndim == 2:
            band_selection = self.data
        else:
            band_selection = self._stack(
                [self.data[i - 1] for i in self._get_band_indexes(indexes)]
            )

        # return either full array or a window resampled to grid
        if grid is None:
            return band_selection
        else:
            return resample_from_array(
                array=band_selection,
                in_affine=self.transform,
                in_crs=self.crs,
                nodataval=self.nodata,
                nodata=self.nodata,
                out_grid=grid,
                resampling=resampling,
            )

    def _get_band_indexes(self, indexes: Union[List[int], int] = None) -> List[int]:
        """Return valid band indexes."""
        if isinstance(indexes, int):
            return [indexes]
        else:
            return indexes

    def _stack(self, *args) -> np.ndarray:
        """return stack of numpy or numpy.masked depending on array type"""
        return (
            ma.stack(*args)
            if isinstance(self.data, ma.masked_array)
            else np.stack(*args)
        )

    def to_file(
        self,
        path: MPath,
        indexes: Union[int, List[int]] = None,
        tile: Optional[BufferedTile] = None,
        grid: Optional[GridProtocol] = None,
        resampling: str = "nearest",
        **kwargs,
    ) -> MPath:
        """Write raster to output."""
        if tile:  # pragma: no cover
            warnings.warn(
                DeprecationWarning("'tile' is deprecated and should be 'grid'")
            )
            grid = grid or tile
        with rasterio_open(path, "w", **dict(self.meta, **kwargs)) as dst:
            src_array = self.read(indexes=indexes, grid=grid, resampling=resampling)
            if src_array.ndim == 2:
                index = 1
            elif src_array.ndim == 3:
                index = None
            else:  # pragma: no cover
                raise TypeError(
                    "dumping to file is only possible with 2 or 3-dimensional arrays"
                )
            dst.write(src_array, index)
        return path

    @staticmethod
    def from_rasterio(src, masked: bool = True) -> "ReferencedRaster":
        return ReferencedRaster(
            data=src.read(masked=masked).copy(),
            transform=src.transform,
            bounds=src.bounds,
            crs=src.crs,
        )

    @staticmethod
    def from_file(path, masked: bool = True) -> "ReferencedRaster":
        with rasterio_open(path) as src:
            return ReferencedRaster.from_rasterio(src, masked=masked)


def read_raster(
    inp: MPathLike, grid: Optional[GridProtocol] = None, **kwargs
) -> ReferencedRaster:
    if kwargs.get("tile"):  # pragma: no cover
        warnings.warn(DeprecationWarning("'tile' is deprecated and should be 'grid'"))
        grid = grid or kwargs.get("tile")
        kwargs.pop("tile")
    inp = MPath.from_inp(inp)
    logger.debug(f"reading {str(inp)} into memory")
    if grid:
        return ReferencedRaster(
            data=read_raster_window(inp, grid=grid, **kwargs),
            transform=grid.transform,
            bounds=grid.bounds,
            crs=grid.crs,
        )
    with rasterio_open(inp, "r") as src:
        return ReferencedRaster(
            data=src.read(masked=True),
            transform=src.transform,
            bounds=src.bounds,
            crs=src.crs,
        )
