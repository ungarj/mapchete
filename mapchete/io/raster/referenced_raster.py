from __future__ import annotations

import logging
from typing import List, Optional, Union

import numpy as np
import numpy.ma as ma
from affine import Affine
from rasterio.transform import array_bounds
from retry import retry
from shapely.geometry import mapping, shape

from mapchete.bounds import Bounds
from mapchete.grid import Grid
from mapchete.io.raster.array import resample_from_array
from mapchete.io.raster.open import rasterio_open
from mapchete.io.raster.read import read_raster_window
from mapchete.path import MPath
from mapchete.protocols import GridProtocol
from mapchete.settings import IORetrySettings
from mapchete.types import BoundsLike, CRSLike, MPathLike, NodataVal

logger = logging.getLogger(__name__)


class ReferencedRaster:
    """
    A loose in-memory representation of a rasterio dataset.

    Useful to ship cached raster files between dask worker nodes.
    """

    data: Union[np.ndarray, ma.masked_array]
    array: Union[np.ndarray, ma.masked_array]
    transform: Affine
    bounds: Bounds
    crs: CRSLike
    nodata: Optional[NodataVal] = None
    driver: Optional[str] = None

    def __init__(
        self,
        data: Union[np.ndarray, ma.masked_array],
        transform: Affine,
        crs: CRSLike,
        bounds: Optional[BoundsLike] = None,
        nodata: Optional[NodataVal] = None,
        driver: Optional[str] = "COG",
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
        self.data = self.array = data
        self.driver = driver
        self.dtype = self.data.dtype
        self.nodata = nodata
        self.crs = crs
        self.transform = self.affine = transform
        self.bounds = Bounds.from_inp(
            bounds or array_bounds(self.height, self.width, self.transform)
        )
        self.__geo_interface__ = mapping(shape(self.bounds))

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
        indexes: Optional[Union[int, List[int]]] = None,
        grid: Optional[Union[Grid, GridProtocol]] = None,
        resampling: str = "nearest",
    ) -> np.ndarray:
        """Either read full array or resampled to grid."""
        # select bands using band indexes
        if indexes is None or self.data.ndim == 2:
            band_selection = self.data
        else:
            band_selection = self._stack(
                [self.data[i - 1] for i in self.get_band_indexes(indexes)]
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
                out_grid=Grid.from_obj(grid),
                resampling=resampling,
            )

    def get_band_indexes(
        self, indexes: Optional[Union[List[int], int]] = None
    ) -> List[int]:
        """Return valid band indexes."""
        if isinstance(indexes, int):
            return [indexes]
        elif isinstance(indexes, list):
            return indexes
        else:
            return list(range(1, self.count + 1))

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
        indexes: Optional[Union[int, List[int]]] = None,
        grid: Optional[Union[Grid, GridProtocol]] = None,
        resampling: str = "nearest",
        **kwargs,
    ) -> MPath:
        """Write raster to output."""
        grid = Grid.from_obj(grid) if grid else None
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
    def from_rasterio(
        src,
        masked: bool = True,
    ) -> ReferencedRaster:
        return ReferencedRaster(
            data=src.read(masked=masked).copy(),
            transform=src.transform,
            bounds=src.bounds,
            crs=src.crs,
        )

    @staticmethod
    def from_file(
        path: MPathLike,
        grid: Optional[Union[Grid, GridProtocol]] = None,
        masked: bool = True,
        **kwargs,
    ) -> ReferencedRaster:
        path = MPath.from_inp(path)

        logger.debug(f"reading {str(path)} into memory")
        if grid:
            grid = Grid.from_obj(grid)
            data = read_raster_window(path, grid=grid, **kwargs)
            return ReferencedRaster(
                data=data if masked else data.filled(),
                transform=grid.transform,
                bounds=grid.bounds,
                crs=grid.crs,
            )

        @retry(logger=logger, **dict(IORetrySettings()))
        def _read_raster():
            with rasterio_open(path, "r") as src:
                return ReferencedRaster.from_rasterio(
                    src,
                    masked=masked,
                )

        return _read_raster()

    @staticmethod
    def from_array_like(
        array_like: Union[np.ndarray, ma.MaskedArray, GridProtocol, ReferencedRaster],
        transform: Optional[Affine] = None,
        crs: Optional[CRSLike] = None,
    ) -> ReferencedRaster:
        if isinstance(array_like, ReferencedRaster):
            return array_like
        elif isinstance(array_like, np.ndarray):
            if transform is None or crs is None:
                raise ValueError("array transform and CRS must be provided")
            return ReferencedRaster(data=array_like, transform=transform, crs=crs)
        raise TypeError(f"cannot convert {array_like} to ReferencedRaster")


def read_raster(
    inp: MPathLike,
    grid: Optional[Union[Grid, GridProtocol]] = None,
    masked: bool = True,
    **kwargs,
) -> ReferencedRaster:
    return ReferencedRaster.from_file(inp, grid=grid, masked=masked, **kwargs)
