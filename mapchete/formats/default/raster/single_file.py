"""
Raster file input which can be read by rasterio.

Currently limited by extensions .tif, .vrt., .png and .jp2 but could be
extended easily.
"""
from __future__ import annotations

import logging
from copy import deepcopy
from typing import Optional, Tuple

import numpy.ma as ma
from affine import Affine
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from shapely.geometry import box

from mapchete.formats.base import InputData as BaseInputData
from mapchete.formats.base import RasterInputTile as BaseRasterInputTile
from mapchete.formats.protocols import RasterInput
from mapchete.io.raster import (
    convert_raster,
    rasterio_open,
    read_raster,
    read_raster_window,
    resample_from_array,
)
from mapchete.io.raster.referenced_raster import ReferencedRaster
from mapchete.io.vector import reproject_geometry, segmentize_geometry
from mapchete.path import MPath
from mapchete.tile import BufferedTile
from mapchete.types import BandIndexes, Bounds, ResamplingLike

logger = logging.getLogger(__name__)

METADATA = {
    "driver_name": "raster_file",
    "data_type": "raster",
    "mode": "r",
    "file_extensions": ["tif", "vrt", "png", "jp2"],
}


class InputData(BaseInputData):
    """
    Main input class.

    Parameters
    ----------
    input_params : dictionary
        driver specific parameters

    Attributes
    ----------
    path : string
        path to input file
    profile : dictionary
        rasterio metadata dictionary
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    srid : string
        spatial reference ID of CRS (e.g. "{'init': 'epsg:4326'}")
    """

    METADATA = METADATA
    _cached_path: Optional[MPath] = None
    _cache_keep: bool = False
    _memory_cache_active: bool = False

    def __init__(self, input_params, **kwargs):
        """Initialize."""
        super().__init__(input_params, **kwargs)
        self.path = (
            input_params["abstract"]["path"]
            if "abstract" in input_params
            else input_params["path"]
        )
        with rasterio_open(self.path, "r") as src:
            self.profile = deepcopy(src.meta)
            # determine bounding box
            if src.transform.is_identity:
                if src.gcps[1] is not None:
                    with WarpedVRT(src) as dst:
                        self._src_bounds = dst.bounds
                        self._src_crs = src.gcps[1]
                        self._src_transform = dst.transform
                elif src.rpcs:  # pragma: no cover
                    with WarpedVRT(src) as dst:
                        self._src_bounds = dst.bounds
                        self._src_crs = CRS.from_string("EPSG:4326")
                        self._src_transform = dst.transform
                else:  # pragma: no cover
                    raise TypeError("cannot determine georeference")
            else:
                self._src_crs = src.crs
                self._src_bounds = src.bounds
                self._src_transform = src.transform
            self._src_bbox = box(*self._src_bounds)

        self._cache_task = f"cache_{self.path}"
        if "abstract" in input_params and "cache" in input_params["abstract"]:
            if isinstance(input_params["abstract"]["cache"], dict):
                if "path" in input_params["abstract"]["cache"]:
                    cached_path = MPath.from_inp(input_params["abstract"]["cache"])
                    if cached_path.is_absolute():
                        self._cached_path = cached_path
                    else:  # pragma: no cover
                        self._cached_path = cached_path.absolute_path(
                            base_dir=input_params["conf_dir"],
                        )
                else:  # pragma: no cover
                    raise ValueError("please provide a cache path")
                # add preprocessing task to cache data
                self.add_preprocessing_task(
                    convert_raster,
                    key=self._cache_task,
                    fkwargs=dict(
                        inp=self.path,
                        out=self._cached_path,
                        format=input_params["abstract"]["cache"].get("format", "COG"),
                    ),
                    geometry=self.bbox(),
                )
                self._cache_keep = input_params["abstract"]["cache"].get("keep", False)
            elif (
                isinstance(input_params["abstract"]["cache"], str)
                and input_params["abstract"]["cache"] == "memory"
            ):
                self._memory_cache_active = True
                self.add_preprocessing_task(
                    read_raster,
                    key=self._cache_task,
                    fkwargs=dict(inp=self.path),
                    geometry=self.bbox(),
                )
            else:  # pragma: no cover
                raise ValueError(
                    f"invalid cache configuration given: {input_params['abstract']['cache']}"
                )

    def open(self, tile, **kwargs) -> RasterInputTile:
        """
        Return InputTile object.

        Parameters
        ----------
        tile : ``Tile``

        Returns
        -------
        input tile : ``InputTile``
            tile view of input data
        """
        if self._memory_cache_active and self.preprocessing_task_finished(
            self._cache_task
        ):
            in_memory_raster = self.get_preprocessing_task_result(self._cache_task)
        else:
            in_memory_raster = None
        return RasterInputTile(
            tile,
            self,
            in_memory_raster=in_memory_raster,
            cache_task_key=self._cache_task,
            **kwargs,
        )

    def bbox(self, out_crs=None):
        """
        Return data bounding box.

        Parameters
        ----------
        out_crs : ``rasterio.crs.CRS``
            rasterio CRS object (default: CRS of process pyramid)

        Returns
        -------
        bounding box : geometry
            Shapely geometry object
        """
        out_crs = self.pyramid.crs if out_crs is None else out_crs

        # If soucre and target CRSes differ, segmentize and reproject
        if self._src_crs != out_crs:
            # estimate segmentize value (raster pixel size * tile size)
            # and get reprojected bounding box
            return reproject_geometry(
                segmentize_geometry(
                    self._src_bbox, self._src_transform[0] * self.pyramid.tile_size
                ),
                src_crs=self._src_crs,
                dst_crs=out_crs,
            )
        else:
            return self._src_bbox

    def exists(self):
        """
        Check if data or file even exists.

        Returns
        -------
        file exists : bool
        """
        return self.path.exists()  # pragma: no cover

    def cleanup(self):
        """Cleanup when mapchete closes."""
        if self._cached_path and not self._cache_keep:  # pragma: no cover
            logger.debug("remove cached file %s", self._cached_path)
            self._cached_path.rm(ignore_errors=True)


class RasterInputTile(BaseRasterInputTile, RasterInput):
    """
    Target Tile representation of input data.

    Parameters
    ----------
    tile : ``Tile``
    kwargs : keyword arguments
        driver specific parameters

    Attributes
    ----------
    tile : tile : ``Tile``
    input_data : ``InputData``
        parent InputData object
    resampling : string
        resampling method passed on to rasterio
    """

    width: int
    height: int
    transform: Affine
    bounds: Bounds
    shape: Tuple[int, int]
    crs: CRS
    _memory_cache_active = False
    _in_memory_raster = None

    def __init__(
        self,
        tile: BufferedTile,
        input_data: InputData,
        in_memory_raster: Optional[ReferencedRaster] = None,
        cache_task_key: Optional[str] = None,
        **kwargs,
    ):
        """Initialize."""
        super().__init__(tile, input_key=input_data.input_key, **kwargs)
        self.width = tile.width
        self.height = tile.height
        self.transform = tile.transform
        self.bounds = Bounds.from_inp(tile.bounds)
        self.shape = tile.shape
        self.crs = tile.crs
        self.bbox = input_data.bbox(out_crs=self.tile.crs)
        self.profile = input_data.profile
        self.cache_task_key = cache_task_key
        if input_data._memory_cache_active:
            self._memory_cache_active = True
            self._in_memory_raster = in_memory_raster
        else:
            self.path = input_data._cached_path or input_data.path
            self.gdal_opts = {}

    def __repr__(self):  # pragma: no cover
        source = (
            repr(self._in_memory_raster) if self._memory_cache_active else self.path
        )
        return f"raster_file.InputTile(tile={self.tile.id}, source={source})"

    def read(
        self,
        indexes: Optional[BandIndexes] = None,
        resampling: ResamplingLike = Resampling.nearest,
        **kwargs,
    ) -> ma.MaskedArray:
        """
        Read reprojected & resampled input data.

        Parameters
        ----------
        indexes : list or int
            Either a list of band indexes or a single band index. If only a single
            band index is given, the function returns a 2D array, otherwise a 3D array.
        resampling : str
            Resampling method to be used.

        Returns
        -------
        data : array
        """
        if self._memory_cache_active:
            logger.debug(
                "available preprocessing tasks results: %s",
                self.preprocessing_tasks_results,
            )
            self._in_memory_raster = (
                self._in_memory_raster
                or self.preprocessing_tasks_results.get(self.cache_task_key)
            )
            if self._in_memory_raster is None:  # pragma: no cover
                raise RuntimeError(
                    "preprocessing tasks have not yet been run "
                    f"(task key {self.cache_task_key} not found in "
                    f"{list(self.preprocessing_tasks_results.keys())})"
                )
            return resample_from_array(
                array=ma.stack(
                    [
                        self._in_memory_raster.data[i - 1]
                        for i in self._get_band_indexes(indexes)
                    ]
                ),
                in_affine=self._in_memory_raster.affine,
                in_crs=self._in_memory_raster.crs,
                out_tile=self.tile,
                resampling=resampling,
            )
        else:
            return read_raster_window(
                self.path,
                self.tile,
                indexes=self._get_band_indexes(indexes),
                resampling=resampling,
                gdal_opts=self.gdal_opts,
            )

    def is_empty(self, indexes: Optional[BandIndexes] = None) -> bool:
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        # empty if tile does not intersect with file bounding box
        return not self.tile.bbox.intersects(self.bbox)

    def _get_band_indexes(self, indexes=None):
        """Return valid band indexes."""
        if indexes:
            return indexes
        else:
            return list(range(1, self.profile["count"] + 1))
