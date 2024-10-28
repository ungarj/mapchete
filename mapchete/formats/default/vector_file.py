"""
Vector file input which can be read by fiona.

Currently limited by extensions .shp and .geojson but could be extended easily.
"""

import logging
from functools import cached_property
from typing import Optional, List, Tuple, Union

from affine import Affine
import numpy as np
from rasterio.crs import CRS
from rasterio.features import geometry_mask
from shapely import unary_union
from shapely.geometry import Point, box, GeometryCollection, mapping

from mapchete.bounds import Bounds
from mapchete.formats import base
from mapchete.formats.protocols import VectorInput
from mapchete.geometry.filter import is_type
from mapchete.geometry.types import Geometry, GeometryTypeLike
from mapchete.geometry import to_shape, reproject_geometry
from mapchete.io import fiona_open
from mapchete.io.vector import (
    IndexedFeatures,
    convert_vector,
    read_vector,
    read_vector_window,
)
from mapchete.io.vector.indexed_features import object_geometry
from mapchete.path import MPath
from mapchete.tile import BufferedTile
from mapchete.types import CRSLike, ShapeLike

logger = logging.getLogger(__name__)


METADATA = {
    "driver_name": "vector_file",
    "data_type": "vector",
    "mode": "r",
    "file_extensions": ["shp", "geojson", "gpkg"],
}


class InputData(base.InputData):
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
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    srid : string
        spatial reference ID of CRS (e.g. "{'init': 'epsg:4326'}")
    """

    METADATA = {
        "driver_name": "vector_file",
        "data_type": "vector",
        "mode": "r",
        "file_extensions": ["shp", "geojson"],
    }
    _cached_path = None
    _cache_keep = False
    _memory_cache_active = False
    _bbox_cache = None

    def __init__(self, input_params, **kwargs):
        """Initialize."""
        super().__init__(input_params, **kwargs)
        self.path = (
            input_params["abstract"]["path"]
            if "abstract" in input_params
            else input_params["path"]
        )
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
                    convert_vector,
                    key=f"cache_{self.path}",
                    fkwargs=dict(
                        inp=self.path,
                        out=self._cached_path,
                        format=input_params["abstract"]["cache"].get(
                            "format", "FlatGeobuf"
                        ),
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
                    read_vector,
                    key=f"cache_{self.path}",
                    fkwargs=dict(path=self.path, index=None),
                    geometry=self.bbox(),
                )
            else:  # pragma: no cover
                raise ValueError(
                    f"invalid cache configuration given: {input_params['abstract']['cache']}"
                )

    @cached_property
    def in_memory_features(self) -> IndexedFeatures:
        """This property can be accessed once the preprocessing task is finished."""
        return IndexedFeatures(self.get_preprocessing_task_result(f"cache_{self.path}"))

    def open(self, tile: BufferedTile, **kwargs):
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
            tile_features = IndexedFeatures(
                self.in_memory_features.filter(
                    reproject_geometry(
                        tile.bbox, src_crs=tile.crs, dst_crs=self.in_memory_features.crs
                    ).bounds
                ),
                crs=self.in_memory_features.crs,
                index=None,
            )

        else:
            tile_features = None

        return InputTile(
            tile,
            self,
            in_memory_features=tile_features,
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
        if self._bbox_cache is None:
            with fiona_open(self.path) as inp:
                self._bbox_cache = (
                    CRS(inp.crs),
                    tuple(inp.bounds) if len(inp) else None,  # type: ignore
                )
        inp_crs, bounds = self._bbox_cache
        if bounds is None:
            # this creates an empty GeometryCollection object
            return Point()
        # TODO find a way to get a good segmentize value in bbox source CRS
        return reproject_geometry(
            box(*bounds), src_crs=inp_crs, dst_crs=out_crs, clip_to_crs_bounds=False
        )

    def cleanup(self):
        """Cleanup when mapchete closes."""
        if self._cached_path and not self._cache_keep:  # pragma: no cover
            logger.debug("remove cached file %s", self._cached_path)
            self._cached_path.rm(ignore_errors=True)


class InputTile(base.InputTile, VectorInput):
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
    input_data : string
        path to input vector file
    """

    _memory_cache_active = False
    _in_memory_features: Optional[IndexedFeatures] = None
    transform: Affine
    width: int
    height: int
    shape: ShapeLike
    bounds: Bounds
    crs: CRSLike

    def __init__(
        self,
        tile: BufferedTile,
        input_data: InputData,
        cache_task_key: str,
        in_memory_features: Optional[IndexedFeatures] = None,
        **kwargs,
    ):
        """Initialize."""
        super().__init__(tile, input_key=input_data.input_key, **kwargs)
        self._cache = {}
        self.bbox = input_data.bbox(out_crs=self.tile.crs)
        self.cache_task_key = cache_task_key
        if input_data._memory_cache_active:
            self._memory_cache_active = True
            self._in_memory_features = in_memory_features
        else:
            self.path = input_data._cached_path or input_data.path
        self.transform = tile.transform
        self.width = tile.width
        self.height = tile.height
        self.shape = tile.shape
        self.bounds = Bounds.from_inp(tile.bounds)
        self.crs = tile.crs

    def __repr__(self):  # pragma: no cover
        source = (
            repr(self._in_memory_features) if self._memory_cache_active else self.path
        )
        return f"vector_file.InputTile(tile={self.tile.id}, source={source})"

    def read(
        self,
        validity_check: bool = True,
        clip_to_crs_bounds: bool = False,
        target_geometry_type: Optional[
            Union[GeometryTypeLike, Tuple[GeometryTypeLike]]
        ] = None,
        **kwargs,
    ) -> List[dict]:
        """
        Read reprojected & resampled input data.

        Parameters
        ----------
        validity_check : bool
            also run checks if reprojected geometry is valid, otherwise throw
            RuntimeError (default: True)
        clip_to_crs_bounds : bool
            Always clip geometries to CRS bounds. (default: False)

        Returns
        -------
        data : list
        """
        if self._memory_cache_active:
            self._in_memory_features = (
                self._in_memory_features
                or self.preprocessing_tasks_results.get(self.cache_task_key)
            )
            if self._in_memory_features is None:  # pragma: no cover
                raise RuntimeError(
                    "preprocessing tasks have not yet been run "
                    f"(task key {self.cache_task_key} not found in "
                    f"{list(self.preprocessing_tasks_results.keys())})"
                )
        return (
            []
            if self.is_empty()
            else self._read_from_cache(
                validity_check=validity_check,
                clip_to_crs_bounds=clip_to_crs_bounds,
                target_geometry_type=target_geometry_type,
            )
        )

    def is_empty(self) -> bool:
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        if not self.tile.bbox.intersects(self.bbox):
            return True
        return len(self._read_from_cache(True)) == 0

    def read_union_geometry(
        self,
        validity_check: bool = True,
        clip_to_crs_bounds: bool = False,
        pixelbuffer: int = 0,
        **kwargs,
    ) -> Geometry:
        """Read union of reprojected and clipped vector features."""
        features = self.read(
            validity_check=validity_check,
            clip_to_crs_bounds=clip_to_crs_bounds,
            **kwargs,
        )
        if features:
            return unary_union([to_shape(feature) for feature in features]).buffer(
                pixelbuffer * self.transform[0]
            )
        return GeometryCollection()

    def read_as_raster_mask(
        self,
        all_touched: bool = False,
        invert: bool = False,
        validity_check: bool = True,
        clip_to_crs_bounds: bool = False,
        pixelbuffer: int = 0,
        band_count: Optional[int] = None,
        **kwargs,
    ) -> np.ndarray:
        """Read rasterized vector input."""
        if pixelbuffer:
            features = [
                mapping(to_shape(feature).buffer(pixelbuffer * self.transform[0]))
                for feature in self.read(
                    validity_check=validity_check,
                    clip_to_crs_bounds=clip_to_crs_bounds,
                    **kwargs,
                )
            ]
        else:
            features = [
                feature["geometry"]
                for feature in self.read(
                    validity_check=validity_check,
                    clip_to_crs_bounds=clip_to_crs_bounds,
                    **kwargs,
                )
            ]

        if features:
            mask = geometry_mask(
                geometries=features,
                out_shape=self.shape,
                transform=self.transform,
                all_touched=all_touched,
                invert=invert,
            )
            if band_count is not None:
                return np.stack([mask for _ in range(band_count)])
            return mask

        out_shape = self.shape if band_count is None else (band_count, *self.shape)
        if invert:
            return np.ones(shape=out_shape, dtype=bool)
        else:
            return np.zeros(shape=out_shape, dtype=bool)

    def _read_from_cache(
        self,
        validity_check=True,
        clip_to_crs_bounds=False,
        target_geometry_type: Optional[
            Union[GeometryTypeLike, Tuple[GeometryTypeLike]]
        ] = None,
    ):
        checked = "checked" if validity_check else "not_checked"
        if checked not in self._cache:
            self._cache[checked] = list(
                self._in_memory_features.read(self.tile)
                if self._memory_cache_active and self._in_memory_features is not None
                else read_vector_window(
                    self.path,
                    self.tile,
                    validity_check=validity_check,
                    clip_to_crs_bounds=clip_to_crs_bounds,
                )
            )
        if target_geometry_type:  # pragma: no cover
            return [
                feature
                for feature in self._cache[checked]
                if is_type(object_geometry(feature), target_geometry_type)
            ]
        return self._cache[checked]
