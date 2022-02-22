"""
Raster file input which can be read by rasterio.

Currently limited by extensions .tif, .vrt., .png and .jp2 but could be
extended easily.
"""

from cached_property import cached_property
from copy import deepcopy
import logging
import numpy.ma as ma
import os
import rasterio
from rasterio.vrt import WarpedVRT
from shapely.geometry import box
import warnings

from mapchete.formats import base
from mapchete.io.vector import reproject_geometry, segmentize_geometry
from mapchete.io.raster import (
    read_raster_window,
    convert_raster,
    read_raster,
    resample_from_array,
)
from mapchete import io


logger = logging.getLogger(__name__)

METADATA = {
    "driver_name": "raster_file",
    "data_type": "raster",
    "mode": "r",
    "file_extensions": ["tif", "vrt", "png", "jp2"],
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

    METADATA = {
        "driver_name": "raster_file",
        "data_type": "raster",
        "mode": "r",
        "file_extensions": ["tif", "vrt", "png", "jp2"],
    }
    _cached_path = None
    _cache_keep = False
    _memory_cache_active = False

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
                    self._cached_path = io.absolute_path(
                        path=input_params["abstract"]["cache"]["path"],
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

    @cached_property
    def profile(self):
        """Return raster metadata."""
        with rasterio.open(self.path, "r") as src:
            return deepcopy(src.meta)

    def open(self, tile, **kwargs):
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
        return InputTile(
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
        with rasterio.open(self.path) as src:
            if src.transform.is_identity and src.gcps:
                with WarpedVRT(src) as dst:
                    src_bounds = dst.bounds
                    src_crs = src.gcps[1]
                    src_transform = dst.transform
            else:
                src_crs = src.crs
                src_bounds = src.bounds
                src_transform = src.transform
            out_bbox = bbox = box(*src_bounds)
        # If soucre and target CRSes differ, segmentize and reproject
        if src_crs != out_crs:
            # estimate segmentize value (raster pixel size * tile size)
            # and get reprojected bounding box
            return reproject_geometry(
                segmentize_geometry(bbox, src_transform[0] * self.pyramid.tile_size),
                src_crs=src_crs,
                dst_crs=out_crs,
            )
        else:
            return out_bbox

    def exists(self):
        """
        Check if data or file even exists.

        Returns
        -------
        file exists : bool
        """
        return os.path.isfile(self.path)  # pragma: no cover

    def cleanup(self):
        """Cleanup when mapchete closes."""
        if self._cached_path and not self._cache_keep:
            logger.debug("remove cached file %s", self._cached_path)
            try:
                io.fs_from_path(self._cached_path).rm(self._cached_path)
            except FileNotFoundError:
                pass


class InputTile(base.InputTile):
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

    _memory_cache_active = False
    _in_memory_raster = None

    def __init__(
        self, tile, input_data, in_memory_raster=None, cache_task_key=None, **kwargs
    ):
        """Initialize."""
        self.tile = tile
        self.bbox = input_data.bbox(out_crs=self.tile.crs)
        self.profile = input_data.profile
        self.cache_task_key = cache_task_key
        self.input_key = input_data.input_key
        if input_data._memory_cache_active:
            self._memory_cache_active = True
            self._in_memory_raster = in_memory_raster
        else:
            self.path = input_data._cached_path or input_data.path
            if io.path_is_remote(input_data.path):
                file_ext = os.path.splitext(self.path)[1]
                self.gdal_opts = {
                    "GDAL_DISABLE_READDIR_ON_OPEN": True,
                    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": "%s,.ovr" % file_ext,
                }
            else:
                self.gdal_opts = {}

    def __repr__(self):  # pragma: no cover
        source = (
            repr(self._in_memory_raster) if self._memory_cache_active else self.path
        )
        return f"raster_file.InputTile(tile={self.tile.id}, source={source})"

    def read(self, indexes=None, resampling="nearest", **kwargs):
        """
        Read reprojected & resampled input data.

        Returns
        -------
        data : array
        """
        if self._memory_cache_active:
            self._in_memory_raster = (
                self._in_memory_raster
                or self.preprocessing_tasks_results.get(self.cache_task_key)
            )
            if self._in_memory_raster is None:  # pragma: no cover
                raise RuntimeError("preprocessing tasks have not yet been run")
            return resample_from_array(
                in_raster=ma.stack(
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

    def is_empty(self, indexes=None):
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
            if isinstance(indexes, list):
                return indexes
            else:
                return [indexes]
        else:
            return range(1, self.profile["count"] + 1)


def get_segmentize_value(input_file=None, tile_pyramid=None):
    """
    Return the recommended segmentation value in input file units.

    It is calculated by multiplyling raster pixel size with tile shape in
    pixels.

    Parameters
    ----------
    input_file : str
        location of a file readable by rasterio
    tile_pyramied : ``TilePyramid`` or ``BufferedTilePyramid``
        tile pyramid to estimate target tile size

    Returns
    -------
    segmenize value : float
        length suggested of line segmentation to reproject file bounds
    """
    warnings.warn(
        DeprecationWarning("get_segmentize_value() has moved to mapchete.io")
    )  # pragma: no cover
    return io.get_segmentize_value(input_file, tile_pyramid)  # pragma: no cover
