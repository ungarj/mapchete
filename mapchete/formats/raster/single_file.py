"""
Raster file input which can be read by rasterio.

Currently limited by extensions .tif, .vrt., .png and .jp2 but could be
extended easily.
"""
from __future__ import annotations

import logging
import math
import os
from contextlib import ExitStack
from copy import deepcopy
from typing import Optional, Tuple

import numpy as np
import numpy.ma as ma
from affine import Affine
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.rio.overview import get_maximum_overview_level
from rasterio.vrt import WarpedVRT
from rasterio.windows import from_bounds
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry

from mapchete.config.base import _OUTPUT_PARAMETERS, snap_bounds
from mapchete.errors import MapcheteConfigError
from mapchete.formats.base import InputTile
from mapchete.formats.base.raster import RasterInputDriver
from mapchete.io.profiles import DEFAULT_PROFILES
from mapchete.io.raster import (
    convert_raster,
    rasterio_open,
    read_raster,
    read_raster_window,
    resample_from_array,
)
from mapchete.io.raster.array import extract_from_array, prepare_array
from mapchete.io.raster.referenced_raster import ReferencedRaster
from mapchete.io.raster.write import rasterio_write
from mapchete.io.vector import reproject_geometry, segmentize_geometry
from mapchete.path import MPath
from mapchete.protocols import GridProtocol
from mapchete.tile import BufferedTile
from mapchete.types import BandIndexes, Bounds, ResamplingLike
from mapchete.validate import validate_values

logger = logging.getLogger(__name__)

METADATA = {
    "driver_name": "raster_file",
    "data_type": "raster",
    "mode": "r",
    "file_extensions": ["tif", "vrt", "png", "jp2"],
}
IN_MEMORY_THRESHOLD = int(os.environ.get("MP_IN_MEMORY_THRESHOLD", 20000 * 20000))

###############
#    INPUT    #
###############


class RasterFile(GridProtocol, InputTile):
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

    tile: BufferedTile
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
        self.tile = tile
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


class InputData(RasterInputDriver):
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
            self.count = src.count
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

    def open(self, tile, **kwargs) -> RasterFile:
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
        return RasterFile(
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


################
#    OUTPUT    #
################


class RasterSingleFileOutputReader:
    def is_valid_with_config(self, config: dict):
        """
        Check if output format is valid with other process parameters.

        Parameters
        ----------
        config : dictionary
            output configuration parameters

        Returns
        -------
        is_valid : bool
        """
        return validate_values(
            config, [("bands", int), ("path", (str, MPath)), ("dtype", str)]
        )


class RasterSingleFileOutputWriter(
    RasterSingleFileOutputReader,
):
    write_in_parent_process = True
    use_stac = True
    zoom: int
    cog: bool
    driver: str
    in_memory: bool

    def __init__(self, params: dict, driver: str, readonly: bool = False, **kwargs):
        logger.debug("output is single file")

        self.dst = None
        if len(params["delimiters"]["zoom"]) != 1:
            raise ValueError("single file output only works with one zoom level")
        self.driver = driver
        self.zoom = params["delimiters"]["zoom"][0]
        self.cog = params.get("cog", False)
        self.in_memory = params.get("in_memory", True)
        self.output_params = params

    @property
    def stac_asset_type(self):  # pragma: no cover
        """GeoTIFF media type."""
        # TODO: manage for other drivers
        if self.driver in ["COG", "GTiff"]:
            return "image/tiff; application=geotiff"
        else:
            raise ValueError(f"cannot determine media type for driver {self.driver}")

    def prepare(self, process_area: BaseGeometry, **kwargs):
        bounds = (
            snap_bounds(
                bounds=Bounds(
                    *process_area.intersection(
                        box(*self.output_params["delimiters"]["effective_bounds"])
                    ).bounds
                ),
                pyramid=self.pyramid,
                zoom=self.zoom,
            )
            if process_area
            else self.output_params["delimiters"]["effective_bounds"]
        )
        height = math.ceil(
            (bounds.top - bounds.bottom) / self.pyramid.pixel_x_size(self.zoom)
        )
        width = math.ceil(
            (bounds.right - bounds.left) / self.pyramid.pixel_x_size(self.zoom)
        )
        logger.debug("output raster bounds: %s", bounds)
        logger.debug("output raster shape: %s, %s", height, width)
        creation_options = {
            k: v for k, v in self.output_params.items() if k not in _OUTPUT_PARAMETERS
        }
        self._profile = DEFAULT_PROFILES["COG" if self.cog else "GTiff"](
            transform=Affine(
                self.pyramid.pixel_x_size(self.zoom),
                0,
                bounds.left,
                0,
                -self.pyramid.pixel_y_size(self.zoom),
                bounds.top,
            ),
            height=height,
            width=width,
            count=self.count,
            crs=self.pyramid.crs,
            **creation_options,
        )
        if self.cog:
            if self._profile.get("blocksize") is not None:
                self._profile["blocksize"] = int(self._profile.get("blocksize"))
        else:
            for blocksize in ["blockxsize", "blockysize"]:
                if self._profile.get(blocksize) is not None:
                    self._profile[blocksize] = int(self._profile[blocksize])
        logger.debug("single GTiff profile: %s", str(self._profile))
        logger.debug(
            get_maximum_overview_level(
                width,
                height,
                minsize=self._profile.get("blocksize", self._profile.get("blockxsize")),
            )
        )
        if self.cog or "overviews" in self.output_params:
            self.overviews = True
            self.overviews_resampling = self.output_params.get(
                "overviews_resampling", "nearest"
            )
            self.overviews_levels = self.output_params.get(
                "overviews_levels",
                [
                    2**i
                    for i in range(
                        1,
                        get_maximum_overview_level(
                            width,
                            height,
                            minsize=self._profile.get(
                                "blocksize", self._profile.get("blockxsize")
                            ),
                        ),
                    )
                ],
            )
        else:
            self.overviews = False

        self.in_memory = (
            self.in_memory
            if self.in_memory is False
            else height * width < IN_MEMORY_THRESHOLD
        )

        # set up rasterio
        if self.path.exists():
            if self.output_params["mode"] != "overwrite":
                raise MapcheteConfigError(
                    "single GTiff file already exists, use overwrite mode to replace"
                )
            elif not self.path.is_remote():
                logger.debug("remove existing file: %s", self.path)
                self.path.rm()
        # create output directory if necessary
        logger.debug("open output file: %s", self.path)
        self._ctx = ExitStack()
        self.dst = self._ctx.enter_context(
            rasterio_write(self.path, "w+", **self._profile)
        )

    def read(self, output_tile: BufferedTile, **kwargs) -> ma.MaskedArray:
        """
        Read existing process output.

        Parameters
        ----------
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        NumPy array
        """
        return self.dst.read(window=self.dst.window(*output_tile.bounds), masked=True)

    def get_path(self, *_):
        """
        Determine target file path.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        path : string
        """
        return self.path

    def tiles_exist(
        self,
        process_tile: Optional[BufferedTile] = None,
        output_tile: Optional[BufferedTile] = None,
    ) -> bool:
        """
        Check whether output tiles of a tile (either process or output) exists.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        exists : bool
        """
        if process_tile and output_tile:
            raise ValueError("just one of 'process_tile' and 'output_tile' allowed")
        if process_tile:
            return any(
                not self.read(tile).mask.all()
                for tile in self.pyramid.intersecting(process_tile)
            )
        if output_tile:
            return not self.read(output_tile).mask.all()

    def write(self, process_tile: BufferedTile, data: np.ndarray):
        """
        Write data from process tiles into GeoTIFF file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """

        def _window_in_out_file(window, rio_file):
            return all(
                [
                    window.row_off >= 0,
                    window.col_off >= 0,
                    window.row_off + window.height <= rio_file.height,
                    window.col_off + window.width <= rio_file.width,
                ]
            )

        data = prepare_array(
            data,
            masked=True,
            nodata=self.output_params["nodata"],
            dtype=self.profile(process_tile)["dtype"],
        )

        if data.mask.all():
            logger.debug("data empty, nothing to write")
        else:
            # Convert from process_tile to output_tiles and write
            for tile in self.pyramid.intersecting(process_tile):
                # TODO: do we need pixelbuffer?
                # out_tile = BufferedTile(tile, self.pixelbuffer)
                out_tile = tile
                write_window = (
                    from_bounds(
                        *out_tile.bounds,
                        transform=self.dst.transform,
                    )
                    .round_lengths(pixel_precision=0)
                    .round_offsets(pixel_precision=0)
                )
                if _window_in_out_file(write_window, self.dst):
                    logger.debug("write data to window: %s", write_window)
                    self.dst.write(
                        extract_from_array(
                            array=data,
                            in_affine=process_tile.affine,
                            out_tile=out_tile,
                        )
                        if process_tile != out_tile
                        else data,
                        window=write_window,
                    )

    def profile(self, tile=None):
        """
        Create a metadata dictionary for rasterio.

        Returns
        -------
        metadata : dictionary
            output profile dictionary used for rasterio.
        """
        return self._profile

    def close(self, exc_type=None, exc_value=None, exc_traceback=None):
        """Build overviews and write file."""
        try:
            # only in case no Exception was raised
            if exc_type is None:
                # build overviews
                if self.overviews and self.dst is not None:
                    logger.debug(
                        "build overviews using %s resampling and levels %s",
                        self.overviews_resampling,
                        self.overviews_levels,
                    )
                    self.dst.build_overviews(
                        self.overviews_levels, Resampling[self.overviews_resampling]
                    )
                    self.dst.update_tags(
                        OVR_RESAMPLING_ALG=Resampling[
                            self.overviews_resampling
                        ].name.upper()
                    )
        finally:
            self._ctx.__exit__(exc_type, exc_value, exc_traceback)
