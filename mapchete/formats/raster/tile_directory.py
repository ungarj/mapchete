from __future__ import annotations

import logging
import warnings
from typing import Optional, Tuple

import numpy as np
import numpy.ma as ma
from affine import Affine
from numpy.typing import DTypeLike
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.io import MemoryFile

from mapchete.config.base import _OUTPUT_PARAMETERS
from mapchete.formats.base import InputTile
from mapchete.formats.base.raster import RasterInputDriver, RasterOutputDriver
from mapchete.formats.tile_directory import DEFAULT_TILE_PATH_SCHEMA, TileDirectory
from mapchete.io import MatchingMethod
from mapchete.io.profiles import DEFAULT_PROFILES
from mapchete.io.raster import read_raster_window
from mapchete.io.raster.array import prepare_array
from mapchete.io.raster.read import memory_file, read_raster_no_crs
from mapchete.io.raster.write import write_raster_window
from mapchete.path import MPath
from mapchete.protocols import GridProtocol
from mapchete.tile import BufferedTile, BufferedTilePyramid
from mapchete.types import BandIndexes, Bounds, NodataVal, ResamplingLike
from mapchete.validate import validate_values

logger = logging.getLogger(__name__)


###############
#    INPUT    #
###############


class RasterTileDirectory(GridProtocol, InputTile):
    """Reader class for mapchete process."""

    tile: BufferedTile
    tile_directory: TileDirectory
    resampling: Resampling = Resampling.nearest
    count: int
    dtype: DTypeLike
    nodataval: NodataVal

    # for GridProtocol
    crs: CRS
    width: int
    height: int
    shape: Tuple[int, int]
    transform: Affine
    bounds: Bounds

    def __init__(
        self,
        tile: BufferedTile,
        tile_directory: TileDirectory,
        count: int,
        dtype: DTypeLike,
        nodataval: NodataVal,
        resampling: Resampling = Resampling.nearest,
        **kwargs,
    ):
        self.tile = tile
        self.tile_directory = tile_directory
        self.resampling = resampling
        self.count = count
        self.dtype = dtype
        self.nodataval = nodataval

        # for GridProtocol
        self.crs = self.tile.crs
        self.width = self.tile.width
        self.height = self.tile.height
        self.shape = self.tile.shape
        self.transform = self.tile.transform
        self.bounds = Bounds.from_inp(self.tile.bounds)

    def bands_count(self, indexes: Optional[BandIndexes] = None) -> int:
        if isinstance(indexes, int):
            return indexes
        elif isinstance(indexes, list):
            return len(indexes)
        else:
            return self.count

    def band_indexes(self, indexes: Optional[BandIndexes] = None) -> BandIndexes:
        return indexes or list(range(1, self.count + 1))

    def read(
        self,
        indexes: Optional[BandIndexes] = None,
        resampling: ResamplingLike = Resampling.nearest,
        tile_directory_zoom: Optional[int] = None,
        matching_method: MatchingMethod = MatchingMethod.gdal,
        matching_max_zoom: Optional[int] = None,
        matching_precision: int = 8,
        fallback_to_higher_zoom: bool = False,
        dst_nodata: Optional[NodataVal] = None,
        gdal_opts: Optional[dict] = None,
        **kwargs,
    ) -> ma.MaskedArray:
        logger.debug("reading data from CRS %s to CRS %s", self.crs, self.tile.tp.crs)
        tiles_paths = self.tile_directory.get_tiles_paths(
            self.tile,
            tile_directory_zoom=tile_directory_zoom,
            fallback_to_higher_zoom=fallback_to_higher_zoom,
            matching_method=matching_method,
            matching_precision=matching_precision,
            matching_max_zoom=matching_max_zoom,
        )
        if tiles_paths:
            return read_raster_window(
                [path for _, path in tiles_paths],
                self.tile,
                indexes=self.band_indexes(indexes),
                resampling=resampling,
                src_nodata=self.nodataval,
                dst_nodata=dst_nodata,
                gdal_opts=gdal_opts,
                skip_missing_files=True,
                dst_dtype=self.dtype,
            )
        else:
            return ma.masked_array(
                data=np.full(
                    (self.bands_count(indexes), self.tile.height, self.tile.width),
                    self.nodataval,
                    dtype=self.dtype,
                ),
                mask=True,
            )

    def is_empty(
        self,
        tile_directory_zoom: Optional[int] = None,
        fallback_to_higher_zoom: bool = False,
        matching_method: MatchingMethod = MatchingMethod.gdal,
        matching_precision: int = 8,
        matching_max_zoom: Optional[int] = None,
    ):
        """
        Check if there is data within this tile.

        Parameters
        ----------
        tile_directory_zoom : None
            If set, data will be read from exactly this zoom level
        matching_method : str ('gdal' or 'min') (default: 'gdal')
            gdal: Uses GDAL's standard method. Here, the target resolution is calculated
                by averaging the extent's pixel sizes over both x and y axes. This
                approach returns a zoom level which may not have the best quality but will
                speed up reading significantly.
            min: Returns the zoom level which matches the minimum resolution of the
                extents four corner pixels. This approach returns the zoom level with the
                best possible quality but with low performance. If the tile extent is
                outside of the destination pyramid, a TopologicalError will be raised.
        matching_max_zoom : int (default: None)
            If set, it will prevent reading from zoom levels above the maximum.
        matching_precision : int
            Round resolutions to n digits before comparing.
        fallback_to_higher_zoom : bool (default: False)
            In case no data is found at zoom level, try to read data from higher zoom
            levels. Enabling this setting can lead to many IO requests in areas with no
            data.

        Returns
        -------
        is empty : bool
        """
        return (
            len(
                self.tile_directory.get_tiles_paths(
                    self.tile,
                    tile_directory_zoom=tile_directory_zoom,
                    fallback_to_higher_zoom=fallback_to_higher_zoom,
                    matching_method=matching_method,
                    matching_precision=matching_precision,
                    matching_max_zoom=matching_max_zoom,
                    exists_check=True,
                )
            )
            == 0
        )  # pragma: no cover


class RasterTileDirectoryInputDriver(RasterInputDriver):
    """Read driver class"""

    tile_directory: TileDirectory

    def __init__(self, params: dict, *args, **kwargs):
        super().__init__(params, *args, **kwargs)
        self.pyramid = BufferedTilePyramid(
            grid=params.get("grid"),
            pixelbuffer=params.get("pixelbuffer", 0),
            metatiling=params.get("metatiling", 0),
        )
        self.path = MPath.from_inp(params["path"])
        self.resampling = params.get("resampling", Resampling.nearest)
        self.tile_directory = TileDirectory(
            self.pyramid,
            self.path,
            file_extension=file_extension,
            tile_path_schema=params.get("tile_path_schema", DEFAULT_TILE_PATH_SCHEMA),
            min_zoom=params.get("min_zoom"),
            max_zoom=params.get("max_zoom"),
        )

    def open(self, tile: BufferedTile, **kwargs) -> RasterTileDirectory:
        return RasterTileDirectory(
            tile,
            path=self.path,
            file_extension=self.file_extension,
            tile_path_schema=self.tile_path_schema,
            td_pyramid=self.td_pyramid,
            min_zoom=self.min_zoom,
            max_zoom=self.max_zoom,
            resampling=self.resampling,
            **kwargs,
        )


################
#    OUTPUT    #
################


class RasterTileDirectoryOutputReader:
    pyramid: BufferedTilePyramid
    tile_directory: TileDirectory
    file_extension: str
    path: MPath
    resampling: Resampling = Resampling.nearest
    output_params: dict
    nodataval: NodataVal
    dtype: DTypeLike

    def __init__(self, params, file_extension: str, **kwargs):
        """Initialize."""
        logger.debug("output is tile directory")
        self.pyramid = BufferedTilePyramid(
            grid=params.get("grid"),
            pixelbuffer=params.get("pixelbuffer", 0),
            metatiling=params.get("metatiling", 0),
        )
        self.path = MPath.from_inp(params["path"])
        self.resampling = params.get("resampling", Resampling.nearest)
        self.tile_directory = TileDirectory(
            self.pyramid,
            self.path,
            file_extension=file_extension,
            tile_path_schema=params.get("tile_path_schema", DEFAULT_TILE_PATH_SCHEMA),
            min_zoom=params.get("min_zoom"),
            max_zoom=params.get("max_zoom"),
        )
        self.file_extension = file_extension
        self.output_params = params
        self.nodataval = params.get("nodata", 0)
        self.dtype = params.get("dtype")
        self.count = params.get("bands")

    def open(
        self, tile: BufferedTile, process: "MapcheteProcess", **kwargs
    ) -> RasterTileDirectory:
        # TODO: handle this as input for another process
        return RasterTileDirectory(
            tile,
            tile_directory=self.tile_directory,
            count=self.count,
            dtype=self.dtype,
            nodataval=self.nodataval,
            resampling=self.resampling,
            **kwargs,
        )

    def get_path(self, tile: BufferedTile) -> MPath:
        return self.tile_directory.get_path(tile)

    def tiles_exist(self, process_tile=None, output_tile=None):
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
        if process_tile and output_tile:  # pragma: no cover
            raise ValueError("just one of 'process_tile' and 'output_tile' allowed")
        if process_tile:
            for tile in self.pyramid.intersecting(process_tile):
                if self.get_path(tile).exists():
                    return True
            else:
                return False
        if output_tile:
            return self.get_path(output_tile).exists()

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
        logger.debug("read %s", self.get_path(output_tile))
        try:
            return read_raster_no_crs(self.get_path(output_tile))
        except FileNotFoundError:
            return self.empty(output_tile)

    def empty(self, process_tile: BufferedTile) -> ma.MaskedArray:
        """
        Return empty data.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        empty data : array
            empty array with data type provided in output profile
        """
        profile = self.profile(process_tile)
        return ma.masked_array(
            data=np.full(
                (profile["count"],) + process_tile.shape,
                profile["nodata"],
                dtype=profile["dtype"],
            ),
            mask=True,
            fill_value=profile["nodata"],
        )

    def profile(self, tile: Optional[BufferedTile] = None) -> dict:
        """
        Create a metadata dictionary for rasterio.

        Parameters
        ----------
        tile : ``BufferedTile``

        Returns
        -------
        metadata : dictionary
            output profile dictionary used for rasterio.
        """
        # TODO: make driver specific
        dst_metadata = dict(
            DEFAULT_PROFILES["COG"](),
            count=self.output_params["bands"],
            **{
                k: v
                for k, v in self.output_params.items()
                if k not in _OUTPUT_PARAMETERS
            },
        )
        dst_metadata.pop("transform", None)
        if tile is not None:
            dst_metadata.update(
                crs=tile.crs, width=tile.width, height=tile.height, affine=tile.affine
            )
        else:
            for k in ["crs", "width", "height", "affine"]:
                dst_metadata.pop(k, None)
        try:
            if "compression" in self.output_params:
                warnings.warn(
                    DeprecationWarning("use 'compress' instead of 'compression'")
                )
                dst_metadata.update(compress=self.output_params["compression"])
            else:
                dst_metadata.update(compress=self.output_params["compress"])
            dst_metadata.update(predictor=self.output_params["predictor"])
        except KeyError:
            pass
        return dst_metadata

    def for_web(self, data: np.ndarray) -> Tuple[MemoryFile, str]:
        """
        Convert data to web output (raster only).

        Parameters
        ----------
        data : array

        Returns
        -------
        web data : array
        """
        return (
            memory_file(
                prepare_array(
                    data,
                    masked=True,
                    nodata=self.output_params["nodata"],
                    dtype=self.profile()["dtype"],
                ),
                self.profile(),
            ),
            "image/tiff",
        )


class RasterTileDirectoryOutputWriter(
    RasterTileDirectoryOutputReader, RasterOutputDriver
):
    use_stac = True

    def __init__(
        self, params: dict, file_extension: str, readonly: bool = False, **kwargs
    ):
        super().__init__(
            params, file_extension=file_extension, readonly=readonly, **kwargs
        )

    def write(self, process_tile: BufferedTile, data: np.ndarray):
        """
        Write data from process tiles into GeoTIFF file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        data : ``np.ndarray``
        """
        if isinstance(data, tuple) and len(data) == 2 and isinstance(data[1], dict):
            data, tags = data
        else:
            tags = {}
        data = prepare_array(
            data,
            masked=True,
            nodata=self.nodataval,
            dtype=self.dtype,
        )

        if data.mask.all():
            logger.debug("data empty, nothing to write")
        else:
            # Convert from process_tile to output_tiles and write
            for tile in self.pyramid.intersecting(process_tile):
                out_path = self.get_path(tile)
                # TODO: do we really need the pixelbuffer here?
                # out_tile = BufferedTile(tile, self.pixelbuffer)
                out_tile = tile
                write_raster_window(
                    in_grid=process_tile,
                    in_data=data,
                    out_profile=self.profile(out_tile),
                    out_grid=out_tile,
                    out_path=out_path,
                    tags=tags,
                )

    @property
    def stac_asset_type(self):
        """GeoTIFF media type."""
        return "image/tiff; application=geotiff"
