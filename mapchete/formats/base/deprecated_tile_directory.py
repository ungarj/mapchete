import logging
from typing import List, Optional, Tuple

from rasterio.enums import Resampling
from shapely.geometry import box, shape

from mapchete.formats.base._base import InputTile
from mapchete.formats.base.raster import RasterInputDriver, RasterOutputDriver
from mapchete.formats.tools import write_output_metadata
from mapchete.io import MatchingMethod, tile_to_zoom_level
from mapchete.io.vector import reproject_geometry
from mapchete.path import MPath
from mapchete.tile import BufferedTile, BufferedTilePyramid
from mapchete.types import Bounds, BoundsLike

logger = logging.getLogger(__name__)


DEFAULT_TILE_PATH_SCHEMA = "{zoom}/{row}/{col}.{extension}"


class TileDirectoryInput(InputTile):
    """Reader class for mapchete process."""

    td_pyramid: BufferedTilePyramid
    path: MPath
    file_extension: str
    tile_path_schema: str

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
                self._get_tiles_paths(
                    tile_directory_zoom=tile_directory_zoom,
                    fallback_to_higher_zoom=fallback_to_higher_zoom,
                    matching_method=matching_method,
                    matching_precision=matching_precision,
                    matching_max_zoom=matching_max_zoom,
                )
            )
            == 0
        )  # pragma: no cover

    def get_path(self, tile: BufferedTile) -> MPath:
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
        return self.path / self.tile_path_schema.format(
            zoom=str(tile.zoom),
            row=str(tile.row),
            col=str(tile.col),
            extension=self.file_extension.lstrip("."),
        )

    def _get_tiles_paths(
        self,
        tile_directory_zoom: Optional[int] = None,
        fallback_to_higher_zoom: bool = False,
        matching_method: MatchingMethod = MatchingMethod.gdal,
        matching_precision: int = 8,
        matching_max_zoom: Optional[int] = None,
    ) -> List[Tuple[BufferedTile, MPath]]:
        def _get_tiles_paths(
            pyramid: BufferedTilePyramid,
            bounds: BoundsLike,
            zoom: int,
            exists_check: bool = False,
        ) -> List[Tuple[BufferedTile, MPath]]:
            return [
                (_tile, _path)
                for _tile, _path in [
                    (tt, self.get_path(tt))
                    for tt in pyramid.tiles_from_bounds(bounds, zoom)
                ]
                if not exists_check or (exists_check and _path.exists())
            ]

        # determine tile bounds in TileDirectory CRS
        # NOTE: because fiona/OGR cannot handle geometries crossing the antimeridian,
        # we have to clip the source bounds to the CRS bounds.
        _geom = reproject_geometry(
            self.tile.bbox.intersection(box(*self.tile.buffered_tp.bounds)),
            src_crs=self.tile.tp.crs,
            dst_crs=self.td_pyramid.crs,
        )
        if _geom.is_empty:  # pragma: no cover
            logger.debug(
                "tile %s reprojected to %s is empty",
                self.tile,
                self.td_pyramid,
            )
            return []
        td_bounds = _geom.bounds

        # find target zoom level either automatically or use a given one
        if tile_directory_zoom is None:
            zoom = tile_to_zoom_level(
                self.tile,
                dst_pyramid=self.td_pyramid,
                matching_method=matching_method,
                precision=matching_precision,
            )
            if matching_max_zoom is not None:
                zoom = min([zoom, matching_max_zoom])
        else:
            zoom = tile_directory_zoom

        # see if files are available and optionally look at the next zoom levels to get data
        if fallback_to_higher_zoom:
            tiles_paths = []
            # check if tiles exist otherwise try higher zoom level
            while len(tiles_paths) == 0 and zoom >= 0:
                tiles_paths = _get_tiles_paths(
                    pyramid=self.td_pyramid,
                    bounds=td_bounds,
                    zoom=zoom,
                    exists_check=True,
                )
                logger.debug("%s potential tiles at zoom %s", len(tiles_paths), zoom)
                zoom -= 1
        else:
            tiles_paths = _get_tiles_paths(
                pyramid=self.td_pyramid,
                bounds=td_bounds,
                zoom=zoom,
            )
            logger.debug("%s potential tiles at zoom %s", len(tiles_paths), zoom)
        return tiles_paths


class TileDirectoryInputData(RasterInputDriver):
    path: MPath
    td_pyramid: BufferedTilePyramid
    file_extension: str
    tile_path_schema: str = DEFAULT_TILE_PATH_SCHEMA
    bounds: Bounds

    def __init__(self, params: dict, **kwargs):
        super().__init__(params, **kwargs)
        logger.debug("InputData params: %s", params)
        # # populate internal parameters initially depending on whether this input was
        # # defined as simple or abstract input
        # self._params = params.get("abstract") or dict(path=params["path"])
        # # construct path and append optional filesystem options
        # self.path = MPath.from_inp(self._params).absolute_path(
        #     params.get("conf_dir")
        # )

        # TODO: can this be deleted? self.td_pyramid is created in super class
        # if "abstract" in params:
        #     # define pyramid either by hardcoded given values or by existing metadata.json
        #     if "grid" in self._params:
        #         self.td_pyramid = BufferedTilePyramid(
        #             self._params["grid"],
        #             metatiling=self._params.get("metatiling", 1),
        #             tile_size=self._params.get("tile_size", 256),
        #             pixelbuffer=self._params.get("pixelbuffer", 0),
        #         )
        #     else:
        #         try:
        #             self.td_pyramid = self._tiledir_metadata_json["pyramid"]
        #         except FileNotFoundError:
        #             raise MapcheteConfigError(
        #                 f"Pyramid not defined and cannot find metadata.json in {self.path}"
        #             )
        # elif "path" in params:
        #     # define pyramid
        #     self.td_pyramid = self._tiledir_metadata_json["pyramid"]
        #     self._data_type = driver_metadata(
        #         self._tiledir_metadata_json["driver"]["format"]
        #     )["data_type"]

        # TODO: i don't know what this is
        # try:
        #     self.output_data = load_output_writer(
        #         dict(
        #             self._tiledir_metadata_json["driver"],
        #             metatiling=self.td_pyramid.metatiling,
        #             pixelbuffer=self.td_pyramid.pixelbuffer,
        #             pyramid=self.td_pyramid,
        #             grid=self.td_pyramid.grid,
        #             path=self.path,
        #         ),
        #         readonly=True,
        #     )
        #     self._read_as_tiledir_func = self.output_data._read_as_tiledir
        #     self._params.update(
        #         extension=self.output_data.file_extension.split(".")[-1],
        #         **self._tiledir_metadata_json["driver"],
        #     )
        # except FileNotFoundError:
        #     self.output_data = None
        #     self._read_as_tiledir_func = self._read_as_tiledir_func

        # TODO WTF is this
        # self._params.update(
        #     grid=self.td_pyramid.grid.to_dict(),
        #     metatiling=self.td_pyramid.metatiling,
        #     pixelbuffer=self.td_pyramid.pixelbuffer,
        #     tile_size=self.td_pyramid.tile_size,
        # )
        # # validate parameters
        # validate_values(
        #     self._params,
        #     [("path", (str, MPath)), ("grid", (str, dict)), ("extension", str)],
        # )
        # self._ext = self._params["extension"]

        # # additional params
        # self._metadata = dict(
        #     self.METADATA,
        #     data_type=self.data_type,
        #     file_extensions=[self._ext],
        # )
        # self._min_zoom = self._params.get("min_zoom")
        # self._max_zoom = self._params.get("max_zoom")
        # self._resampling = self._params.get("resampling")
        self.path = params["path"]
        self.file_extension = params["extension"]
        self.tile_path_schema = params.get("tile_path_schema", DEFAULT_TILE_PATH_SCHEMA)
        self.td_pyramid = BufferedTilePyramid(
            grid=params.get("grid"),
            metatiling=params.get("metatiling", 1),
            pixelbuffer=params.get("pixelbuffer", 0),
        )
        self.bounds = Bounds.from_inp(params.get("bounds", self.td_pyramid.bounds))
        self.min_zoom = params.get("min_zoom")
        self.max_zoom = params.get("max_zoom")
        self.resampling = params.get("resampling", Resampling.nearest)

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
        return reproject_geometry(
            shape(self.bounds),
            src_crs=self.td_pyramid.crs,
            dst_crs=self.pyramid.crs if out_crs is None else out_crs,
            segmentize_on_clip=True,
        )


class TileDirectoryOutputReader(RasterOutputDriver):
    tile_path_schema: str = DEFAULT_TILE_PATH_SCHEMA
    td_pyramid: BufferedTilePyramid
    file_extension: str

    def __init__(
        self, params: dict, file_extension: str, readonly: bool = False, **kwargs
    ):
        super().__init__(params, readonly=readonly, **kwargs)
        self.path = params["path"]
        self.tile_path_schema = params.get("tile_path_schema", DEFAULT_TILE_PATH_SCHEMA)
        if not readonly:
            write_output_metadata(
                {k: v for k, v in params.items() if k not in ["stac"]}
            )
        self.td_pyramid = BufferedTilePyramid(
            grid=params.get("grid"),
            metatiling=params.get("metatiling", 1),
            pixelbuffer=params.get("pixelbuffer", 0),
        )
        self.file_extension = file_extension
        self.min_zoom = params.get("min_zoom")
        self.max_zoom = params.get("max_zoom")
        self.resampling = params.get("resampling", Resampling.nearest)

    def get_path(self, tile: BufferedTile) -> MPath:
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
        return self.path / self.tile_path_schema.format(
            zoom=str(tile.zoom),
            row=str(tile.row),
            col=str(tile.col),
            extension=self.file_extension.lstrip("."),
        )

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


class TileDirectoryOutputWriter(TileDirectoryOutputReader):
    pass
