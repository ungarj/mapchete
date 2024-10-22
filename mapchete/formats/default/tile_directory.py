"""Use a directory of zoom/row/column tiles as input."""

import logging
from functools import cached_property

from shapely.geometry import box

from mapchete.errors import MapcheteConfigError
from mapchete.formats import (
    base,
    data_type_from_extension,
    driver_metadata,
    load_output_writer,
    read_output_metadata,
)
from mapchete.formats.protocols import RasterInput
from mapchete.io import MPath, tile_to_zoom_level
from mapchete.geometry import reproject_geometry
from mapchete.tile import BufferedTilePyramid
from mapchete.validate import validate_values

logger = logging.getLogger(__name__)
METADATA = {
    "driver_name": "TileDirectory",
    "data_type": None,
    "mode": "r",
    "file_extensions": None,
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
        path to Mapchete file
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

    def __init__(self, input_params: dict, **kwargs):
        """Initialize."""
        super().__init__(input_params, **kwargs)
        self._read_as_tiledir_func = None
        logger.debug("InputData params: %s", input_params)
        # populate internal parameters initially depending on whether this input was
        # defined as simple or abstract input
        self._params = input_params.get("abstract") or dict(path=input_params["path"])
        # construct path and append optional filesystem options
        self.path = MPath.from_inp(self._params).absolute_path(
            input_params.get("conf_dir")
        )
        if "abstract" in input_params:
            # define pyramid either by hardcoded given values or by existing metadata.json
            if "grid" in self._params:
                self.td_pyramid = BufferedTilePyramid(
                    self._params["grid"],
                    metatiling=self._params.get("metatiling", 1),
                    tile_size=self._params.get("tile_size", 256),
                    pixelbuffer=self._params.get("pixelbuffer", 0),
                )
            else:
                try:
                    self.td_pyramid = self._tiledir_metadata_json["pyramid"]
                except FileNotFoundError:
                    raise MapcheteConfigError(
                        f"Pyramid not defined and cannot find metadata.json in {self.path}"
                    )
            self._read_as_tiledir_func = base._read_as_tiledir
            if "extension" in self._params:
                self._data_type = data_type_from_extension(self._params["extension"])
            else:
                try:
                    self._data_type = self._tiledir_metadata_json["driver"].get(
                        "data_type",
                        driver_metadata(
                            self._tiledir_metadata_json["driver"]["format"]
                        )["data_type"],
                    )
                except FileNotFoundError:
                    # in case no metadata.json is available, try to guess data type via the
                    # format file extension
                    raise MapcheteConfigError(
                        f"data type not defined and cannot find metadata.json in {self.path}"
                    )

        elif "path" in input_params:
            # define pyramid
            self.td_pyramid = self._tiledir_metadata_json["pyramid"]
            self._data_type = driver_metadata(
                self._tiledir_metadata_json["driver"]["format"]
            )["data_type"]

        try:
            self.output_data = load_output_writer(
                dict(
                    self._tiledir_metadata_json["driver"],
                    metatiling=self.td_pyramid.metatiling,
                    pixelbuffer=self.td_pyramid.pixelbuffer,
                    pyramid=self.td_pyramid,
                    grid=self.td_pyramid.grid,
                    path=self.path,
                ),
                readonly=True,
            )
            self._read_as_tiledir_func = self.output_data._read_as_tiledir
            self._params.update(
                extension=self.output_data.file_extension.split(".")[-1],
                **self._tiledir_metadata_json["driver"],
            )
        except FileNotFoundError:
            self.output_data = None
            self._read_as_tiledir_func = self._read_as_tiledir_func
        self._params.update(
            grid=self.td_pyramid.grid.to_dict(),
            metatiling=self.td_pyramid.metatiling,
            pixelbuffer=self.td_pyramid.pixelbuffer,
            tile_size=self.td_pyramid.tile_size,
        )
        # validate parameters
        validate_values(
            self._params,
            [("path", (str, MPath)), ("grid", (str, dict)), ("extension", str)],
        )
        self._ext = self._params["extension"]

        # additional params
        self._bounds = self._params.get("bounds", self.td_pyramid.bounds)
        self._metadata = dict(
            self.METADATA,
            data_type=self._data_type,
            file_extensions=[self._ext],
        )
        if self._metadata.get("data_type") == "raster":
            self._params["count"] = self._params.get(
                "count", self._params.get("bands", None)
            )
            validate_values(self._params, [("dtype", str), ("count", int)])
            self._profile = {
                "nodata": self._params.get("nodata", 0),
                "dtype": self._params["dtype"],
                "count": self._params["count"],
            }
        else:
            self._profile = None
        self._min_zoom = self._params.get("min_zoom")
        self._max_zoom = self._params.get("max_zoom")
        self._resampling = self._params.get("resampling")

    @cached_property
    def _tiledir_metadata_json(self):
        return read_output_metadata(self.path.joinpath("metadata.json"))

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
        return InputTile(
            tile,
            data_type=self._metadata.get("data_type"),
            basepath=self.path,
            file_extension=self._ext,
            profile=self._profile,
            td_pyramid=self.td_pyramid,
            read_as_tiledir_func=self._read_as_tiledir_func,
            min_zoom=self._min_zoom,
            max_zoom=self._max_zoom,
            resampling=self._resampling,
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
        return reproject_geometry(
            box(*self._bounds),
            src_crs=self.td_pyramid.crs,
            dst_crs=self.pyramid.crs if out_crs is None else out_crs,
            segmentize_on_clip=True,
        )


def _get_tiles_paths(
    basepath=None, ext=None, pyramid=None, bounds=None, zoom=None, exists_check=False
):
    return [
        (_tile, _path)
        for _tile, _path in [
            (t, basepath.joinpath(str(t.zoom), str(t.row), str(t.col)).with_suffix(ext))
            for t in pyramid.tiles_from_bounds(bounds, zoom)
        ]
        if not exists_check or (exists_check and _path.exists())
    ]


class InputTile(base.InputTile, RasterInput):
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
    """

    def __init__(
        self,
        tile,
        data_type=None,
        basepath=None,
        file_extension=None,
        profile=None,
        td_crs=None,
        td_pyramid=None,
        read_as_tiledir_func=None,
        min_zoom=None,
        max_zoom=None,
        resampling=None,
    ):
        """Initialize."""
        self.tile = tile
        self._data_type = data_type
        self._basepath = basepath
        self._ext = file_extension
        self._profile = profile
        self._td_pyramid = td_pyramid
        self._read_as_tiledir = read_as_tiledir_func
        self._min_zoom = min_zoom
        self._max_zoom = max_zoom
        self._resampling = resampling

    def read(
        self,
        indexes=None,
        resampling=None,
        tile_directory_zoom=None,
        matching_method="gdal",
        matching_max_zoom=None,
        matching_precision=8,
        fallback_to_higher_zoom=False,
        validity_check=False,
        dst_nodata=None,
        gdal_opts=None,
        **kwargs,
    ):
        """
        Read reprojected & resampled input data.

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
        resampling : string
            raster file: one of "nearest", "average", "bilinear" or "lanczos"
        validity_check : bool
            vector file: also run checks if reprojected geometry is valid,
            otherwise throw RuntimeError (default: True)
        indexes : list or int
            Either a list of band indexes or a single band index. If only a single
            band index is given, the function returns a 2D array, otherwise a 3D array.
        dst_nodata : int or float, optional
            raster file: if not set, the nodata value from the source dataset
            will be used
        gdal_opts : dict
            raster file: GDAL options passed on to rasterio.Env()

        Returns
        -------
        data : list for vector files or numpy array for raster files
        """
        if resampling:
            _resampling = resampling
        else:
            _resampling = self._resampling or "nearest"
        return self._read_as_tiledir(
            data_type=self._data_type,
            out_tile=self.tile,
            td_crs=self._td_pyramid.crs,
            tiles_paths=self._get_tiles_paths(
                tile_directory_zoom=tile_directory_zoom,
                fallback_to_higher_zoom=fallback_to_higher_zoom,
                matching_method=matching_method,
                matching_precision=matching_precision,
                matching_max_zoom=self._max_zoom
                if matching_max_zoom is None
                else matching_max_zoom,
            ),
            profile=self._profile,
            validity_check=validity_check,
            indexes=indexes,
            resampling=_resampling,
            dst_nodata=dst_nodata,
            gdal_opts=gdal_opts,
            **{k: v for k, v in kwargs.items() if k != "data_type"},
        )

    def is_empty(
        self,
        tile_directory_zoom=None,
        fallback_to_higher_zoom=False,
        matching_method="gdal",
        matching_precision=8,
        matching_max_zoom=None,
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

    def _get_tiles_paths(
        self,
        tile_directory_zoom=None,
        fallback_to_higher_zoom=False,
        matching_method="gdal",
        matching_precision=8,
        matching_max_zoom=None,
    ):
        # determine tile bounds in TileDirectory CRS
        # NOTE: because fiona/OGR cannot handle geometries crossing the antimeridian,
        # we have to clip the source bounds to the CRS bounds.
        _geom = reproject_geometry(
            self.tile.bbox.intersection(box(*self.tile.buffered_tp.bounds)),
            src_crs=self.tile.tp.crs,
            dst_crs=self._td_pyramid.crs,
        )
        if _geom.is_empty:  # pragma: no cover
            logger.debug(
                "tile %s reprojected to %s is empty", self.tile, self._td_pyramid
            )
            return []
        td_bounds = _geom.bounds

        # find target zoom level
        if tile_directory_zoom is None:
            zoom = tile_to_zoom_level(
                self.tile,
                dst_pyramid=self._td_pyramid,
                matching_method=matching_method,
                precision=matching_precision,
            )
            if matching_max_zoom is not None:
                zoom = min([zoom, matching_max_zoom])
        else:
            zoom = tile_directory_zoom
        if fallback_to_higher_zoom:
            tiles_paths = []
            # check if tiles exist otherwise try higher zoom level
            while len(tiles_paths) == 0 and zoom >= 0:
                tiles_paths = _get_tiles_paths(
                    basepath=self._basepath,
                    ext=self._ext,
                    pyramid=self._td_pyramid,
                    bounds=td_bounds,
                    zoom=zoom,
                    exists_check=True,
                )
                logger.debug("%s potential tiles at zoom %s", len(tiles_paths), zoom)
                zoom -= 1
        else:
            tiles_paths = _get_tiles_paths(
                basepath=self._basepath,
                ext=self._ext,
                pyramid=self._td_pyramid,
                bounds=td_bounds,
                zoom=zoom,
            )
            logger.debug("%s potential tiles at zoom %s", len(tiles_paths), zoom)
        return tiles_paths
