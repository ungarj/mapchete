"""Use a directory of zoom/row/column tiles as input."""

import logging
import os
from shapely.geometry import box

from mapchete.config import validate_values
from mapchete.errors import MapcheteConfigError
from mapchete.formats import base, load_output_writer, read_output_metadata
from mapchete.io import (path_exists, absolute_path, tile_to_zoom_level)
from mapchete.io.vector import reproject_geometry
from mapchete.tile import BufferedTilePyramid


logger = logging.getLogger(__name__)
METADATA = {
    "driver_name": "TileDirectory",
    "data_type": None,
    "mode": "r",
    "file_extensions": None
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

    def __init__(self, input_params, **kwargs):
        """Initialize."""
        super().__init__(input_params, **kwargs)
        if "abstract" in input_params:
            self._params = input_params["abstract"]
            self.path = absolute_path(
                path=self._params["path"],
                base_dir=input_params["conf_dir"]
            )
            logger.debug("InputData params: %s", input_params)
            # define pyramid
            self.td_pyramid = BufferedTilePyramid(
                self._params["grid"],
                metatiling=self._params.get("metatiling", 1),
                tile_size=self._params.get("tile_size", 256),
                pixelbuffer=self._params.get("pixelbuffer", 0)
            )
            self._read_as_tiledir_func = base._read_as_tiledir

        elif "path" in input_params:
            self.path = absolute_path(
                path=input_params["path"], base_dir=input_params.get("conf_dir")
            )
            try:
                params = read_output_metadata(os.path.join(self.path, "metadata.json"))
            except FileNotFoundError:
                raise MapcheteConfigError(
                    "Cannot find metadata.json in %s" % input_params["path"]
                )
            # define pyramid
            self.td_pyramid = params["pyramid"]
            self.output_data = load_output_writer(
                dict(
                    params["driver"],
                    metatiling=self.td_pyramid.metatiling,
                    pixelbuffer=self.td_pyramid.pixelbuffer,
                    pyramid=self.td_pyramid,
                    grid=self.td_pyramid.grid,
                    path=self.path
                ),
                readonly=True
            )
            self._params = dict(
                path=self.path,
                grid=self.td_pyramid.grid.to_dict(),
                metatiling=self.td_pyramid.metatiling,
                pixelbuffer=self.td_pyramid.pixelbuffer,
                tile_size=self.td_pyramid.tile_size,
                extension=self.output_data.file_extension.split(".")[-1],
                **params["driver"]
            )
            self._read_as_tiledir_func = self.output_data._read_as_tiledir

        # validate parameters
        validate_values(
            self._params,
            [
                ("path", str),
                ("grid", (str, dict)),
                ("extension", str)
            ]
        )
        self._ext = self._params["extension"]

        # additional params
        self._bounds = self._params.get("bounds", self.td_pyramid.bounds)
        self._file_type = (
            "vector" if self._params["extension"] == "geojson" else "raster"
        )
        if self._file_type == "raster":
            self._params["count"] = self._params.get(
                "count", self._params.get("bands", None)
            )
            validate_values(self._params, [("dtype", str), ("count", int)])
            self._profile = {
                "nodata": self._params.get("nodata", 0),
                "dtype": self._params["dtype"],
                "count": self._params["count"]
            }
        else:
            self._profile = None

    def open(
        self,
        tile,
        **kwargs
    ):
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
            file_type=self._file_type,
            basepath=self.path,
            file_extension=self._ext,
            profile=self._profile,
            td_pyramid=self.td_pyramid,
            read_as_tiledir_func=self._read_as_tiledir_func,
            **kwargs
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
            dst_crs=self.pyramid.crs if out_crs is None else out_crs
        )


def _get_tiles_paths(basepath=None, ext=None, pyramid=None, bounds=None, zoom=None):
    return [
        (_tile, _path)
        for _tile, _path in [
            (
                t,
                "%s.%s" % (
                    os.path.join(*([basepath, str(t.zoom), str(t.row), str(t.col)])), ext
                )
            )
            for t in pyramid.tiles_from_bounds(bounds, zoom)
        ]
        if path_exists(_path)
    ]


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
    """

    def __init__(
        self,
        tile,
        file_type=None,
        basepath=None,
        file_extension=None,
        profile=None,
        td_crs=None,
        td_pyramid=None,
        read_as_tiledir_func=None
    ):
        """Initialize."""
        self.tile = tile
        self._file_type = file_type
        self._basepath = basepath
        self._ext = file_extension
        self._profile = profile
        self._td_pyramid = td_pyramid
        self._read_as_tiledir = read_as_tiledir_func

    def read(
        self,
        indexes=None,
        resampling="nearest",
        tile_directory_zoom=None,
        matching_method="gdal",
        matching_max_zoom=None,
        matching_precision=8,
        fallback_to_higher_zoom=False,
        validity_check=False,
        dst_nodata=None,
        gdal_opts=None,
        **kwargs
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
            raster file: a list of band numbers; None will read all.
        dst_nodata : int or float, optional
            raster file: if not set, the nodata value from the source dataset
            will be used
        gdal_opts : dict
            raster file: GDAL options passed on to rasterio.Env()

        Returns
        -------
        data : list for vector files or numpy array for raster files
        """
        return self._read_as_tiledir(
            data_type=self._file_type,
            out_tile=self.tile,
            td_crs=self._td_pyramid.crs,
            tiles_paths=self._get_tiles_paths(
                tile_directory_zoom=tile_directory_zoom,
                fallback_to_higher_zoom=fallback_to_higher_zoom,
                matching_method=matching_method,
                matching_precision=matching_precision,
                matching_max_zoom=matching_max_zoom,
            ),
            profile=self._profile,
            validity_check=validity_check,
            indexes=indexes,
            resampling=resampling,
            dst_nodata=dst_nodata,
            gdal_opts=gdal_opts,
            **{k: v for k, v in kwargs.items() if k != "data_type"}
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
        return len(
            self._get_tiles_paths(
                tile_directory_zoom=tile_directory_zoom,
                fallback_to_higher_zoom=fallback_to_higher_zoom,
                matching_method=matching_method,
                matching_precision=matching_precision,
                matching_max_zoom=matching_max_zoom,
            )
        ) == 0  # pragma: no cover

    def _get_tiles_paths(
        self,
        tile_directory_zoom=None,
        fallback_to_higher_zoom=False,
        matching_method="gdal",
        matching_precision=8,
        matching_max_zoom=None,
    ):
        # determine tile bounds in TileDirectory CRS
        td_bounds = reproject_geometry(
            self.tile.bbox,
            src_crs=self.tile.tp.crs,
            dst_crs=self._td_pyramid.crs
        ).bounds

        # find target zoom level
        if tile_directory_zoom is None:
            zoom = tile_to_zoom_level(
                self.tile,
                dst_pyramid=self._td_pyramid,
                matching_method=matching_method,
                precision=matching_precision
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
                    zoom=zoom
                )
                logger.debug("%s existing tiles found at zoom %s", len(tiles_paths), zoom)
                zoom -= 1
        else:
            tiles_paths = _get_tiles_paths(
                basepath=self._basepath,
                ext=self._ext,
                pyramid=self._td_pyramid,
                bounds=td_bounds,
                zoom=zoom
            )
            logger.debug("%s existing tiles found at zoom %s", len(tiles_paths), zoom)

        return tiles_paths
