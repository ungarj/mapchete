"""Use a directory of zoom/row/column tiles as input."""

from itertools import chain
import logging
import numpy as np
import numpy.ma as ma
import os
from rasterio.warp import calculate_default_transform
from shapely.geometry import box, shape, mapping

from mapchete.config import validate_values
from mapchete.errors import MapcheteConfigError
from mapchete.formats import base, load_output_writer
from mapchete.io import path_exists, absolute_path, read_json
from mapchete.io.vector import reproject_geometry, read_vector_window
from mapchete.io.raster import read_raster_window, create_mosaic, resample_from_array
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
        super(InputData, self).__init__(input_params, **kwargs)

        if "abstract" in input_params:
            self._params = input_params["abstract"]
            self.path = absolute_path(
                path=self._params["path"],
                base_dir=input_params["conf_dir"]
            )
            # define pyramid
            self.td_pyramid = BufferedTilePyramid(
                self._params["type"],
                metatiling=self._params.get("metatiling", 1),
                tile_size=self._params.get("tile_size", 256),
                pixelbuffer=self._params.get("pixelbuffer", 0)
            )

        elif "path" in input_params:
            self.path = absolute_path(
                path=input_params["path"], base_dir=input_params.get("conf_dir")
            )
            try:
                params = read_json(os.path.join(self.path, "metadata.json"))
            except FileNotFoundError:
                raise MapcheteConfigError(
                    "Cannot find metadata.json in %s" % input_params["path"]
                )
            # define pyramid
            self.td_pyramid = BufferedTilePyramid(
                params["pyramid"]["grid"]["type"],
                metatiling=params["pyramid"].get("metatiling", 1),
                tile_size=params["pyramid"].get("tile_size", 256),
                pixelbuffer=params["pyramid"].get("pixelbuffer", 0)
            )

            output = load_output_writer(
                dict(
                    params["driver"],
                    metatiling=self.td_pyramid.metatiling,
                    pixelbuffer=self.td_pyramid.pixelbuffer,
                    pyramid=self.td_pyramid,
                    type=self.td_pyramid.type,
                    path=self.path
                ),
                readonly=True
            )
            logger.debug(output)
            self._params = dict(
                path=self.path,
                type=params["pyramid"]["grid"]["type"],
                metatiling=params["pyramid"].get("metatiling", 1),
                pixelbuffer=params["pyramid"].get("pixelbuffer", 0),
                tile_size=params["pyramid"].get("tile_size", 256),
                extension=output.file_extension.split(".")[-1],
                **params["driver"]
            )

        # validate parameters
        validate_values(
            self._params,
            [
                ("path", str),
                ("type", str),
                ("extension", str)
            ]
        )
        if not self._params["extension"] in [
            "tif", "vrt", "png", "jpg", "mixed", "jp2", "geojson"
        ]:
            raise MapcheteConfigError(
                "invalid file extension given: %s" % self._params["extension"]
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
        if self.td_pyramid.crs == tile.tp.crs:
            return InputTile(
                tile,
                tiles_paths=_get_tiles_paths(
                    basepath=self.path,
                    ext=self._ext,
                    pyramid=self.td_pyramid,
                    bounds=tile.bounds,
                    zoom=tile.zoom
                ),
                file_type=self._file_type,
                profile=self._profile,
                td_crs=self.td_pyramid.crs,
                **kwargs
            )
        else:
            # determine tile bounds in TileDirectory CRS
            logger.debug("tile bounds: %s", tile.bounds)
            td_bounds = reproject_geometry(
                tile.bbox, src_crs=tile.tp.crs, dst_crs=self.td_pyramid.crs
            ).bounds
            logger.debug("converted tile bounds in TileDirectory CRS: %s", td_bounds)
            # determine best zoom level to get data from TileDirectory
            transform, width, height = calculate_default_transform(
                tile.tp.crs,
                self.td_pyramid.crs,
                tile.width,
                tile.height,
                *tile.bounds
            )
            # this is the resolution the tile would have in the TileDirectory CRS
            tile_resolution = transform[0]
            logger.debug("target tile is %s", tile)
            logger.debug(
                "we are looking for a zoom level interpolating to %s resolution",
                tile_resolution
            )
            zoom = 0
            while True:
                td_resolution = self.td_pyramid.pixel_x_size(zoom)
                if td_resolution <= tile_resolution:
                    break
                zoom += 1
            else:
                raise RuntimeError("no zoom level could be found")
            logger.debug("target zoom: %s (%s)", zoom, td_resolution)
            # check if tiles exist and pass on to InputTile
            tiles_paths = []
            while len(tiles_paths) == 0 and zoom >= 0:
                tiles_paths = _get_tiles_paths(
                    basepath=self.path,
                    ext=self._ext,
                    pyramid=self.td_pyramid,
                    bounds=td_bounds,
                    zoom=zoom
                )
                logger.debug(
                    "%s existing tiles found for zoom %s", len(tiles_paths), zoom
                )
                zoom -= 1
            return InputTile(
                tile,
                tiles_paths=tiles_paths,
                file_type=self._file_type,
                profile=self._profile,
                td_crs=self.td_pyramid.crs,
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

    def __init__(self, tile, **kwargs):
        """Initialize."""
        self.tile = tile
        self._tiles_paths = kwargs["tiles_paths"]
        self._file_type = kwargs["file_type"]
        self._profile = kwargs["profile"]
        self._td_crs = kwargs["td_crs"]

    def read(
        self,
        validity_check=False,
        indexes=None,
        resampling="nearest",
        dst_nodata=None,
        gdal_opts=None
    ):
        """
        Read reprojected & resampled input data.

        Parameters
        ----------
        validity_check : bool
            vector file: also run checks if reprojected geometry is valid,
            otherwise throw RuntimeError (default: True)

        indexes : list or int
            raster file: a list of band numbers; None will read all.
        resampling : string
            raster file: one of "nearest", "average", "bilinear" or "lanczos"
        dst_nodata : int or float, optional
            raster file: if not set, the nodata value from the source dataset
            will be used
        gdal_opts : dict
            raster file: GDAL options passed on to rasterio.Env()

        Returns
        -------
        data : list for vector files or numpy array for raster files
        """
        logger.debug("reading data from CRS %s to CRS %s", self._td_crs, self.tile.tp.crs)
        if self._file_type == "vector":
            if self.is_empty():
                return []
            else:
                return [
                    {
                        "properties": g["properties"],
                        "geometry": mapping(
                            reproject_geometry(
                                shape(g["geometry"]),
                                src_crs=self._td_crs,
                                dst_crs=self.tile.tp.crs
                            )
                        )
                    }
                    for g in chain.from_iterable([
                        read_vector_window(p, self.tile, validity_check=validity_check)
                        for _, p in self._tiles_paths
                    ])
                ]
        else:
            if self.is_empty():
                bands = len(indexes) if indexes else self._profile["count"]
                return ma.masked_array(
                    data=np.full(
                        (bands, self.tile.height, self.tile.width),
                        self._profile["nodata"],
                        dtype=self._profile["dtype"]
                    ),
                    mask=True
                )
            else:
                return resample_from_array(
                    in_raster=create_mosaic(
                        tiles=[
                            (
                                _tile,
                                read_raster_window(
                                    _path,
                                    _tile,
                                    indexes=indexes,
                                    resampling=resampling,
                                    src_nodata=self._profile["nodata"],
                                    dst_nodata=dst_nodata,
                                    gdal_opts=gdal_opts
                                )
                            )
                            for _tile, _path in self._tiles_paths
                        ],
                        nodata=self._profile["nodata"]
                    ),
                    out_tile=self.tile,
                    resampling=resampling,
                    nodataval=self._profile["nodata"]
                )

    def is_empty(self):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        return len(self._tiles_paths) == 0
