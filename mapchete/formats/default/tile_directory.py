"""Use a directory of zoom/row/column tiles as input."""

from itertools import chain
import os
import six
from shapely.geometry import box

from mapchete.tile import BufferedTilePyramid
from mapchete.config import validate_values
from mapchete.errors import MapcheteConfigError
from mapchete.formats import base
from mapchete.io.vector import reproject_geometry, read_vector_window


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

    METADATA = {
        "driver_name": "TileDirectory",
        "data_type": None,
        "mode": "r",
        "file_extensions": None
    }

    def __init__(self, input_params, **kwargs):
        """Initialize."""
        super(InputData, self).__init__(input_params, **kwargs)
        self._params = input_params["abstract"]

        # validate parameters
        validate_values(
            self._params, [
                ("path", six.string_types),
                ("type", six.string_types),
                ("extension", six.string_types)])
        if not self._params["extension"] in [
            "tif", "vrt", "png", "jpg", "mixed", "jp2", "geojson"
        ]:
            raise MapcheteConfigError(
                "invalid file extension given: %s" % self._params["extension"])
        self._ext = self._params["extension"]
        if self._params["path"].startswith("http"):
            self.path = self._params["path"]
            self._remote = True
        else:
            self.path = os.path.abspath(
                os.path.join(input_params["conf_dir"], self._params["path"]))
            self._remote = False

        # define pyramid
        self.td_pyramid = BufferedTilePyramid(
            self._params["type"],
            metatiling=self._params.get("metatiling", 1),
            tile_size=self._params.get("tile_size", 256),
            pixelbuffer=self._params.get("pixelbuffer", 0))

        # additional params
        self._bounds = self._params.get("bounds", self.td_pyramid.bounds)
        self._file_type = (
            "vector" if self._params["extension"] == "geojson" else "raster")
        if self._file_type == "raster":
            validate_values(self._params, [
                ("dtype", six.string_types), ("nodata", (int, float))])
            self.nodata = self._params("nodata")
            self.dtype = input_params("dtype")
        else:
            self.nodata, self.dtype = None, None

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
        _paths = [
            os.path.join(*([self.path] + map(str, t.id))) + "." + self._ext
            for t in self.td_pyramid.tiles_from_bounds(tile.bounds, tile.zoom)
        ]
        return InputTile(
            tile,
            source_files=_paths if self._remote else [
                _path for _path in _paths if os.path.exists(_path)],
            file_type=self._file_type,
            **kwargs)

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
            dst_crs=self.pyramid.crs if out_crs is None else out_crs)


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
        self._cache = {}
        self._source_files = kwargs["source_files"]
        self._file_type = kwargs["file_type"]

    def read(self, validity_check=False, **kwargs):
        """
        Read reprojected & resampled input data.

        Parameters
        ----------
        validity_check : bool
            vector file: also run checks if reprojected geometry is valid,
            otherwise throw RuntimeError (default: True)

        Returns
        -------
        data : list for vector files or numpy array for raster files
        """
        if self._file_type == "vector":
            return list(chain.from_iterable([
                read_vector_window(
                    _path, self.tile, validity_check=validity_check)
                for _path in self._source_files
            ]))
        else:
            raise NotImplementedError

    def is_empty(self):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        return len(self._source_files) == 0
