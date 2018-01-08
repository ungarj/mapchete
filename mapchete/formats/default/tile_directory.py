"""Use a directory of zoom/row/column tiles as input."""

import os
import six
from shapely.geometry import box

from mapchete.tile import BufferedTilePyramid
from mapchete.config import validate_values
from mapchete.errors import MapcheteConfigError
from mapchete.formats import base
from mapchete.io.vector import reproject_geometry


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
                ("path", six.string_types), ("type", six.string_types),
                ("extension", six.string_types)])
        if not self._params["extension"] in [
            "tif", "vrt", "png", "jpg", "mixed", "jp2", "geojson"
        ]:
            raise MapcheteConfigError(
                "invalid file extension given: %s" % self._params["extension"])
        self.path = self._params["path"]
        if not self.path.startswith("http") and not os.path.exists(self.path):
            raise MapcheteConfigError(
                "path does not exist: %s" % self.path)

        # define pyramid
        self.td_pyramid = BufferedTilePyramid(
            self._params["type"],
            metatiling=self._params.get("metatiling", 1),
            tile_size=self._params.get("tile_size", 256),
            pixelbuffer=self._params.get("pixelbuffer", 0))

        # ADDITIONAL PARAMS
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
        return self.process.config.output.open(tile, self.process, **kwargs)

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
