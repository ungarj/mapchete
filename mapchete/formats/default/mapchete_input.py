"""Use another Mapchete process as input."""

from mapchete import Mapchete
from mapchete.config import MapcheteConfig
from mapchete.formats import base
from mapchete.io.vector import reproject_geometry


METADATA = {
    "driver_name": "Mapchete",
    "data_type": None,
    "mode": "r",
    "file_extensions": ["mapchete"]
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
        "driver_name": "Mapchete",
        "data_type": None,
        "mode": "r",
        "file_extensions": ["mapchete"]
    }

    def __init__(self, input_params):
        """Initialize."""
        super(InputData, self).__init__(input_params)
        self.path = input_params["path"]
        if self.path:
            self.process = Mapchete(MapcheteConfig(self.path, mode="readonly"))
        self._bbox_cache = {}

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
        if out_crs is None:
            out_crs = self.pyramid.crs
        if str(out_crs) not in self._bbox_cache:
            self._bbox_cache[str(out_crs)] = reproject_geometry(
                self.process.config.process_area(),
                src_crs=self.process.config.crs,
                dst_crs=out_crs)

        return self._bbox_cache[str(out_crs)]
