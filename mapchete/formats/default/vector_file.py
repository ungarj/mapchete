"""
Vector file input which can be read by fiona.

Currently limited by extensions .shp and .geojson but could be extended easily.
"""

import fiona
from shapely.geometry import box
from rasterio.crs import CRS

from mapchete.formats import base
from mapchete.io.vector import reproject_geometry, read_vector_window


METADATA = {
    "driver_name": "vector_file",
    "data_type": "vector",
    "mode": "r",
    "file_extensions": ["shp", "geojson", "gpkg"]
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
        "driver_name": "vector_file",
        "data_type": "vector",
        "mode": "r",
        "file_extensions": ["shp", "geojson"]
    }

    def __init__(self, input_params, **kwargs):
        """Initialize."""
        super().__init__(input_params, **kwargs)
        self.path = input_params["path"]

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
        return InputTile(tile, self, **kwargs)

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
        with fiona.open(self.path) as inp:
            inp_crs = CRS(inp.crs)
            bbox = box(*inp.bounds)
        # TODO find a way to get a good segmentize value in bbox source CRS
        return reproject_geometry(bbox, src_crs=inp_crs, dst_crs=out_crs)


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
    vector_file : string
        path to input vector file
    """

    def __init__(self, tile, vector_file, **kwargs):
        """Initialize."""
        self.tile = tile
        self.vector_file = vector_file
        self._cache = {}

    def read(self, validity_check=True, **kwargs):
        """
        Read reprojected & resampled input data.

        Parameters
        ----------
        validity_check : bool
            also run checks if reprojected geometry is valid, otherwise throw
            RuntimeError (default: True)

        Returns
        -------
        data : list
        """
        return [] if self.is_empty() else self._read_from_cache(validity_check)

    def is_empty(self):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        if not self.tile.bbox.intersects(self.vector_file.bbox()):
            return True
        return len(self._read_from_cache(True)) == 0

    def _read_from_cache(self, validity_check):
        checked = "checked" if validity_check else "not_checked"
        if checked not in self._cache:
            self._cache[checked] = list(read_vector_window(
                self.vector_file.path, self.tile,
                validity_check=validity_check)
            )
        return self._cache[checked]
