"""
Vector file input which can be read by fiona.

Currently limited by extensions .shp and .geojson but could be extended easily.
"""

import fiona
from shapely.geometry import box, Polygon
from rasterio.crs import CRS

from mapchete.formats import base
from mapchete.io.vector import reproject_geometry, read_vector_window


METADATA = {
    "driver_name": "vector_file",
    "data_type": "vector",
    "mode": "r",
    "file_extensions": ["shp", "geojson"]
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

    def __init__(self, input_params):
        """Initialize."""
        super(InputData, self).__init__(input_params)
        self.path = input_params["path"]
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
        if out_crs is None:
            out_crs = self.pyramid.crs
        if str(out_crs) not in self._bbox_cache:
            with fiona.open(self.path) as inp:
                inp_crs = CRS(inp.crs)
                try:
                    assert inp_crs.is_valid
                except AssertionError:
                    raise IOError("CRS could not be read from %s" % self.path)
                bbox = box(*inp.bounds)
            # Check if file bounding box is empty.
            if len(set(inp.bounds)) == 1:
                self._bbox_cache[str(out_crs)] = Polygon()
            # If soucre and target CRSes differ, segmentize and reproject
            if inp_crs != out_crs:
                self._bbox_cache[str(out_crs)] = reproject_geometry(
                    bbox, src_crs=inp_crs, dst_crs=out_crs
                )
            else:
                self._bbox_cache[str(out_crs)] = bbox

        return self._bbox_cache[str(out_crs)]


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

    def __init__(self, tile, vector_file):
        """Initialize."""
        self.tile = tile
        self.vector_file = vector_file
        self._cache = {}

    def read(self, validity_check=True):
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
        return self._read_from_cache(validity_check)

    def is_empty(self):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        if not self.tile.bbox.intersects(self.vector_file.bbox()):
            return True
        if self.read():
            return False
        else:
            return True

    def _read_from_cache(self, validity_check):
        checked = "checked" if validity_check else "not_checked"
        if checked not in self._cache:
            self._cache[checked] = list(read_vector_window(
                self.vector_file.path, self.tile,
                validity_check=validity_check))
        return self._cache[checked]
