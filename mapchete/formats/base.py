"""
Main base classes for input and output formats.

When writing a new driver, please inherit from these classes and implement the
respective interfaces.
"""

import os
from tilematrix import TilePyramid


class InputData(object):
    """
    Template class handling geographic input data.

    Parameters
    ----------
    input_params : dictionary
        driver specific parameters

    Attributes
    ----------
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
        "driver_name": None,
        "data_type": None,
        "mode": "r"
    }

    def __init__(self, input_params, **kwargs):
        """Initialize relevant input information."""
        self.pyramid = input_params["pyramid"]
        self.pixelbuffer = input_params["pixelbuffer"]
        self.crs = self.pyramid.crs
        self.srid = self.pyramid.srid

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
        raise NotImplementedError

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
        raise NotImplementedError

    def exists(self):
        """
        Check if data or file even exists.

        Returns
        -------
        file exists : bool
        """
        raise NotImplementedError

    def cleanup(self):
        """Optional cleanup function called when Mapchete exits."""
        pass


class InputTile(object):
    """
    Target Tile representation of input data.

    Parameters
    ----------
    tile : ``Tile``
    kwargs : keyword arguments
        driver specific parameters
    """

    def __init__(self, tile, **kwargs):
        """Initialize."""

    def read(self, **kwargs):
        """
        Read reprojected & resampled input data.

        Returns
        -------
        data : array or list
            NumPy array for raster data or feature list for vector data
        """
        raise NotImplementedError

    def is_empty(self):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        raise NotImplementedError

    def __enter__(self):
        """Required for 'with' statement."""
        return self

    def __exit__(self, t, v, tb):
        """Clean up."""
        pass


class OutputData(object):
    """
    Template class handling process output data.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
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
        "driver_name": None,
        "data_type": None,
        "mode": "w"
    }

    def __init__(self, output_params):
        """Initialize."""
        self.pixelbuffer = output_params["pixelbuffer"]
        self.pyramid = TilePyramid(
            output_params["type"], metatiling=output_params["metatiling"])
        self.crs = self.pyramid.crs
        self.srid = self.pyramid.srid

    def read(self, output_tile):
        """
        Read existing process output.

        Parameters
        ----------
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        process output : array or list
        """
        raise NotImplementedError

    def write(self, process_tile):
        """
        Write data from one or more process tiles.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        raise NotImplementedError

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
        if process_tile and output_tile:
            raise ValueError(
                "just one of 'process_tile' and 'output_tile' allowed")
        if process_tile:
            return any(
                os.path.exists(self.get_path(tile))
                for tile in self.pyramid.intersecting(process_tile))
        if output_tile:
            return os.path.exists(self.get_path(output_tile))

    def is_valid_with_config(self, config):
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
        raise NotImplementedError

    def for_web(self, data):
        """
        Convert data to web output (raster only).

        Parameters
        ----------
        data : array

        Returns
        -------
        web data : array
        """
        raise NotImplementedError

    def empty(self, process_tile):
        """
        Return empty data.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        empty data : array or list
            empty array with correct data type for raster data or empty list
            for vector data
        """
        raise NotImplementedError

    def open(self, tile, process):
        """
        Open process output as input for other process.

        Parameters
        ----------
        tile : ``Tile``
        process : ``MapcheteProcess``
        """
        raise NotImplementedError
