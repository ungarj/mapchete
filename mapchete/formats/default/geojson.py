"""
Handles writing process output into a pyramid of GeoJSON files.

This output format is restricted to the geodetic (WGS84) projection because it
is the only projection the GeoJSON spec supports.

output configuration parameters
-------------------------------

output type has to be ``geodetic``

mandatory
~~~~~~~~~

path: string
    output directory
schema: key-value pairs
    the schema is passed on to fiona
    properties: key-value pairs
        fields and field types, like "id: int" etc.
    geometry: geometry type
        output geometry type (Geometry, Point, MultiPoint, Line, MultiLine,
        Polygon, MultiPolygon)
"""

import os
import types
import fiona

from mapchete.tile import BufferedTile
from mapchete.formats import base
from mapchete.io.vector import write_vector_window


METADATA = {
    "driver_name": "GeoJSON",
    "data_type": "vector",
    "mode": "rw"
}


class OutputData(base.OutputData):
    """
    Output class for GeoJSON.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    path : string
        path to output directory
    file_extension : string
        file extension for output files (.geojson)
    output_params : dictionary
        output parameters from Mapchete file
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
        "driver_name": "GeoJSON",
        "data_type": "vector",
        "mode": "rw"
    }

    def __init__(self, output_params):
        """Initialize."""
        super(OutputData, self).__init__(output_params)
        self.path = output_params["path"]
        self.file_extension = ".geojson"
        self.output_params = output_params

    def read(self, output_tile):
        """
        Read existing process output.

        Parameters
        ----------
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        process output : list
        """
        path = self.get_path(output_tile)
        if os.path.isfile(path):
            with fiona.open(path, "r") as src:
                output_tile.data = list(src)
        else:
            output_tile.data = self.empty(output_tile)
        return output_tile

    def write(self, process_tile):
        """
        Write data from process tiles into GeoJSON file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        if process_tile.data is None:
            return
        assert isinstance(process_tile.data, (list, types.GeneratorType))
        if isinstance(process_tile.data, types.GeneratorType):
            process_tile.data = list(process_tile.data)
        # Convert from process_tile to output_tiles
        for tile in self.pyramid.intersecting(process_tile):
            # skip if file exists and overwrite is not set
            out_path = self.get_path(tile)
            self.prepare_path(tile)
            out_tile = BufferedTile(tile, self.pixelbuffer)
            write_vector_window(
                in_tile=process_tile, out_schema=self.output_params["schema"],
                out_tile=out_tile, out_path=out_path)

    def tiles_exist(self, process_tile):
        """
        Check whether output tiles of a process tile exist.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        exists : bool
        """
        return any(
            os.path.exists(self.get_path(tile))
            for tile in self.pyramid.intersecting(process_tile)
        )

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
        assert isinstance(config, dict)
        assert "schema" in config
        assert isinstance(config["schema"], dict)
        assert "properties" in config["schema"]
        assert isinstance(config["schema"]["properties"], dict)
        assert "geometry" in config["schema"]
        assert config["schema"]["geometry"] in [
            "Geometry", "Point", "MultiPoint", "Line", "MultiLine",
            "Polygon", "MultiPolygon"]
        assert "path" in config
        assert isinstance(config["path"], str)
        assert config["type"] == "geodetic"
        return True

    def get_path(self, tile):
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
        zoomdir = os.path.join(self.path, str(tile.zoom))
        rowdir = os.path.join(zoomdir, str(tile.row))
        return os.path.join(rowdir, str(tile.col) + self.file_extension)

    def prepare_path(self, tile):
        """
        Create directory and subdirectory if necessary.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``
        """
        zoomdir = os.path.join(self.path, str(tile.zoom))
        if not os.path.exists(zoomdir):
            os.makedirs(zoomdir)
        rowdir = os.path.join(zoomdir, str(tile.row))
        if not os.path.exists(rowdir):
            os.makedirs(rowdir)

    def empty(self, process_tile=None):
        """
        Return empty data.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        empty data : list
        """
        return []

    def open(self, tile, process):
        """
        Open process output as input for other process.

        Parameters
        ----------
        tile : ``Tile``
        process : ``MapcheteProcess``
        """
        return InputTile(tile, process)


class InputTile(base.InputTile):
    """
    Target Tile representation of input data.

    Parameters
    ----------
    tile : ``Tile``
    process : ``MapcheteProcess``

    Attributes
    ----------
    tile : ``Tile``
    process : ``MapcheteProcess``
    """

    def __init__(self, tile, process):
        """Initialize."""
        self.tile = tile
        self.process = process
        self._cache = {}

    def read(self, validity_check=True, no_neighbors=False):
        """
        Read data from process output.

        Parameters
        ----------
        validity_check : bool
            run geometry validity check (default: True)
        no_neighbors : bool
            don't include neighbor tiles if there is a pixelbuffer (default:
            False)

        Returns
        -------
        features : list
            GeoJSON-like list of features
        """
        if no_neighbors:
            raise NotImplementedError()
        return self._from_cache(validity_check=validity_check)

    def is_empty(self, validity_check=True):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        if self._from_cache(validity_check=validity_check) == []:
            return False
        else:
            return True

    def _from_cache(self, validity_check=True):
        if validity_check not in self._cache:
            self._cache[validity_check] = self.process.get_raw_output(
                self.tile).data
        return self._cache[validity_check]

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, t, v, tb):
        """Clear cache on close."""
        del self._cache
