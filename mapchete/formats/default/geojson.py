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

import fiona
import os
import six
import types

from mapchete.tile import BufferedTile
from mapchete.formats import base
from mapchete.io.vector import write_vector_window
from mapchete.config import validate_values


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
                return list(src)
        else:
            return self.empty(output_tile)

    def write(self, process_tile, data):
        """
        Write data from process tiles into GeoJSON file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        if data is None or len(data) == 0:
            return
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        assert isinstance(data, (list, types.GeneratorType))
        data = list(data)
        # Convert from process_tile to output_tiles
        for tile in self.pyramid.intersecting(process_tile):
            # skip if file exists and overwrite is not set
            out_path = self.get_path(tile)
            self.prepare_path(tile)
            out_tile = BufferedTile(tile, self.pixelbuffer)
            write_vector_window(
                in_data=data, out_schema=self.output_params["schema"],
                out_tile=out_tile, out_path=out_path
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
        validate_values(config, [("schema", dict), ("path", six.string_types)])
        validate_values(
            config["schema"], [
                ("properties", dict), ("geometry", six.string_types)]
        )
        if config["schema"]["geometry"] not in [
            "Geometry", "Point", "MultiPoint", "Line", "MultiLine",
            "Polygon", "MultiPolygon"
        ]:
            raise TypeError("invalid geometry type")
        if config["type"].type != "geodetic":
            raise ValueError("output pyramid has to be geodetic")
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
        return list(data), "application/json"

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
        return len(self._from_cache(validity_check=validity_check)) == 0

    def _from_cache(self, validity_check=True):
        if validity_check not in self._cache:
            self._cache[validity_check] = self.process.get_raw_output(
                self.tile
            )
        return self._cache[validity_check]

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, t, v, tb):
        """Clear cache on close."""
        self._cache = {}
