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
    - properties: key-value pairs (fields and field types, like "id: int" etc.)
    - geometry: output geometry type (Geometry, Point, MultiPoint, Line, MultiLine,
    Polygon, MultiPolygon)
"""

from mapchete.formats.default import _fiona_base

METADATA = {"driver_name": "GeoJSON", "data_type": "vector", "mode": "rw"}


class OutputDataReader(_fiona_base.OutputDataReader):
    """
    Output reader class for GeoJSON.

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

    METADATA = METADATA

    def __init__(self, output_params, **kwargs):
        """Initialize."""
        super().__init__(output_params)
        self.path = output_params["path"]
        self.file_extension = ".geojson"
        self.output_params = output_params
        self._bucket = (
            self.path.split("/")[2] if self.path.startswith("s3://") else None
        )


class TileDirectoryOutputDataWriter(
    _fiona_base.TileDirectoryOutputDataWriter, OutputDataReader
):

    METADATA = METADATA


class SingleFileOutputDataWriter(
    _fiona_base.SingleFileOutputDataWriter, OutputDataReader
):

    METADATA = METADATA


class OutputDataWriter:
    def __new__(self, output_params, **kwargs):
        """Initialize."""
        self.path = output_params["path"]
        self.file_extension = ".geojson"
        if self.path.endswith(self.file_extension):
            return SingleFileOutputDataWriter(output_params, **kwargs)
        else:
            return TileDirectoryOutputDataWriter(output_params, **kwargs)


class InputTile(_fiona_base.InputTile):
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

    pass
