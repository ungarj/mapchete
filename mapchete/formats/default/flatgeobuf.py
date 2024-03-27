"""
Handles writing process output into a pyramid of FlatGeobuf files.

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

import warnings

from mapchete.formats.default import _fiona_base

METADATA = {"driver_name": "FlatGeobuf", "data_type": "vector", "mode": "rw"}


class OutputDataReader(_fiona_base.OutputDataReader):
    """
    Output reader class for FlatGeobuf.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    path : string
        path to output directory
    file_extension : string
        file extension for output files (.fgb)
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
        self.file_extension = ".fgb"

        # make sure only field types allowed by FlatGeobuf are defined
        for k, v in output_params["schema"]["properties"].items():
            if v == "date":  # pragma: no cover
                warnings.warn(
                    UserWarning(
                        f"""'{k}' field has type '{v}' which is not allowed by FlatGeobuf """
                        """and will be changed to 'string'"""
                    )
                )
                output_params["schema"]["properties"][k] = "str"

        self.output_params = output_params


class OutputDataWriter(_fiona_base.OutputDataWriter, OutputDataReader):
    METADATA = METADATA


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
