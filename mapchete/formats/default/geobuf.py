"""
Handles writing process output into a pyramid of Geobuf files.

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

import logging
from shapely.geometry import mapping, shape

from mapchete.config import validate_values
from mapchete.formats.default import geojson
from mapchete.io import fs_from_path
from mapchete.io._geometry_operations import _repair, reproject_geometry


logger = logging.getLogger(__name__)
METADATA = {"driver_name": "Geobuf", "data_type": "vector", "mode": "rw"}


class OutputDataReader(geojson.OutputDataReader):
    """
    Output reader class for Geobuf Tile Directory.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    path : string
        path to output directory
    file_extension : string
        file extension for output files (.geobuf)
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
        self.file_extension = ".pbf"
        self.output_params = output_params
        self._bucket = None

    def read(self, output_tile, **kwargs):
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
        import geobuf

        path = self.get_path(output_tile)
        try:
            with fs_from_path(path).open(path, "rb") as src:
                return geobuf.decode(src.read()).get("features", [])
        except FileNotFoundError:
            return self.empty(output_tile)

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
        validate_values(config, [("schema", dict), ("path", str)])
        validate_values(config["schema"], [("properties", dict), ("geometry", str)])
        if config["schema"]["geometry"] not in [
            "Geometry",
            "Point",
            "MultiPoint",
            "Line",
            "MultiLine",
            "Polygon",
            "MultiPolygon",
        ]:  # pragma: no cover
            raise TypeError("invalid geometry type")
        return True

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
        import geobuf

        return (
            geobuf.encode(
                dict(
                    type="FeatureCollection",
                    features=[
                        dict(
                            f,
                            geometry=mapping(_repair(shape(f["geometry"]))),
                            type="Feature",
                        )
                        for f in data
                    ],
                )
            ),
            "application/octet-stream",
        )

    def _read_as_tiledir(self, out_tile=None, tiles_paths=None, **kwargs):
        return [
            dict(
                feature,
                geometry=mapping(
                    reproject_geometry(
                        shape(feature["geometry"]),
                        src_crs=tile.crs,
                        dst_crs=out_tile.crs,
                    ).intersection(out_tile.bbox)
                ),
            )
            for tile, _ in tiles_paths
            for feature in self.read(tile)
        ]  # pragma: no cover
        # --> this part is covered by the CLI tests but somehow gets omitted


class OutputDataWriter(geojson.OutputDataWriter, OutputDataReader):
    """
    Output writer class.
    """

    METADATA = METADATA


class InputTile(geojson.InputTile):
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
