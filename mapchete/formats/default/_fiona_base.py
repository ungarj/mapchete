"""
Baseclasses for all drivers using fiona for reading and writing data.
"""
from contextlib import ExitStack
import fiona
from fiona.errors import DriverError
import logging
import os
from shapely.geometry import mapping
import types

from mapchete.config import validate_values, _OUTPUT_PARAMETERS
from mapchete.errors import MapcheteConfigError
from mapchete.formats import base
from mapchete.io import get_boto3_bucket, makedirs, path_exists, path_is_remote
from mapchete.io._geometry_operations import (
    to_shape,
    multipart_to_singleparts,
    clean_geometry_type,
)
from mapchete.io.vector import write_vector_window, fiona_write
from mapchete.tile import BufferedTile


logger = logging.getLogger(__name__)


class OutputDataReader:
    """
    Output reader base class for vector drivers.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    path : string
        path to output directory
    file_extension : string
        file extension for output files
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
        try:
            with fiona.open(self.get_path(output_tile), "r") as src:
                return list(src)
        except DriverError as e:
            for i in (
                "does not exist in the file system",
                "No such file or directory",
                "specified key does not exist.",
            ):
                if i in str(e):
                    return self.empty(output_tile)
            else:  # pragma: no cover
                raise

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
            "LineString",
            "MultiLine",
            "Polygon",
            "MultiPolygon",
            "Unknown",
        ]:  # pragma: no cover
            raise TypeError("invalid geometry type")
        return True

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


class TileDirectoryOutputDataWriter(
    base.TileDirectoryOutputWriter,
    OutputDataReader,
    base.TileDirectoryOutputReader,
):
    def write(self, process_tile, data):
        """
        Write data from process tiles into vector file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        if data is None or len(data) == 0:
            return
        if not isinstance(data, (list, types.GeneratorType)):  # pragma: no cover
            raise TypeError(
                "vector driver data has to be a list or generator of GeoJSON objects"
            )

        data = list(data)
        if not len(data):  # pragma: no cover
            logger.debug("no features to write")
        else:
            # in case of S3 output, create an boto3 resource
            bucket_resource = get_boto3_bucket(self._bucket) if self._bucket else None

            # Convert from process_tile to output_tiles
            for tile in self.pyramid.intersecting(process_tile):
                out_path = self.get_path(tile)
                self.prepare_path(tile)
                out_tile = BufferedTile(tile, self.pixelbuffer)
                write_vector_window(
                    in_data=data,
                    out_driver=self.METADATA["driver_name"],
                    out_schema=self.output_params["schema"],
                    out_tile=out_tile,
                    out_path=out_path,
                    bucket_resource=bucket_resource,
                    allow_multipart_geometries=(
                        self.output_params["schema"]["geometry"].startswith("Multi")
                    ),
                )


class SingleFileOutputDataWriter(OutputDataReader, base.SingleFileOutputWriter):

    write_in_parent_process = True

    def __init__(self, output_params, **kwargs):
        """Initialize."""
        logger.debug("output is single file")
        self.dst = None
        super().__init__(output_params, **kwargs)
        self._set_attributes(output_params)
        if len(self.output_params["delimiters"]["zoom"]) != 1:
            raise ValueError("single file output only works with one zoom level")
        self.zoom = output_params["delimiters"]["zoom"][0]
        self.in_memory = output_params.get("in_memory", True)

    def _set_attributes(self, output_params):
        self.path = output_params["path"]
        self.output_params = output_params

    def prepare(self, process_area=None, **kwargs):
        # set up fiona
        if path_exists(self.path):
            if self.output_params["mode"] != "overwrite":
                raise MapcheteConfigError(
                    f"{self.path} already exists, use overwrite mode to replace"
                )
            elif not path_is_remote(self.path):
                logger.debug("remove existing file: %s", self.path)
                os.remove(self.path)
        # create output directory if necessary
        makedirs(os.path.dirname(self.path))
        logger.debug("open output file: %s", self.path)
        self._ctx = ExitStack()
        self.dst = self._ctx.enter_context(
            fiona_write(
                self.path,
                "w",
                driver=self.METADATA["driver_name"],
                schema=self.output_params["schema"],
                crs=self.crs,
            )
        )

    def read(self, output_tile, **kwargs):
        """
        Read existing process output.

        Parameters
        ----------
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        NumPy array
        """
        # ATTENTION: Fiona cannot handle reading from Collection opened in write mode.
        # return list(self.dst)
        return []

    def get_path(self, tile=None):
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
        return self.path

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
            raise ValueError("just one of 'process_tile' and 'output_tile' allowed")
        if process_tile:
            for tile in self.pyramid.intersecting(process_tile):
                if len(self.read(tile)) > 0:
                    return True
            else:
                return False

        if output_tile:
            return len(self.read(output_tile)) > 0

    def write(self, process_tile, data, allow_multipart_geometries=False):
        """
        Write data from process tiles into GeoTIFF file(s).

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        if data is None or len(data) == 0:
            return
        if not isinstance(data, (list, types.GeneratorType)):  # pragma: no cover
            raise TypeError(
                "vector driver data has to be a list or generator of GeoJSON objects"
            )

        data = list(data)
        if not len(data):  # pragma: no cover
            logger.debug("no features to write")
        else:
            # Convert from process_tile to output_tiles and write
            for tile in self.pyramid.intersecting(process_tile):
                out_tile = BufferedTile(tile, self.pixelbuffer)
                out_features = []
                for feature in data:
                    try:
                        # clip feature geometry to tile bounding box and append for writing
                        clipped = clean_geometry_type(
                            to_shape(feature["geometry"]).intersection(out_tile.bbox),
                            self.output_params["schema"]["geometry"],
                        )
                        if allow_multipart_geometries:
                            cleaned_output_features = [clipped]
                        else:
                            cleaned_output_features = multipart_to_singleparts(clipped)
                        for out_geom in cleaned_output_features:
                            if out_geom.is_empty:  # pragma: no cover
                                continue

                            out_features.append(
                                {
                                    "geometry": mapping(out_geom),
                                    "properties": feature["properties"],
                                }
                            )
                    except Exception as e:
                        logger.warning("failed to prepare geometry for writing: %s", e)
                        continue
                if out_features:
                    self.dst.writerecords(out_features)

    def close(self, exc_type=None, exc_value=None, exc_traceback=None):
        """Build overviews and write file."""
        self._ctx.close()


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

    def read(self, validity_check=True, no_neighbors=False, **kwargs):
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
        if no_neighbors:  # pragma: no cover
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
            self._cache[validity_check] = self.process.get_raw_output(self.tile)
        return self._cache[validity_check]

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, t, v, tb):
        """Clear cache on close."""
        self._cache = {}
