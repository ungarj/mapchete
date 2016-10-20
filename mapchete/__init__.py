"""Mapchete."""

import os
import py_compile
import logging
import logging.config
import traceback
import imp
import types
import time
import numpy as np
import numpy.ma as ma
from tilematrix import Tile, TilePyramid

from mapchete import commons
from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTile
from mapchete.io import raster

LOGGER = logging.getLogger("mapchete")


class Mapchete(object):
    """
    Main entry point to every processing job.

    From here, the process tiles can be determined and executed.
    """

    def __init__(self, config):
        """Initialize Mapchete job."""
        assert isinstance(config, MapcheteConfig)
        self.config = config
        config.output
        try:
            py_compile.compile(self.config.process_file, doraise=True)
        except:
            raise
        self.process_name = os.path.splitext(
            os.path.basename(self.config.process_file)
        )[0]

    def get_process_tiles(self, zoom=None):
        """
        Return process tiles.

        Tiles intersecting with the input data bounding boxes as well as
        process bounds, if provided, are considered process tiles. This is to
        avoid iterating through empty tiles.

        - zoom: zoom level
        """
        try:
            if zoom or zoom == 0:
                assert isinstance(zoom, int)
                bbox = self.config.process_area(zoom)
                for tile in self.config.process_pyramid.tiles_from_geom(
                    bbox, zoom
                ):
                    yield tile
            else:
                for zoom in reversed(self.config.zoom_levels):
                    bbox = self.config.process_area(zoom)
                    for tile in self.config.process_pyramid.tiles_from_geom(
                        bbox, zoom
                    ):
                        yield tile
        except Exception:
            LOGGER.error(
                "error getting work tiles: %s" % traceback.print_exc())
            raise

    def execute(self, process_tile):
        """
        Run the Mapchete process.

        Execute, write and return process_tile with data.
        - process_tile: Member of the process tile pyramid (not necessarily
            the output pyramid, if output has a different metatiling setting)

        Returns a BufferedTile with process output in the data attribute. If
        there is no process output, data is None and there is information
        on the process status in the message attribute.
        """
        if isinstance(process_tile, Tile):
            process_tile = BufferedTile(
                process_tile, pixelbuffer=self.config.pixelbuffer)
        elif isinstance(process_tile, BufferedTile):
            pass
        else:
            raise ValueError("invalid process_tile type for execute()")
        starttime = time.time()
        message = "execute"
        try:
            output = self._execute(process_tile)
            error = "no errors"
        except Exception as e:
            output = None
            error = e
        endtime = time.time()
        elapsed = "%ss" % (round((endtime - starttime), 3))
        LOGGER.info(
            (self.process_name, process_tile.id, message, error, elapsed))
        return output

    def read(self, output_tile):
        """
        Read from written process output.

        Return output_tile with appended data.
        - output_tile: Member of the output tile pyramid (not necessarily
            the process pyramid, if output has a different metatiling setting)
        """
        raise NotImplementedError

    def write(self, process_tile, overwrite=False):
        """
        Write data into output format.

        - process_tile: the process_tile with appended data
        - overwrite: overwrite existing data (default: True)
        """
        starttime = time.time()
        if not process_tile or process_tile.data is None:
            LOGGER.info((process_tile.id, "empty"))
            return
        message = "write"
        try:
            self.config.output.write(process_tile, overwrite=True)
            error = "no errors"
        except Exception as e:
            raise
            error = e
        endtime = time.time()
        elapsed = "%ss" % (round((endtime - starttime), 3))
        LOGGER.info(
            (self.process_name, output_tile.id, message, error, elapsed))

    def get_web_tile_from_output(
        self, web_tile, output_tile, web_metatiling=1, overwrite=False,
        no_write=False
    ):
        """
        Get output as a file object.

        read() or execute() (and optional write()), convert to view output.
        Return output_tile with appended output data.
        - output_tile: Member of the output tile pyramid (not necessarily
            the process pyramid, if output has a different metatiling setting)
        - web_metatiling: metatiling setting for REST endpoint
        - overwrite: overwrite existing data (default: True)
        - no_write: override read() and write() and always execute()
        """

        return
        assert isinstance(web_tile, Tile)
        assert isinstance(output_tile, BufferedTile)
        web_pyramid = TilePyramid(
            self.config.output_type, metatiling=web_metatiling)
        web_output_tiles = {}
        if no_write:
            process_tile = self.config.process_pyramid.intersecting(
                    output_tile)[0]
            LOGGER.info(
                "output tile %s getting process tile %s" %
                (output_tile.id, process_tile.id))
            process_output = self.execute(process_tile)
            if self.config.output.METADATA["data_type"] == "raster":
                # get all web tiles
                # if raster output: use raster clip
                output_tile.data = raster.extract_from_tile(
                    process_output, output_tile)
                for web_tile in web_pyramid.intersecting(output_tile):
                    web_tile = BufferedTile(web_tile)
                    # convert with output.for_web(data)
                    web_output = self.config.output.for_web(
                        raster.extract_from_tile(
                            output_tile, web_tile))
                    # web_output_tiles[web_tile.id] = web_output
                    web_output_tiles[str(hash(web_tile.id))] = web_output
                return web_output_tiles
                # return dictionary with web tile IDs and file objects
            elif self.config.output.METADATA["data_type"] == "vector":
                raise NotImplementedError
            else:
                raise RuntimeError("output driver invalid")
        else:
            # if not overwrite and output_tile exists:
            #     read() and get all web tiles ...
            # else:
            #     execute(), write(), get all web tiles ...
            raise NotImplementedError

    def get_raw_output(self, output_tile, overwrite=False, no_write=False):
        """
        Get output raw data.

        read() or execute() (and optional write()). Return output_tile with
        appended output data.
        - output_tile: Member of the output tile pyramid (not necessarily
            the process pyramid, if output has a different metatiling setting)
        - overwrite: overwrite existing data (default: True)
        """
        if isinstance(output_tile, tuple):
            output_tile = BufferedTile(
                self.config.output_pyramid.tile(*output_tile),
                self.config.raw["output"]["metatiling"])
        elif isinstance(output_tile, BufferedTile):
            pass
        else:
            raise TypeError("tile id or BufferedTile required")
        if no_write:
            process_tile = self.config.process_pyramid.intersecting(
                    output_tile)[0]
            LOGGER.info(
                "output tile %s getting process tile %s" %
                (output_tile.id, process_tile.id))
            process_output = self.execute(process_tile)
            if self.config.output.METADATA["data_type"] == "raster":
                # get all web tiles
                # if raster output: use raster clip
                output_tile.data = raster.extract_from_tile(
                    process_output, output_tile)
                return output_tile
            elif self.config.output.METADATA["data_type"] == "vector":
                raise NotImplementedError
            else:
                raise RuntimeError("output driver invalid")


    def _execute(self, process_tile):

        # TODO If baselevel is active and zoom is outside of baselevel,
        # interpolate.

        # Otherwise, load process source and execute.
        try:
            new_process = imp.load_source(
                self.process_name + "Process", self.config.process_file)
            tile_process = new_process.Process(
                config=self.config, tile=process_tile,
                params=self.config.at_zoom(process_tile.zoom)
            )
        except Exception as e:
            raise RuntimeError(
                "error invoking process: %s" % e)
        try:
            # Actually run process.
            process_data = tile_process.execute()
        except Exception as e:
            raise
            raise RuntimeError(
                "error executing process: %s" % e)
        finally:
            tile_process = None
        # Analyze proess output.
        if isinstance(process_data, str):
            process_tile.data = self.config.output.empty(process_tile)
            process_tile.message = process_data
        elif isinstance(
            process_data, (list, tuple, np.ndarray, ma.MaskedArray)
        ):
            process_tile.data = process_data
        elif isinstance(process_data, types.GeneratorType):
            process_tile.data = list(process_data)
        elif process_data is None:
            raise RuntimeError("process output is empty")
        else:
            raise RuntimeError(
                "not a valid process output: %s" % type(process_data))
        return process_tile


class MapcheteProcess(object):
    """
    Actual process running on a tile.

    In fact, it is a Tile.
    Its attributes and methods can be accessed via "self" from within a
    Mapchete process Python file.
    """

    def __init__(self, tile, config=None, params=None):
        """Initialize Mapchete process."""
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""
        self.tile = tile
        self.tile_pyramid = tile.tile_pyramid
        self.params = params
        self.config = config

    def write(self, data, **kwargs):
        """Deprecated."""
        raise DeprecationWarning(
            "Please return process output data instead of using self.write().")

    def open(self, input_file, **kwargs):
        """
        Return appropriate InputTile object.

        - input_file: file path or file name from configuration file
        - **kwargs: driver specific parameters (e.g. resampling)
        """
        if input_file not in self.params["input_files"]:
            raise ValueError(
                "%s not found in config as input file" % input_file)
        return self.params["input_files"][input_file].open(self.tile, **kwargs)

    def hillshade(
        self, elevation, azimuth=315.0, altitude=45.0, z=1.0, scale=1.0
    ):
        """
        Calculate hillshading from elevation data.

        Returns an array with the same shape as the input array.
        - elevation: input array
        - azimuth: horizontal angle of light source (315: North-West)
        - altitude: vertical angle of light source (90 would result in slope
                    shading)
        - z: vertical exaggeration
        - scale: scale factor of pixel size units versus height units (insert
                 112000 when having elevation values in meters in a geodetic
                 projection)
        """
        return commons.hillshade(
            elevation, self, azimuth, altitude, z, scale)

    def contours(
        self, elevation, interval=100, field='elev'
    ):
        """
        Extract contour lines from elevation data.

        Returns contours as GeoJSON-like pairs of properties and geometry.
        - elevation: input array
        - interval: elevation value interval
        - field: output field name containing elevation value
        """
        return commons.contours(
            elevation, self.tile, interval=interval,
            pixelbuffer=self.pixelbuffer, field=field)

    def clip(
        self, array, geometries, inverted=False, clip_buffer=0
    ):
        """
        Return input array clipped by geometries.

        - inverted: bool, invert clipping
        - clip_buffer: int (in pixels), buffer geometries befor applying clip
        """
        return commons.clip_array_with_vector(
            array, self.tile.affine, geometries,
            inverted=inverted, clip_buffer=clip_buffer*self.tile.pixel_x_size)
