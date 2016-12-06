"""Mapchete."""

import os
import py_compile
import logging
import logging.config
import traceback
import imp
import types
import time
import threading
import numpy as np
import numpy.ma as ma
from cachetools import LRUCache
from copy import copy

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

    def __init__(self, config, with_cache=False):
        """
        Initialize Mapchete processing endpoint.

        - config: a valid MapcheteConfig object
        - with_cache: cache processed output data in memory
        """
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
        if self.config.mode == "memory":
            self.with_cache = True
        else:
            self.with_cache = with_cache
        if self.with_cache:
            self.process_tile_cache = LRUCache(maxsize=32)
            self.current_processes = {}
            self.process_lock = threading.Lock()

    def get_process_tiles(self, zoom=None):
        """
        Return process tiles.

        Tiles intersecting with the input data bounding boxes as well as
        process bounds, if provided, are considered process tiles. This is to
        avoid iterating through empty tiles.

        - zoom: Just return tiles of a specific zoom level

        Returns an iterable of BufferedTile objects.
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
        - overwrite: Overwrite existing tiles (default: False)
        - no_write: Never write, just process and cache tiles in RAM (doesn't
            work with multiprocessing; default: False)

        Returns a BufferedTile with process output in the data attribute. If
        there is no process output, data is None and there is information
        on the process status in the message attribute.
        """
        assert self.config.mode in ["memory", "continue", "overwrite"]
        if process_tile.zoom not in self.config.zoom_levels:
            process_tile.data = self.config.output.empty(process_tile)
            return process_tile
        assert isinstance(process_tile, BufferedTile)
        return self._execute(process_tile)

    def read(self, output_tile):
        """
        Read from written process output.

        - output_tile: Member of the output tile pyramid (not necessarily
            the process pyramid, if output has a different metatiling setting)

        Returns output_tile with appended data.
        """
        assert self.config.mode in ["readonly", "continue", "overwrite"]
        return self.config.output.read(output_tile)

    def write(self, process_tile):
        """
        Write data into output format.

        - process_tile: the process_tile with appended data
        - overwrite: overwrite existing data (default: True)
        """
        assert self.config.mode in ["continue", "overwrite"]
        starttime = time.time()
        if not process_tile or process_tile.data is None:
            message = "nothing written"
            error = "no errors"
        else:
            message = "write"
            try:
                self.config.output.write(copy(process_tile), overwrite=True)
                error = "no errors"
            except Exception as e:
                raise
                error = e
        endtime = time.time()
        elapsed = "%ss" % (round((endtime - starttime), 3))
        LOGGER.info(
            (self.process_name, process_tile.id, message, error, elapsed))

    def get_raw_output(self, tile, metatiling=1, pixelbuffer=0):
        """
        Get output raw data.

        This function won't work with multiprocessing, as it uses the
        threading.Lock() class.

        - tile: Either a tuple tile index, Tile or BufferedTile. If a tile
            index is given, a tile will be generated using the metatiling
            setting. Tile cannot be bigger than process tile!
        - metatiling: Tile metatile size. Only relevant if tile index is
            provided. (default: 1)
        - pixelbuffer: Tile pixelbuffer. Only relevant if no BufferedTile is
            provided. Also, cannot be greater than process pixelbuffer.
            (default: 0)
        - overwrite: Overwrite existing tiles (default: False)
        - no_write: Never write, just process and cache tiles in RAM (doesn't
            work with multiprocessing; default: False)

        Returns BufferedTile with appended output data.
        """
        assert isinstance(tile, BufferedTile)
        # Return empty data if zoom level is outside of process zoom levels.
        if tile.zoom not in self.config.zoom_levels:
            tile.data = self.config.output.empty(tile)
            return tile
        if self.config.mode == "memory":
            # Determine affected process Tile and check whether it is already
            # cached.
            process_tile = self.config.process_pyramid.intersecting(tile)[0]
            output = self._execute_using_cache(process_tile)
            return self._extract(output, tile)

        # TODO: cases where tile intersects with multiple process tiles
        process_tile = self.config.process_pyramid.intersecting(tile)[0]
        output_tiles = self.config.output_pyramid.intersecting(tile)

        if self.config.mode == "readonly":
            if self.config.output.tiles_exist(process_tile):
                return self._read_existing_output(tile, output_tiles)
            else:
                tile.data = self.config.output.empty(tile)
                return tile

        if self.config.mode == "continue":
            if self.config.output.tiles_exist(process_tile):
                return self._read_existing_output(tile, output_tiles)
            else:
                return self._process_and_overwrite_output(tile, process_tile)

        if self.config.mode == "overwrite":
            return self._process_and_overwrite_output(tile, process_tile)

    def _process_and_overwrite_output(self, tile, process_tile):
        if self.with_cache:
            output = self._execute_using_cache(process_tile)
        else:
            output = self.execute(process_tile)
            self.write(output)
            extract = self._extract(output, tile)
            return extract

    def _read_existing_output(self, tile, output_tiles):
        if self.config.output.METADATA["data_type"] == "raster":
            mosaic, affine = raster.create_mosaic(
                [self.read(output_tile) for output_tile in output_tiles])
            tile.data = raster.extract_from_array(mosaic, affine, tile)
            return tile
        elif self.config.output.METADATA["data_type"] == "vector":
            raise NotImplementedError()

    def _execute_using_cache(self, process_tile):
        assert self.with_cache
        assert self.config.mode in ["memory", "continue", "overwrite"]
        # Extract Tile subset from process Tile and return.
        try:
            return self.process_tile_cache[process_tile.id]
        except KeyError:
            pass
        # Lock process for Tile or wait.
        with self.process_lock:
            process_event = self.current_processes.get(process_tile.id)
            if not process_event:
                self.current_processes[process_tile.id] = threading.Event()
        # Wait and return.
        if process_event:
            process_event.wait()
            try:
                return self.process_tile_cache[process_tile.id]
            except KeyError:
                raise RuntimeError("tile not in cache")
        else:
            try:
                output = self.execute(process_tile)
                self.process_tile_cache[process_tile.id] = output
                if self.config.mode in ["continue", "overwrite"]:
                    try:
                        self.write(output)
                    except OSError:
                        pass
                return self.process_tile_cache[process_tile.id]
            except:
                raise
            finally:
                with self.process_lock:
                    process_event = self.current_processes.get(
                        process_tile.id)
                    del self.current_processes[process_tile.id]
                    process_event.set()

    def _extract(self, process_tile, tile):
        try:
            process_tile = self.process_tile_cache[process_tile.id]
        except:
            pass
        if self.config.output.METADATA["data_type"] == "raster":
            tile.data = raster.extract_from_tile(
                process_tile, tile)
            return tile
        elif self.config.output.METADATA["data_type"] == "vector":
            raise NotImplementedError()

    def _execute(self, process_tile):
        # If baselevel is active and zoom is outside of baselevel,
        # interpolate from other zoom levels.
        if self.config.baselevels:
            if process_tile.zoom < min(self.config.baselevels["zooms"]):
                process_data = self._interpolate_from_baselevel(
                    process_tile, "lower")
            elif process_tile.zoom > max(self.config.baselevels["zooms"]):
                process_data = self._interpolate_from_baselevel(
                    process_tile, "higher")
        # Otherwise, load process source and execute.
        else:
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
                starttime = time.time()
                message = "execute"
                error = "no errors"
                # Actually run process.
                process_data = tile_process.execute()
                # Log process time
            except Exception as e:
                raise
                error = e
                raise RuntimeError(
                    "error executing process: %s" % e)
            finally:
                endtime = time.time()
                elapsed = "%ss" % (round((endtime - starttime), 3))
                LOGGER.info((
                    self.process_name, process_tile.id, message,
                    error, elapsed))
                del tile_process
        # Analyze proess output.
        return self._streamline_output(process_data, process_tile)

    def _streamline_output(self, process_data, process_tile):
        if isinstance(process_data, str):
            process_tile.data = self.config.output.empty(process_tile)
            process_tile.message = process_data
        elif isinstance(process_data, ma.MaskedArray):
            process_tile.data = process_data.copy()
        elif isinstance(process_data, (list, tuple, np.ndarray)):
            process_tile.data = process_data
        elif isinstance(process_data, types.GeneratorType):
            process_tile.data = list(process_data)
        elif process_data is None:
            raise RuntimeError("process output is empty")
        else:
            raise RuntimeError(
                "not a valid process output: %s" % type(process_data))
        return process_tile

    def _interpolate_from_baselevel(self, process_tile, baselevel):
        try:
            starttime = time.time()
            message = "generate from baselevel"
            error = "no errors"
            if baselevel == "lower":
                parent_tile = self.get_raw_output(process_tile.get_parent())
                process_data = raster.resample_from_array(
                    parent_tile.data, parent_tile.affine, process_tile,
                    self.config.baselevels["higher"],
                    nodataval=self.config.output.nodata)
            elif baselevel == "higher":
                mosaic, mosaic_affine = raster.create_mosaic([
                    self.get_raw_output(base_tile)
                    for base_tile in process_tile.get_children()
                ])
                process_data = raster.resample_from_array(
                    mosaic, mosaic_affine, process_tile,
                    self.config.baselevels["lower"],
                    nodataval=self.config.output.nodata)
        except Exception as e:
            error = e
            raise
        finally:
            endtime = time.time()
            elapsed = "%ss" % (round((endtime - starttime), 3))
            LOGGER.info((
                self.process_name, process_tile.id, message,
                error, elapsed))
        return process_data


class MapcheteProcess(object):
    """
    Process class inherited by user process script.

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

    def read(self, **kwargs):
        """Read existing output data."""
        existing_tile = self.config.output.open(self.tile, **kwargs)
        if existing_tile.is_empty():
            return self.config.output.empty(self.tile)
        else:
            return existing_tile.read(**kwargs)

    def open(self, input_file, **kwargs):
        """
        Return appropriate InputTile object.

        - input_file: file path or file name from configuration file
        - **kwargs: driver specific parameters (e.g. resampling)
        """
        if not isinstance(input_file, str):
            try:
                return input_file.open(self.tile, **kwargs)
            except:
                raise IOError("please specify correct input_file name")
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
