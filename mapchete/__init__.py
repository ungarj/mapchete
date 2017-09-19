"""Main module managing processes."""

import os
import py_compile
import logging
import imp
import types
import time
import threading
import numpy as np
import numpy.ma as ma
from traceback import format_exc
from multiprocessing import cpu_count
from cachetools import LRUCache
from copy import copy
from itertools import chain

from mapchete._batch import batch_process
from mapchete.commons import clip as commons_clip
from mapchete.commons import contours as commons_contours
from mapchete.commons import hillshade as commons_hillshade
from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTile
from mapchete.io import raster, vector
from mapchete.errors import (
    MapcheteProcessSyntaxError, MapcheteProcessImportError,
    MapcheteProcessException, MapcheteProcessOutputError)

logging.basicConfig(
    level=logging.INFO, format='%(levelname)s %(name)s %(message)s'
)
LOGGER = logging.getLogger(__name__)


def open(
    config, mode="continue", zoom=None, bounds=None, single_input_file=None,
    with_cache=False, debug=False
):
    """
    Open a Mapchete process.

    Parameters
    ----------
    config : MapcheteConfig pr dict
        Mapchete process configuration
    mode : string
        * ``memory``: Generate process output on demand without reading
          pre-existing data or writing new data.
        * ``readonly``: Just read data without processing new data.
        * ``continue``: (default) Don't overwrite existing output.
        * ``overwrite``: Overwrite existing output.
    zoom : list or integer
        process zoom level or a pair of minimum and maximum zoom level
    bounds : tuple
        left, bottom, right, top process boundaries in output pyramid
    single_input_file : string
        single input file if supported by process
    with_cache : bool
        process output data cached in memory

    Returns
    -------
    Mapchete
        a Mapchete process object
    """
    return Mapchete(
        MapcheteConfig(
            config, mode=mode, zoom=zoom, bounds=bounds,
            single_input_file=single_input_file, debug=debug
        ),
        with_cache=with_cache
    )


class Mapchete(object):
    """
    Main entry point to every processing job.

    From here, the process tiles can be determined and executed.

    Parameters
    ----------
    config : MapcheteConfig
        Mapchete process configuration
    with_cache : bool
        cache processed output data in memory (default: False)

    Attributes
    ----------
    config : MapcheteConfig
        Mapchete process configuration
    process_name : string
        process name
    with_cache : bool
        process output data cached in memory
    process_tile_cache : LRUCache
        cache object storing the output data (only if with_cache = True)
    current_processed : dict
        process tiles currently processed (only if with_cache = True)
    process_lock : Lock
        lock object (only if with_cache = True)
    """

    def __init__(self, config, with_cache=False):
        """
        Initialize Mapchete processing endpoint.

        Parameters
        ----------
        config : MapcheteConfig
            Mapchete process configuration
        with_cache : bool
            cache processed output data in memory (default: False)
        """
        LOGGER.info("preparing process ...")
        if not isinstance(config, MapcheteConfig):
            raise TypeError("config must be MapcheteConfig object")
        self.config = config
        try:
            py_compile.compile(self.config.process_file, doraise=True)
        except py_compile.PyCompileError as e:
            raise MapcheteProcessSyntaxError(e)
        self.process_name = os.path.splitext(
            os.path.basename(self.config.process_file))[0]
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

        Parameters
        ----------
        zoom : integer
            zoom level process tiles should be returned from; if none is given,
            return all process tiles

        Returns
        -------
        generator
            iterable of BufferedTile objects
        """
        if zoom or zoom == 0:
            for tile in self.config.process_pyramid.tiles_from_geom(
                self.config.process_area(zoom), zoom
            ):
                yield tile
        else:
            for zoom in reversed(self.config.zoom_levels):
                for tile in self.config.process_pyramid.tiles_from_geom(
                    self.config.process_area(zoom), zoom
                ):
                    yield tile

    def batch_process(
        self, zoom=None, tile=None, multi=cpu_count(), quiet=False, debug=False
    ):
        """
        Process a large batch of tiles.

        Parameters
        ----------
        process : MapcheteProcess
            process to be run
        zoom : list or int
            either single zoom level or list of minimum and maximum zoom level;
            None processes all (default: None)
        tile : tuple
            zoom, row and column of tile to be processed (cannot be used with
            zoom)
        multi : int
            number of workers (default: number of CPU cores)
        quiet : bool
            set log level to "warning" and disable progress bar
        debug : bool
            set log level to "debug" and disable progress bar (cannot be used
            with quiet)
        """
        batch_process(self, zoom, tile, multi, quiet, debug)

    def execute(self, process_tile):
        """
        Run the Mapchete process.

        Execute, write and return process_tile with data.

        Parameters
        ----------
        process_tile : Tile or tile index tuple
            Member of the process tile pyramid (not necessarily the output
            pyramid, if output has a different metatiling setting)

        Returns
        -------
        BufferedTile
            Process output is stored in the ``data`` attribute. If
            there is no process output, data is ``None`` and there is
            information on the process status in the message attribute.
        """
        if self.config.mode not in ["memory", "continue", "overwrite"]:
            raise ValueError(
                "process mode must be memory, continue or overwrite")
        if isinstance(process_tile, tuple):
            process_tile = self.config.process_pyramid.tile(*process_tile)
        elif isinstance(process_tile, BufferedTile):
            pass
        else:
            raise TypeError("process_tile must be tuple or BufferedTile")
        if process_tile.zoom not in self.config.zoom_levels:
            process_tile.data = self.config.output.empty(process_tile)
            return process_tile
        return self._execute(process_tile)

    def read(self, output_tile):
        """
        Read from written process output.

        Parameters
        ----------
        output_tile : BufferedTile or tile index tuple
            Member of the output tile pyramid (not necessarily the process
            pyramid, if output has a different metatiling setting)

        Returns
        -------
        BufferedTile
            Tile with appended data.
        """
        if self.config.mode not in ["readonly", "continue", "overwrite"]:
            raise ValueError(
                "process mode must be readonly, continue or overwrite")
        if isinstance(output_tile, tuple):
            output_tile = self.config.output_pyramid.tile(*output_tile)
        elif isinstance(output_tile, BufferedTile):
            pass
        else:
            raise TypeError("output_tile must be tuple or BufferedTile")
        return self.config.output.read(output_tile)

    def write(self, process_tile):
        """
        Write data into output format.

        Parameters
        ----------
        process_tile : BufferedTile or tile index tuple
            process tile with appended data
        overwrite : bool
            overwrite existing data (default: True)
        """
        if self.config.mode not in ["continue", "overwrite"]:
            raise ValueError("process mode must be continue or overwrite")
        starttime = time.time()
        if process_tile.data is None or process_tile.message == "empty":
            LOGGER.debug((process_tile.id, "nothing to write"))
        else:
            if self.config.mode == "continue" and (
                self.config.output.tiles_exist(process_tile)
            ):
                LOGGER.debug((process_tile.id, "exists, not overwritten"))
            else:
                self.config.output.write(copy(process_tile))
                elapsed = "%ss" % (round((time.time() - starttime), 3))
                LOGGER.debug((process_tile.id, "output written", elapsed))

    def get_raw_output(self, tile, _baselevel_readonly=False):
        """
        Get output raw data.

        This function won't work with multiprocessing, as it uses the
        ``threading.Lock()`` class.

        Parameters
        ----------
        tile : tuple, Tile or BufferedTile
            If a tile index is given, a tile from the output pyramid will be
            assumed. Tile cannot be bigger than process tile!
        overwrite : bool
            Overwrite existing tiles (default: False)
        no_write : bool
            Never write, just process and cache tiles in RAM (doesn't work with
            multiprocessing; default: False)

        Returns
        -------
        BufferedTile
            output data stored in ``data`` attribute
        """
        if not isinstance(tile, (BufferedTile, tuple)):
            raise ValueError("'tile' must be a tuple or BufferedTile")
        tile = (
            self.config.output_pyramid.tile(*tile)
            if isinstance(tile, tuple)
            else tile
        )
        if _baselevel_readonly:
            tile = self.config.baselevels["tile_pyramid"].tile(*tile.id)
        # Return empty data if zoom level is outside of process zoom levels.
        if tile.zoom not in self.config.zoom_levels:
            tile.data = self.config.output.empty(tile)
            return tile

        # TODO implement reprojection
        if tile.crs != self.config.process_pyramid.crs:
            raise NotImplementedError(
                "tile CRS and process CRS must be the same"
            )

        if self.config.mode == "memory":
            # Determine affected process Tile and check whether it is already
            # cached.
            process_tile = self.config.process_pyramid.intersecting(tile)[0]
            output = self._execute_using_cache(process_tile)
            return self._extract(output, tile)

        # TODO: cases where tile intersects with multiple process tiles
        process_tile = self.config.process_pyramid.intersecting(tile)[0]

        # get output_tiles that intersect with current tile
        if tile.pixelbuffer > self.config.output.pixelbuffer:
            output_tiles = list(self.config.output_pyramid.tiles_from_bounds(
                tile.bounds, tile.zoom
            ))
        else:
            output_tiles = self.config.output_pyramid.intersecting(tile)

        if self.config.mode == "readonly" or _baselevel_readonly:
            if self.config.output.tiles_exist(process_tile):
                return self._read_existing_output(tile, output_tiles)
            else:
                tile.data = self.config.output.empty(tile)
                return tile
        elif self.config.mode == "continue" and not _baselevel_readonly:
            if self.config.output.tiles_exist(process_tile):
                return self._read_existing_output(tile, output_tiles)
            else:
                return self._process_and_overwrite_output(tile, process_tile)
        elif self.config.mode == "overwrite" and not _baselevel_readonly:
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
                [self.read(output_tile) for output_tile in output_tiles]
            )
            tile.data = raster.extract_from_array(mosaic, affine, tile)
        elif self.config.output.METADATA["data_type"] == "vector":
            tile.data = list(chain.from_iterable([
                self.read(output_tile).data for output_tile in output_tiles
            ]))
        return tile

    def _execute_using_cache(self, process_tile):
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
            return self.process_tile_cache[process_tile.id]
        else:
            try:
                output = self.execute(process_tile)
                self.process_tile_cache[process_tile.id] = output
                if self.config.mode in ["continue", "overwrite"]:
                    self.write(output)
                return self.process_tile_cache[process_tile.id]
            finally:
                with self.process_lock:
                    process_event = self.current_processes.get(
                        process_tile.id)
                    del self.current_processes[process_tile.id]
                    process_event.set()

    def _extract(self, process_tile, tile):
        try:
            process_tile = self.process_tile_cache[process_tile.id]
        except Exception:
            pass
        if self.config.output.METADATA["data_type"] == "raster":
            process_tile.data = raster.prepare_array(
                process_tile.data, nodata=self.config.output.nodata,
                dtype=self.config.output.output_params["dtype"]
            )
            tile.data = raster.extract_from_tile(process_tile, tile)
        elif self.config.output.METADATA["data_type"] == "vector":
            tile.data = vector.extract_from_tile(process_tile, tile)
        return tile

    def _execute(self, process_tile):
        # If baselevel is active and zoom is outside of baselevel,
        # interpolate from other zoom levels.
        if self.config.baselevels:
            if process_tile.zoom < min(self.config.baselevels["zooms"]):
                process_data = self._interpolate_from_baselevel(
                    process_tile,
                    "lower"
                )
                # Analyze proess output.
                return self._streamline_output(process_data, process_tile)
            elif process_tile.zoom > max(self.config.baselevels["zooms"]):
                process_data = self._interpolate_from_baselevel(
                    process_tile,
                    "higher"
                )
                # Analyze proess output.
                return self._streamline_output(process_data, process_tile)
        # Otherwise, load process source and execute.
        try:
            new_process = imp.load_source(
                self.process_name + "Process", self.config.process_file)
            tile_process = new_process.Process(
                config=self.config, tile=process_tile,
                params=self.config.at_zoom(process_tile.zoom)
            )
        except ImportError as e:
            raise MapcheteProcessImportError(e)
        try:
            starttime = time.time()
            # Actually run process.
            process_data = tile_process.execute()
            # Log process time
        except Exception as e:
            elapsed = "%ss" % (round((time.time() - starttime), 3))
            LOGGER.error(
                (process_tile.id, "exception in user process", e, elapsed))
            for line in format_exc().split("\n"):
                LOGGER.error(line)
            raise MapcheteProcessException(format_exc())
        finally:
            del tile_process
        elapsed = "%ss" % (round((time.time() - starttime), 3))
        LOGGER.debug((process_tile.id, "processed", elapsed))
        # Analyze proess output.
        return self._streamline_output(process_data, process_tile)

    def _streamline_output(self, process_data, process_tile):
        if isinstance(process_data, str):
            process_tile.data = self.config.output.empty(process_tile)
            process_tile.message = process_data
        elif isinstance(process_data, (np.ndarray, ma.MaskedArray)):
            process_tile.data = process_data.copy()
        elif isinstance(process_data, types.GeneratorType):
            process_tile.data = list(process_data)
        elif isinstance(process_data, list):
            process_tile.data = process_data
        elif not process_data:
            raise MapcheteProcessOutputError("process output is empty")
        else:
            raise MapcheteProcessOutputError(
                "invalid output type: %s" % type(process_data)
            )
        return process_tile

    def _interpolate_from_baselevel(self, process_tile, baselevel):
        try:
            starttime = time.time()
            # resample from parent tile
            if baselevel == "higher":
                parent_tile = self.get_raw_output(
                    process_tile.get_parent(), _baselevel_readonly=True
                )
                process_data = raster.resample_from_array(
                    parent_tile.data,
                    parent_tile.affine,
                    process_tile,
                    self.config.baselevels["higher"],
                    nodataval=self.config.output.nodata
                )
            # resample from children tiles
            elif baselevel == "lower":
                mosaic, mosaic_affine = raster.create_mosaic([
                    self.get_raw_output(child_tile, _baselevel_readonly=True)
                    for child_tile in process_tile.get_children()
                ])
                process_data = raster.resample_from_array(
                    mosaic,
                    mosaic_affine,
                    process_tile,
                    self.config.baselevels["lower"],
                    nodataval=self.config.output.nodata
                )
            elapsed = "%ss" % (round((time.time() - starttime), 3))
            LOGGER.debug((
                process_tile.id, "generated from baselevel", elapsed))
        except Exception as e:
            elapsed = "%ss" % (round((time.time() - starttime), 3))
            LOGGER.error((process_tile.id, "baselevel error", e, elapsed))
            raise
        return process_data

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, t, v, tb):
        """Clear cache on close."""
        if self.with_cache:
            del self.process_tile_cache
            del self.current_processes
            del self.process_lock


class MapcheteProcess(object):
    """
    Process class inherited by user process script.

    Its attributes and methods can be accessed via "self" from within a
    Mapchete process Python file.

    Parameters
    ----------
    tile : BufferedTile
        Tile process should be run on
    config : MapcheteConfig
        process configuration
    params : dictionary
        process parameters

    Attributes
    ----------
    identifier : string
        process identifier
    title : string
        process title
    version : string
        process version string
    abstract : string
        short text describing process purpose
    tile : BufferedTile
        Tile process should be run on
    tile_pyramid : TilePyramid
        process tile pyramid
    params : dictionary
        process parameters
    config : MapcheteConfig
        process configuration
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
        """
        Read existing output data from a previous run.

        Returns
        -------
        process output : NumPy array (raster) or feature iterator (vector)
        """
        with self.config.output.open(self.tile, **kwargs) as existing_tile:
            if existing_tile.is_empty():
                return self.config.output.empty(self.tile)
            else:
                return existing_tile.read(**kwargs)

    def open(self, input_id, **kwargs):
        """
        Open input data.

        Parameters
        ----------
        input_id : string
            input identifier from configuration file or file path
        kwargs : driver specific parameters (e.g. resampling)

        Returns
        -------
        tiled input data : InputTile
            reprojected input data within tile
        """
        if not isinstance(input_id, str):
            return input_id.open(self.tile, **kwargs)
        if input_id not in self.params["input"]:
            raise ValueError(
                "%s not found in config as input file" % input_id)
        return self.params["input"][input_id].open(self.tile, **kwargs)

    def hillshade(
        self, elevation, azimuth=315.0, altitude=45.0, z=1.0, scale=1.0
    ):
        """
        Calculate hillshading from elevation data.

        Parameters
        ----------
        elevation : array
            input elevation data
        azimuth : float
            horizontal angle of light source (315: North-West)
        altitude : float
            vertical angle of light source (90 would result in slope shading)
        z : float
            vertical exaggeration factor
        scale : float
            scale factor of pixel size units versus height units (insert 112000
            when having elevation values in meters in a geodetic projection)

        Returns
        -------
        hillshade : array
        """
        return commons_hillshade.hillshade(
            elevation, self, azimuth, altitude, z, scale)

    def contours(
        self, elevation, interval=100, field='elev', base=0
    ):
        """
        Extract contour lines from elevation data.

        Parameters
        ----------
        elevation : array
            input elevation data
        interval : integer
            elevation value interval when drawing contour lines
        field : string
            output field name containing elevation value
        base : integer
            elevation base value the intervals are computed from

        Returns
        -------
        contours : iterable
            contours as GeoJSON-like pairs of properties and geometry
        """
        return commons_contours.extract_contours(
            elevation, self.tile, interval=interval, field=field, base=base)

    def clip(
        self, array, geometries, inverted=False, clip_buffer=0
    ):
        """
        Clip array by geometry.

        Parameters
        ----------
        array : array
            raster data to be clipped
        geometries : iterable
            geometries used to clip source array
        inverted : bool
            invert clipping (default: False)
        clip_buffer : int
            buffer (in pixels) geometries before applying clip

        Returns
        -------
        clipped array : array
        """
        return commons_clip.clip_array_with_vector(
            array, self.tile.affine, geometries,
            inverted=inverted, clip_buffer=clip_buffer*self.tile.pixel_x_size)
