"""Main module managing processes."""

from cachetools import LRUCache
from collections import namedtuple
import concurrent.futures
import inspect
from itertools import chain, product
import logging
from multiprocessing import cpu_count, current_process
import numpy as np
import numpy.ma as ma
from shapely.geometry import shape
import threading
from tilematrix import TilePyramid
import time
from traceback import format_exc
import types

from mapchete.commons import clip as commons_clip
from mapchete.commons import contours as commons_contours
from mapchete.commons import hillshade as commons_hillshade
from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTile
from mapchete.io import raster
from mapchete.errors import (
    MapcheteProcessException, MapcheteProcessOutputError, MapcheteNodataTile
)

logger = logging.getLogger(__name__)


def open(
    config, mode="continue", zoom=None, bounds=None, single_input_file=None,
    with_cache=False, debug=False
):
    """
    Open a Mapchete process.

    Parameters
    ----------
    config : MapcheteConfig object, config dict or path to mapchete file
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
            single_input_file=single_input_file, debug=debug),
        with_cache=with_cache)


ProcessInfo = namedtuple('ProcessInfo', 'tile processed process_msg written write_msg')


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
    with_cache : bool
        process output data cached in memory
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
        logger.debug("initialize process")
        if not isinstance(config, MapcheteConfig):
            raise TypeError("config must be MapcheteConfig object")
        self.config = config
        self.process_name = self.config.process_name
        self.with_cache = True if self.config.mode == "memory" else with_cache
        if self.with_cache:
            self.process_tile_cache = LRUCache(maxsize=512)
            self.current_processes = {}
            self.process_lock = threading.Lock()
        self._count_tiles_cache = {}

    def get_process_tiles(self, zoom=None):
        """
        Yield process tiles.

        Tiles intersecting with the input data bounding boxes as well as
        process bounds, if provided, are considered process tiles. This is to
        avoid iterating through empty tiles.

        Parameters
        ----------
        zoom : integer
            zoom level process tiles should be returned from; if none is given,
            return all process tiles

        yields
        ------
        BufferedTile objects
        """
        if zoom or zoom == 0:
            for tile in self.config.process_pyramid.tiles_from_geom(
                self.config.area_at_zoom(zoom), zoom
            ):
                yield tile
        else:
            for zoom in reversed(self.config.zoom_levels):
                for tile in self.config.process_pyramid.tiles_from_geom(
                    self.config.area_at_zoom(zoom), zoom
                ):
                    yield tile

    def batch_process(
        self, zoom=None, tile=None, multi=cpu_count(), max_chunksize=1
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
        max_chunksize : int
            maximum number of process tiles to be queued for each worker;
            (default: 1)
        """
        list(self.batch_processor(zoom, tile, multi, max_chunksize))

    def batch_processor(
        self, zoom=None, tile=None, multi=cpu_count(), max_chunksize=1
    ):
        """
        Process a large batch of tiles and yield report messages per tile.

        Parameters
        ----------
        zoom : list or int
            either single zoom level or list of minimum and maximum zoom level;
            None processes all (default: None)
        tile : tuple
            zoom, row and column of tile to be processed (cannot be used with
            zoom)
        multi : int
            number of workers (default: number of CPU cores)
        max_chunksize : int
            maximum number of process tiles to be queued for each worker;
            (default: 1)
        """
        if zoom and tile:
            raise ValueError("use either zoom or tile")

        # run single tile
        if tile:
            yield _run_on_single_tile(self, tile)
        # run concurrently
        elif multi > 1:
            for process_info in _run_with_multiprocessing(
                self, list(_get_zoom_level(zoom, self)), multi, max_chunksize
            ):
                yield process_info
        # run sequentially
        elif multi == 1:
            for process_info in _run_without_multiprocessing(
                self, list(_get_zoom_level(zoom, self))
            ):
                yield process_info

    def count_tiles(self, minzoom, maxzoom, init_zoom=0):
        """
        Count number of tiles intersecting with geometry.

        Parameters
        ----------
        geometry : shapely geometry
        pyramid : TilePyramid
        minzoom : int
        maxzoom : int
        init_zoom : int

        Returns
        -------
        number of tiles
        """
        if (minzoom, maxzoom) not in self._count_tiles_cache:
            self._count_tiles_cache[(minzoom, maxzoom)] = count_tiles(
                self.config.area_at_zoom(), self.config.process_pyramid,
                minzoom, maxzoom, init_zoom=0
            )
        return self._count_tiles_cache[(minzoom, maxzoom)]

    def execute(self, process_tile, raise_nodata=False):
        """
        Run the Mapchete process.

        Execute, write and return data.

        Parameters
        ----------
        process_tile : Tile or tile index tuple
            Member of the process tile pyramid (not necessarily the output
            pyramid, if output has a different metatiling setting)

        Returns
        -------
        data : NumPy array or features
            process output
        """
        if self.config.mode not in ["memory", "continue", "overwrite"]:
            raise ValueError("process mode must be memory, continue or overwrite")
        if isinstance(process_tile, tuple):
            process_tile = self.config.process_pyramid.tile(*process_tile)
        elif isinstance(process_tile, BufferedTile):
            pass
        else:
            raise TypeError("process_tile must be tuple or BufferedTile")

        if process_tile.zoom not in self.config.zoom_levels:
            return self.config.output.empty(process_tile)

        return self._execute(process_tile, raise_nodata=raise_nodata)

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
        data : NumPy array or features
            process output
        """
        if self.config.mode not in ["readonly", "continue", "overwrite"]:
            raise ValueError("process mode must be readonly, continue or overwrite")
        if isinstance(output_tile, tuple):
            output_tile = self.config.output_pyramid.tile(*output_tile)
        elif isinstance(output_tile, BufferedTile):
            pass
        else:
            raise TypeError("output_tile must be tuple or BufferedTile")

        return self.config.output.read(output_tile)

    def write(self, process_tile, data):
        """
        Write data into output format.

        Parameters
        ----------
        process_tile : BufferedTile or tile index tuple
            process tile
        data : NumPy array or features
            data to be written
        """
        if isinstance(process_tile, tuple):
            process_tile = self.config.process_pyramid.tile(*process_tile)
        elif not isinstance(process_tile, BufferedTile):
            raise ValueError("invalid process_tile type: %s" % type(process_tile))
        if self.config.mode not in ["continue", "overwrite"]:
            raise ValueError("cannot write output in current process mode")

        if self.config.mode == "continue" and (
            self.config.output.tiles_exist(process_tile)
        ):
            message = "output exists, not overwritten"
            logger.debug((process_tile.id, message))
            return ProcessInfo(
                tile=process_tile,
                processed=False,
                process_msg=None,
                written=False,
                write_msg=message
            )
        elif data is None:
            message = "output empty, nothing written"
            logger.debug((process_tile.id, message))
            return ProcessInfo(
                tile=process_tile,
                processed=False,
                process_msg=None,
                written=False,
                write_msg=message
            )
        else:
            with Timer() as t:
                self.config.output.write(process_tile=process_tile, data=data)
            message = "output written in %s" % t
            logger.debug((process_tile.id, message))
            return ProcessInfo(
                tile=process_tile,
                processed=False,
                process_msg=None,
                written=True,
                write_msg=message
            )

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

        Returns
        -------
        data : NumPy array or features
            process output
        """
        if not isinstance(tile, (BufferedTile, tuple)):
            raise TypeError("'tile' must be a tuple or BufferedTile")
        if isinstance(tile, tuple):
            tile = self.config.output_pyramid.tile(*tile)
        if _baselevel_readonly:
            tile = self.config.baselevels["tile_pyramid"].tile(*tile.id)

        # Return empty data if zoom level is outside of process zoom levels.
        if tile.zoom not in self.config.zoom_levels:
            return self.config.output.empty(tile)

        # TODO implement reprojection
        if tile.crs != self.config.process_pyramid.crs:
            raise NotImplementedError(
                "reprojection between processes not yet implemented"
            )

        if self.config.mode == "memory":
            # Determine affected process Tile and check whether it is already
            # cached.
            process_tile = self.config.process_pyramid.intersecting(tile)[0]
            return self._extract(
                in_tile=process_tile,
                in_data=self._execute_using_cache(process_tile),
                out_tile=tile
            )

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
                return self.config.output.empty(tile)
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
        self.write(process_tile, output)
        return self._extract(
            in_tile=process_tile,
            in_data=output,
            out_tile=tile
        )

    def _read_existing_output(self, tile, output_tiles):
        if self.config.output.METADATA["data_type"] == "raster":
            mosaic = raster.create_mosaic([
                (output_tile, self.read(output_tile))
                for output_tile in output_tiles
            ])
            return raster.extract_from_array(mosaic.data, mosaic.affine, tile)
        elif self.config.output.METADATA["data_type"] == "vector":
            return list(chain.from_iterable([
                self.read(output_tile) for output_tile in output_tiles
            ]))

    def _execute_using_cache(self, process_tile):
        # Extract Tile subset from process Tile and return.
        try:
            return self.process_tile_cache[process_tile.id]
        except KeyError:
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
                        self.write(process_tile, output)
                    return self.process_tile_cache[process_tile.id]
                finally:
                    with self.process_lock:
                        process_event = self.current_processes.get(
                            process_tile.id)
                        del self.current_processes[process_tile.id]
                        process_event.set()

    def _extract(self, in_tile=None, in_data=None, out_tile=None):
        """Extract data from tile."""
        if self.config.output.METADATA["data_type"] == "raster":
            return raster.extract_from_array(
                in_raster=raster.prepare_array(
                    in_data, nodata=self.config.output.nodata,
                    dtype=self.config.output.output_params["dtype"]
                ),
                in_affine=in_tile.affine,
                out_tile=out_tile
            )
        elif self.config.output.METADATA["data_type"] == "vector":
            return [
                feature
                for feature in in_data
                if shape(feature["geometry"]).intersects(out_tile.bbox)
            ]

    def _execute(self, process_tile, raise_nodata=False):
        # If baselevel is active and zoom is outside of baselevel,
        # interpolate from other zoom levels.
        if self.config.baselevels:
            if process_tile.zoom < min(self.config.baselevels["zooms"]):
                return self._streamline_output(
                    self._interpolate_from_baselevel(process_tile, "lower")
                )
            elif process_tile.zoom > max(self.config.baselevels["zooms"]):
                return self._streamline_output(
                    self._interpolate_from_baselevel(process_tile, "higher")
                )
        # Otherwise, execute from process file.
        try:
            with Timer() as t:
                # Actually run process.
                process_data = self.config.process_func(
                    MapcheteProcess(config=self.config, tile=process_tile),
                    # only pass on kwargs which are defined in execute()
                    **{
                        k: v
                        for k, v in self.config.params_at_zoom(process_tile.zoom).items()
                        if k in inspect.signature(self.config.process_func).parameters
                    }
                )
        except Exception as e:
            # Log process time
            logger.exception(
                (process_tile.id, "exception in user process", e, str(t))
            )
            new = MapcheteProcessException(format_exc())
            new.old = e
            raise new
        # Analyze proess output.
        if raise_nodata:
            return self._streamline_output(process_data)
        else:
            try:
                return self._streamline_output(process_data)
            except MapcheteNodataTile:
                return self.config.output.empty(process_tile)

    def _streamline_output(self, process_data):
        if isinstance(process_data, str) and (
            process_data == "empty"
        ):
            raise MapcheteNodataTile
        elif isinstance(process_data, (np.ndarray, ma.MaskedArray)):
            return process_data
        elif isinstance(process_data, (list, types.GeneratorType)):
            return list(process_data)
        # for data, metadata tuples
        elif (
            isinstance(process_data, tuple) and
            len(process_data) == 2 and
            isinstance(process_data[1], dict)
        ):
            data, metadata = process_data
            return self._streamline_output(data), metadata
        elif not process_data:
            raise MapcheteProcessOutputError("process output is empty")
        else:
            raise MapcheteProcessOutputError(
                "invalid output type: %s" % type(process_data))

    def _interpolate_from_baselevel(self, tile=None, baselevel=None):
        with Timer() as t:
            # resample from parent tile
            if baselevel == "higher":
                parent_tile = tile.get_parent()
                process_data = raster.resample_from_array(
                    in_raster=self.get_raw_output(parent_tile, _baselevel_readonly=True),
                    in_affine=parent_tile.affine,
                    out_tile=tile,
                    resampling=self.config.baselevels["higher"],
                    nodataval=self.config.output.nodata
                )
            # resample from children tiles
            elif baselevel == "lower":
                mosaic = raster.create_mosaic([
                    (
                        child_tile,
                        self.get_raw_output(child_tile, _baselevel_readonly=True)
                    )
                    for child_tile in self.config.baselevels["tile_pyramid"].tile(
                        *tile.id
                    ).get_children()
                ])
                process_data = raster.resample_from_array(
                    in_raster=mosaic.data,
                    in_affine=mosaic.affine,
                    out_tile=tile,
                    resampling=self.config.baselevels["lower"],
                    nodataval=self.config.output.nodata
                )
        logger.debug((tile.id, "generated from baselevel", str(t)))
        return process_data

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, t, v, tb):
        """Cleanup on close."""
        for ip in self.config.input.values():
            if ip is not None:
                ip.cleanup()
        if self.with_cache:
            self.process_tile_cache = None
            self.current_processes = None
            self.process_lock = None


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
        self.params = params if params else config.params_at_zoom(tile.zoom)
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
        if self.tile.pixelbuffer > self.config.output.pixelbuffer:
            output_tiles = list(self.config.output_pyramid.tiles_from_bounds(
                self.tile.bounds, self.tile.zoom
            ))
        else:
            output_tiles = self.config.output_pyramid.intersecting(self.tile)
        if self.config.output.METADATA["data_type"] == "raster":
            return raster.extract_from_array(
                in_raster=raster.create_mosaic([
                    (output_tile, self.config.output.read(output_tile))
                    for output_tile in output_tiles
                ]),
                out_tile=self.tile
            )
        elif self.config.output.METADATA["data_type"] == "vector":
            return list(chain.from_iterable([
                self.config.output.read(output_tile)
                for output_tile in output_tiles
            ]))

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
            raise ValueError("%s not found in config as input file" % input_id)
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


class Timer:
    """
    Context manager to facilitate timing code.

    based on http://preshing.com/20110924/timing-your-code-using-pythons-with-statement/
    """
    def __init__(self, elapsed=0., str_round=3):
        self._elapsed = elapsed
        self._str_round = str_round
        self.start = None
        self.end = None

    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self._elapsed = self.end - self.start

    def __lt__(self, other):
        return self._elapsed < other._elapsed

    def __le__(self, other):
        return self._elapsed <= other._elapsed

    def __eq__(self, other):
        return self._elapsed == other._elapsed

    def __ne__(self, other):
        return self._elapsed != other._elapsed

    def __ge__(self, other):
        return self._elapsed >= other._elapsed

    def __gt__(self, other):
        return self._elapsed > other._elapsed

    def __add__(self, other):
        return Timer(elapsed=self._elapsed + other._elapsed)

    def __sub__(self, other):
        return Timer(elapsed=self._elapsed - other._elapsed)

    def __repr__(self):
        return "Timer(start=%s, end=%s, elapsed=%s)" % (
            self.start, self.end, self.__str__()
        )

    def __str__(self):
        minutes, seconds = divmod(self.elapsed, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return "%sh %sm %ss" % (int(hours), int(minutes), int(seconds))
        elif minutes:
            return "%sm %ss" % (int(minutes), int(seconds))
        else:
            return "%ss" % round(seconds, self._str_round)

    @property
    def elapsed(self):
        return time.clock() - self.start if self.start and not self.end else self._elapsed


def count_tiles(geometry, pyramid, minzoom, maxzoom, init_zoom=0):
    """
    Count number of tiles intersecting with geometry.

    Parameters
    ----------
    geometry : shapely geometry
    pyramid : TilePyramid
    minzoom : int
    maxzoom : int
    init_zoom : int

    Returns
    -------
    number of tiles
    """
    if not 0 <= init_zoom <= minzoom <= maxzoom:
        raise ValueError("invalid zoom levels given")
    # tile buffers are not being taken into account
    unbuffered_pyramid = TilePyramid(
        pyramid.grid, tile_size=pyramid.tile_size,
        metatiling=pyramid.metatiling
    )
    # make sure no rounding errors occur
    geometry = geometry.buffer(-0.000000001)
    return _count_tiles(
        [
            unbuffered_pyramid.tile(*tile_id)
            for tile_id in product(
                [init_zoom],
                range(pyramid.matrix_height(init_zoom)),
                range(pyramid.matrix_width(init_zoom))
            )
        ], geometry, minzoom, maxzoom
    )


def _count_tiles(tiles, geometry, minzoom, maxzoom):
    count = 0
    for tile in tiles:
        # determine data covered by tile
        tile_intersection = tile.bbox().intersection(geometry)

        # skip if there is no data
        if tile_intersection.is_empty:
            continue
        # increase counter as tile contains data
        elif tile.zoom >= minzoom:
            count += 1

        # if there are further zoom levels, analyze descendants
        if tile.zoom < maxzoom:

            # if tile is half full, analyze each descendant
            if tile_intersection.area < tile.bbox().area:
                count += _count_tiles(
                    tile.get_children(), tile_intersection, minzoom, maxzoom
                )

            # if tile is full, all of its descendants will be full as well
            else:
                # sum up tiles for each remaining zoom level
                count += sum([
                    4**z for z in range(
                        # only count zoom levels which are greater than minzoom or
                        # count all zoom levels from tile zoom level to maxzoom
                        minzoom - tile.zoom if tile.zoom < minzoom else 1,
                        maxzoom - tile.zoom + 1
                    )
                ])

    return count


# helper functions for batch_processor #
########################################
def _run_on_single_tile(process, tile):
    logger.debug("run process on single tile")
    process_info = _process_worker(
        process, process.config.process_pyramid.tile(*tuple(tile))
    )
    return process_info


def _run_with_multiprocessing(process, zoom_levels, multi, max_chunksize):
    logger.debug("run concurrently")
    num_processed = 0
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    logger.debug("run process on %s tiles using %s workers", total_tiles, multi)
    with Timer() as t:
        logger.debug("run process on %s tiles using %s workers", total_tiles, multi)
        with concurrent.futures.ProcessPoolExecutor(max_workers=multi) as executor:
            for zoom in zoom_levels:
                for task in concurrent.futures.as_completed((
                    executor.submit(_process_worker, process, process_tile)
                    for process_tile in process.get_process_tiles(zoom)
                )):
                    num_processed += 1
                    logger.debug("tile %s/%s finished", num_processed, total_tiles)
                    yield task.result()
    logger.debug("%s tile(s) iterated in %s", str(num_processed), t)


def _run_without_multiprocessing(process, zoom_levels):
    logger.debug("run sequentially")
    num_processed = 0
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    logger.debug("run process on %s tiles using 1 worker", total_tiles)
    with Timer() as t:
        for zoom in zoom_levels:
            for process_tile in process.get_process_tiles(zoom):
                process_info = _process_worker(process, process_tile)
                num_processed += 1
                logger.debug("tile %s/%s finished", num_processed, total_tiles)
                yield process_info
    logger.debug("%s tile(s) iterated in %s", str(num_processed), t)


def _get_zoom_level(zoom, process):
    """Determine zoom levels."""
    if zoom is None:
        return reversed(process.config.zoom_levels)
    if isinstance(zoom, int):
        return [zoom]
    elif len(zoom) == 2:
        return reversed(range(min(zoom), max(zoom)+1))
    elif len(zoom) == 1:
        return zoom


def _process_worker(process, process_tile):
    """Worker function running the process."""
    logger.debug((process_tile.id, "running on %s" % current_process().name))

    # skip execution if overwrite is disabled and tile exists
    if (
        process.config.mode == "continue" and
        process.config.output.tiles_exist(process_tile)
    ):
        logger.debug((process_tile.id, "tile exists, skipping"))
        return ProcessInfo(
            tile=process_tile,
            processed=False,
            process_msg="output already exists",
            written=False,
            write_msg="nothing written"
        )

    # execute on process tile
    else:
        with Timer() as t:
            try:
                output = process.execute(process_tile, raise_nodata=True)
            except MapcheteNodataTile:
                output = None
        processor_message = "processed in %s" % t
        logger.debug((process_tile.id, processor_message))
        writer_info = process.write(process_tile, output)
        return ProcessInfo(
            tile=process_tile,
            processed=True,
            process_msg=processor_message,
            written=writer_info.written,
            write_msg=writer_info.write_msg
        )
