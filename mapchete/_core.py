"""Main module managing processes."""

from cachetools import LRUCache
import inspect
import logging
import multiprocessing
import threading
from traceback import format_exc

from mapchete.commons import clip as commons_clip
from mapchete.commons import contours as commons_contours
from mapchete.commons import hillshade as commons_hillshade
from mapchete.config import MapcheteConfig
from mapchete.errors import (
    MapcheteProcessException, MapcheteProcessOutputError, MapcheteNodataTile
)
from mapchete.io import raster
from mapchete._processing import (
    _run_on_single_tile, _run_without_multiprocessing, _run_with_multiprocessing,
    ProcessInfo
)
from mapchete.tile import BufferedTile, count_tiles
from mapchete._timer import Timer

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
        logger.info("initialize process")
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
        self,
        zoom=None,
        tile=None,
        multi=multiprocessing.cpu_count(),
        max_chunksize=1,
        multiprocessing_module=multiprocessing,
        multiprocessing_start_method="fork"
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
        multiprocessing_module : module
            either Python's standard 'multiprocessing' or Celery's 'billiard' module
            (default: multiprocessing)
        multiprocessing_start_method : str
            "fork", "forkserver" or "spawn"
            (default: "fork")
        """
        list(self.batch_processor(
            zoom=zoom,
            tile=tile,
            multi=multi,
            max_chunksize=max_chunksize,
            multiprocessing_module=multiprocessing_module,
            multiprocessing_start_method=multiprocessing_start_method,
        ))

    def batch_processor(
        self,
        zoom=None,
        tile=None,
        multi=multiprocessing.cpu_count(),
        max_chunksize=1,
        multiprocessing_module=multiprocessing,
        multiprocessing_start_method="fork"
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
        multiprocessing_module : module
            either Python's standard 'multiprocessing' or Celery's 'billiard' module
            (default: multiprocessing)
        multiprocessing_start_method : str
            "fork", "forkserver" or "spawn"
            (default: "fork")
        """
        if zoom and tile:
            raise ValueError("use either zoom or tile")

        # run single tile
        if tile:
            yield _run_on_single_tile(
                process=self,
                tile=self.config.process_pyramid.tile(*tuple(tile))
            )
        # run sequentially
        elif multi == 1:
            for process_info in _run_without_multiprocessing(
                process=self,
                zoom_levels=list(_get_zoom_level(zoom, self))
            ):
                yield process_info
        # run concurrently
        elif multi > 1:
            for process_info in _run_with_multiprocessing(
                process=self,
                zoom_levels=list(_get_zoom_level(zoom, self)),
                multi=multi,
                max_chunksize=max_chunksize,
                multiprocessing_module=multiprocessing_module,
                multiprocessing_start_method=multiprocessing_start_method
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
        return self.config.output.extract_subset(
            input_data_tiles=[
                (output_tile, self.read(output_tile))
                for output_tile in output_tiles
            ],
            out_tile=tile,
        )

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
        return self.config.output.extract_subset(
            input_data_tiles=[(in_tile, in_data)],
            out_tile=out_tile
        )

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
        elif process_data is None:
            raise MapcheteProcessOutputError("process output is empty")
        elif self.config.output.output_is_valid(process_data):
            return self.config.output.output_cleaned(process_data)
        else:
            raise MapcheteProcessOutputError(
                "invalid output type: %s" % type(process_data)
            )

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
        return self.config.output.extract_subset(
            input_data_tiles=[
                (output_tile, self.config.output.read(output_tile))
                for output_tile in output_tiles
            ],
            out_tile=self.tile,
        )

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
        return commons_hillshade.hillshade(elevation, self, azimuth, altitude, z, scale)

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
            elevation, self.tile, interval=interval, field=field, base=base
        )

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
            inverted=inverted, clip_buffer=clip_buffer*self.tile.pixel_x_size
        )


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
