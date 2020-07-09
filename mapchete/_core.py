"""Main module managing processes."""

from cachetools import LRUCache
import logging
import multiprocessing
import threading

from mapchete.config import MapcheteConfig
from mapchete.errors import MapcheteNodataTile
from mapchete.io import tiles_exist
from mapchete._processing import _run_on_single_tile, _run_area, ProcessInfo, TileProcess
from mapchete.tile import count_tiles
from mapchete._timer import Timer
from mapchete.validate import validate_tile, validate_zooms

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

    def skip_tiles(self, tiles=None):
        """
        Quickly determine whether tiles can be skipped for processing.

        The skip value is True if process mode is 'continue' and process output already
        exists. In all other cases, skip is False.

        Parameters
        ----------
        tiles : list of process tiles

        Yields
        ------
        tuples : (tile, skip)
        """
        # only check for existing output in "continue" mode
        if self.config.mode == "continue":
            yield from tiles_exist(config=self.config, process_tiles=tiles)
        # otherwise don't skip tiles
        else:
            for tile in tiles:
                yield (tile, False)

    def batch_process(
        self,
        zoom=None,
        tile=None,
        multi=None,
        max_chunksize=1,
        multiprocessing_module=None,
        multiprocessing_start_method="fork",
        skip_output_check=False
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
        skip_output_check : bool
            skip checking whether process tiles already have existing output before
            starting to process;
        """
        list(self.batch_processor(
            zoom=zoom,
            tile=tile,
            multi=multi or multiprocessing.cpu_count(),
            max_chunksize=max_chunksize,
            multiprocessing_module=multiprocessing_module or multiprocessing,
            multiprocessing_start_method=multiprocessing_start_method,
            skip_output_check=skip_output_check
        ))

    def batch_processor(
        self,
        zoom=None,
        tile=None,
        multi=None,
        max_chunksize=1,
        multiprocessing_module=None,
        multiprocessing_start_method="fork",
        skip_output_check=False
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
        skip_output_check : bool
            skip checking whether process tiles already have existing output before
            starting to process;
        """
        if zoom and tile:
            raise ValueError("use either zoom or tile")

        # run single tile
        if tile:
            yield _run_on_single_tile(
                process=self,
                tile=self.config.process_pyramid.tile(*tuple(tile))
            )
        # run area
        else:
            for process_info in _run_area(
                process=self,
                zoom_levels=list(_get_zoom_level(zoom, self)),
                multi=multi or multiprocessing.cpu_count(),
                max_chunksize=max_chunksize,
                multiprocessing_module=multiprocessing_module or multiprocessing,
                multiprocessing_start_method=multiprocessing_start_method,
                skip_output_check=skip_output_check
            ):
                yield process_info

    def count_tiles(self, minzoom, maxzoom, init_zoom=0):
        """
        Count number of tiles intersecting with process area at given zoom levels.

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
        Run Mapchete process on a tile.

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
        process_tile = validate_tile(process_tile, self.config.process_pyramid)
        try:
            return self.config.output.streamline_output(
                TileProcess(tile=process_tile, config=self.config).execute()
            )
        except MapcheteNodataTile:
            if raise_nodata:  # pragma: no cover
                raise
            else:
                return self.config.output.empty(process_tile)

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
        output_tile = validate_tile(output_tile, self.config.output_pyramid)
        if self.config.mode not in ["readonly", "continue", "overwrite"]:
            raise ValueError("process mode must be readonly, continue or overwrite")
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
        process_tile = validate_tile(process_tile, self.config.process_pyramid)
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
        tile = validate_tile(tile, self.config.output_pyramid)
        tile = (
            self.config.baselevels["tile_pyramid"].tile(*tile.id)
            if _baselevel_readonly
            else tile
        )

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
            if process_event:  # pragma: no cover
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

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Cleanup on close."""
        # run input drivers cleanup
        for ip in self.config.input.values():
            if ip is not None:
                ip.cleanup()
        # run output driver cleanup
        self.config.output.close(
            exc_type=exc_type, exc_value=exc_value, exc_traceback=exc_traceback
        )
        # clean up internal cache
        if self.with_cache:
            self.process_tile_cache = None
            self.current_processes = None
            self.process_lock = None


def _get_zoom_level(zoom, process):
    """Determine zoom levels."""
    return reversed(process.config.zoom_levels) if zoom is None else validate_zooms(zoom)
