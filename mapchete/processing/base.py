"""Main module managing processes."""

import json
import logging
import multiprocessing
import threading
from typing import Any, Iterator, List, Optional, Tuple, Union

from cachetools import LRUCache

from mapchete.config import MapcheteConfig
from mapchete.enums import Concurrency
from mapchete.errors import MapcheteNodataTile, ReprojectionFailed
from mapchete.executor import (
    MULTIPROCESSING_DEFAULT_START_METHOD,
    ExecutorBase,
    MFuture,
)
from mapchete.executor.base import func_partial
from mapchete.executor.types import Profiler
from mapchete.path import batch_sort_property, tiles_exist
from mapchete.processing.compute import (
    PreprocessingProcessInfo,
    TileProcessInfo,
    _execute,
    _execute_and_write,
    _filter_skipable,
    _preprocess,
    _preprocess_task_wrapper,
    _run_area,
    _run_on_single_tile,
    compute,
)
from mapchete.processing.tasks import TaskBatch, TileTask, TileTaskBatch
from mapchete.stac import tile_direcotry_item_to_dict, update_tile_directory_stac_item
from mapchete.tile import BufferedTile, count_tiles
from mapchete.timer import Timer
from mapchete.types import TileLike, ZoomLevels, ZoomLevelsLike
from mapchete.validate import validate_tile

logger = logging.getLogger(__name__)


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

    def __init__(self, config: MapcheteConfig, with_cache: bool = False):
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
        self.with_cache = True if self.config.mode == "memory" else with_cache
        if self.with_cache:
            self.process_tile_cache = LRUCache(maxsize=512)
            self.current_processes = {}
            self.process_lock = threading.Lock()
        self._count_tiles_cache = {}

    def get_process_tiles(
        self, zoom: Optional[int] = None, batch_by: Optional[str] = None
    ) -> Iterator[BufferedTile]:
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
        batch_by : str
            if not None, tiles can be yielded in batches either by "row" or "column".

        yields
        ------
        BufferedTile objects
        """
        logger.debug("get process tiles...")
        if zoom or zoom == 0:
            for tile in self.config.process_pyramid.tiles_from_geom(
                self.config.area_at_zoom(zoom),
                zoom=zoom,
                batch_by=batch_by,
                exact=True,
            ):
                yield tile
        else:
            for i in self.config.zoom_levels.descending():
                for tile in self.config.process_pyramid.tiles_from_geom(
                    self.config.area_at_zoom(i), zoom=i, batch_by=batch_by, exact=True
                ):
                    yield tile

    def skip_tiles(
        self,
        tiles: Iterator[BufferedTile] = None,
        tiles_batches: Iterator[Iterator[BufferedTile]] = None,
    ) -> Iterator[Tuple[BufferedTile, bool]]:
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
        logger.debug("determine which tiles to skip...")
        # only check for existing output in "continue" mode
        if self.config.mode == "continue":
            yield from tiles_exist(
                config=self.config,
                process_tiles=tiles,
                process_tiles_batches=tiles_batches,
            )
        # otherwise don't skip tiles
        else:
            if tiles_batches:
                for batch in tiles_batches:
                    for tile in batch:
                        yield (tile, False)
            else:
                for tile in tiles:
                    yield (tile, False)

    def task_batches(
        self,
        zoom: Optional[ZoomLevelsLike] = None,
        tile: Optional[TileLike] = None,
        skip_output_check: bool = False,
        propagate_results: bool = True,
        profilers: Optional[List[Profiler]] = None,
    ) -> Iterator[Union[TaskBatch, TileTaskBatch]]:
        """
        Generate task batches from preprocessing tasks and tile tasks.
        """
        yield from _task_batches(
            self,
            zoom=zoom,
            tile=tile,
            skip_output_check=skip_output_check,
            propagate_results=propagate_results,
            profilers=profilers,
        )

    def compute(
        self,
        zoom: Optional[ZoomLevelsLike] = None,
        tile: Optional[TileLike] = None,
        executor: Optional[ExecutorBase] = None,
        concurrency: Concurrency = Concurrency.processes,
        workers: int = multiprocessing.cpu_count(),
        multiprocessing_start_method: Optional[str] = None,
        skip_output_check: bool = False,
        dask_scheduler: Optional[str] = None,
        dask_compute_graph: bool = True,
        dask_propagate_results: bool = True,
        dask_max_submitted_tasks: bool = 500,
        raise_errors: bool = True,
        with_results: bool = False,
        **kwargs,
    ) -> Iterator[MFuture]:
        """Compute preprocessing tasks and tile tasks in one go."""
        yield from compute(
            self,
            zoom_levels=(
                self.config.zoom_levels if zoom is None else ZoomLevels.from_inp(zoom)
            ),
            tile=self.config.process_pyramid.tile(*tile) if tile else None,
            executor=executor,
            concurrency=concurrency,
            workers=workers,
            multiprocessing_start_method=multiprocessing_start_method,
            skip_output_check=skip_output_check,
            dask_scheduler=dask_scheduler,
            dask_compute_graph=dask_compute_graph,
            dask_propagate_results=dask_propagate_results,
            dask_max_submitted_tasks=dask_max_submitted_tasks,
            raise_errors=raise_errors,
            with_results=with_results,
            **kwargs,
        )

    def batch_preprocessor(
        self,
        dask_scheduler: Optional[str] = None,
        dask_max_submitted_tasks: int = 500,
        dask_chunksize: int = 100,
        workers: Optional[int] = None,
        executor: Optional[ExecutorBase] = None,
    ) -> Iterator[MFuture]:
        """
        Run all required preprocessing steps and yield over results.

        The task count can be determined by self.config.preprocessing_tasks_count().

        Parameters
        ----------

        dask_schedulter : str
            URL to a dask scheduler if distributed execution is desired
        dask_max_submitted_tasks : int
            Make sure that not more tasks are submitted to dask scheduler at once. (default: 500)
        dask_chunksize : int
            Number of tasks submitted to the scheduler at once. (default: 100)
        workers : int
            number of workers to be used for local processing
        executor : mapchete.Executor
            optional executor class to be used for processing

        """
        # process everything using executor and yield from results
        yield from _preprocess(
            self.config.preprocessing_tasks(),
            process=self,
            dask_scheduler=dask_scheduler,
            dask_max_submitted_tasks=dask_max_submitted_tasks,
            dask_chunksize=dask_chunksize,
            workers=workers,
            executor=executor,
        )

    def batch_preprocess(
        self,
        dask_scheduler: Optional[str] = None,
        dask_max_submitted_tasks: int = 500,
        dask_chunksize: int = 100,
        workers: Optional[int] = None,
        executor: Optional[ExecutorBase] = None,
    ) -> None:
        """
        Run all required preprocessing steps.

        Parameters
        ----------

        dask_schedulter : str
            URL to a dask scheduler if distributed execution is desired
        dask_max_submitted_tasks : int
            Make sure that not more tasks are submitted to dask scheduler at once. (default: 500)
        dask_chunksize : int
            Number of tasks submitted to the scheduler at once. (default: 100)
        workers : int
            number of workers to be used for local processing
        executor : mapchete.Executor
            optional executor class to be used for processing
        """
        list(
            self.batch_preprocessor(
                dask_scheduler=dask_scheduler,
                dask_max_submitted_tasks=dask_max_submitted_tasks,
                dask_chunksize=dask_chunksize,
                workers=workers,
                executor=executor,
            )
        )

    def batch_process(
        self,
        zoom: Optional[ZoomLevelsLike] = None,
        tile: Optional[TileLike] = None,
        dask_scheduler: Optional[str] = None,
        dask_max_submitted_tasks: int = 500,
        dask_chunksize: int = 100,
        workers: Optional[int] = None,
        multiprocessing_start_method: str = MULTIPROCESSING_DEFAULT_START_METHOD,
        skip_output_check: bool = False,
        executor: Optional[ExecutorBase] = None,
    ) -> None:
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
        workers : int
            number of workers (default: number of CPU cores)
        dask_schedulter : str
            URL to a dask scheduler if distributed execution is desired
        dask_max_submitted_tasks : int
            Make sure that not more tasks are submitted to dask scheduler at once. (default: 500)
        dask_chunksize : int
            Number of tasks submitted to the scheduler at once. (default: 100)
        multiprocessing_start_method : str
            "fork", "forkserver" or "spawn"
            (default: "spawn")
        skip_output_check : bool
            skip checking whether process tiles already have existing output before
            starting to process;
        executor : mapchete.Executor
            optional executor class to be used for processing
        """
        list(
            self.batch_processor(
                zoom=zoom,
                tile=tile,
                dask_scheduler=dask_scheduler,
                dask_max_submitted_tasks=dask_max_submitted_tasks,
                dask_chunksize=dask_chunksize,
                workers=workers,
                multiprocessing_start_method=multiprocessing_start_method,
                skip_output_check=skip_output_check,
                executor=executor,
            )
        )

    def batch_processor(
        self,
        zoom: Optional[ZoomLevelsLike] = None,
        tile: Optional[TileLike] = None,
        dask_scheduler: Optional[str] = None,
        dask_max_submitted_tasks: int = 500,
        dask_chunksize: int = 100,
        workers: Optional[int] = None,
        multiprocessing_start_method: str = MULTIPROCESSING_DEFAULT_START_METHOD,
        skip_output_check: bool = False,
        executor: Optional[ExecutorBase] = None,
    ) -> Union[PreprocessingProcessInfo, TileProcessInfo]:
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
        dask_schedulter : str
            URL to a dask scheduler if distributed execution is desired
        dask_max_submitted_tasks : int
            Make sure that not more tasks are submitted to dask scheduler at once. (default: 500)
        dask_chunksize : int
            Number of tasks submitted to the scheduler at once. (default: 100)
        multiprocessing_start_method : str
            "fork", "forkserver" or "spawn"
            (default: "spawn")
        skip_output_check : bool
            skip checking whether process tiles already have existing output before
            starting to process;
        executor : mapchete.Executor
            optional executor class to be used for processing
        """
        if zoom and tile:
            raise ValueError("use either zoom or tile")

        # run single tile
        if tile:
            yield _run_on_single_tile(
                executor=executor,
                dask_scheduler=dask_scheduler,
                process=self,
                tile=self.config.process_pyramid.tile(*tuple(tile)),
            ).result()
        # run area
        else:
            for future in _run_area(
                process=self,
                zoom_levels=self.config.zoom_levels
                if zoom is None
                else ZoomLevels.from_inp(zoom),
                dask_scheduler=dask_scheduler,
                dask_max_submitted_tasks=dask_max_submitted_tasks,
                dask_chunksize=dask_chunksize,
                workers=workers or multiprocessing.cpu_count(),
                multiprocessing_start_method=multiprocessing_start_method,
                skip_output_check=skip_output_check,
                executor=executor,
            ):
                yield future.result()

    def count_tasks(
        self,
        minzoom: Optional[int] = None,
        maxzoom: Optional[int] = None,
        init_zoom: int = 0,
    ) -> int:
        """
        Count all preprocessing tasks and tiles at given zoom levels.

        Parameters
        ----------
        minzoom : int
            limits minimum process zoom
        maxzoom : int
            limits maximum process zoom
        init_zoom : int
            initial zoom level used for tile count algorithm

        Returns
        -------
        number of tasks
        """
        return self.config.preprocessing_tasks_count() + self.count_tiles(
            minzoom=minzoom, maxzoom=maxzoom, init_zoom=init_zoom
        )

    def count_tiles(
        self,
        minzoom: Optional[int] = None,
        maxzoom: Optional[int] = None,
        init_zoom: int = 0,
    ) -> int:
        """
        Count number of tiles intersecting with process area at given zoom levels.

        Parameters
        ----------
        minzoom : int
            limits minimum process zoom
        maxzoom : int
            limits maximum process zoom
        init_zoom : int
            initial zoom level used for tile count algorithm

        Returns
        -------
        number of tiles
        """
        minzoom = self.config.init_zoom_levels.min if minzoom is None else minzoom
        maxzoom = self.config.init_zoom_levels.max if maxzoom is None else maxzoom
        if (minzoom, maxzoom) not in self._count_tiles_cache:
            logger.debug("counting tiles...")
            with Timer() as t:
                self._count_tiles_cache[(minzoom, maxzoom)] = count_tiles(
                    self.config.area_at_zoom(),
                    self.config.process_pyramid,
                    minzoom,
                    maxzoom,
                    init_zoom=init_zoom,
                )
            logger.debug("tiles counted in %s", t)
        return self._count_tiles_cache[(minzoom, maxzoom)]

    def execute(self, process_tile: BufferedTile, raise_nodata: bool = False) -> Any:
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
        self.batch_preprocess()
        try:
            return self.config.output.streamline_output(
                TileTask(tile=process_tile, config=self.config).execute()
            )
        except MapcheteNodataTile:
            if raise_nodata:  # pragma: no cover
                raise
            return self.config.output.empty(process_tile)

    def read(self, output_tile: TileLike) -> Any:
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

    def write(self, process_tile: TileLike, data: Any):
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
            return TileProcessInfo(
                tile=process_tile,
                processed=False,
                process_msg=None,
                written=False,
                write_msg=message,
            )
        elif data is None:
            message = "output empty, nothing written"
            logger.debug((process_tile.id, message))
            return TileProcessInfo(
                tile=process_tile,
                processed=False,
                process_msg=None,
                written=False,
                write_msg=message,
            )
        else:
            with Timer() as t:
                self.config.output.write(process_tile=process_tile, data=data)
            message = "output written in %s" % t
            logger.debug((process_tile.id, message))
            return TileProcessInfo(
                tile=process_tile,
                processed=False,
                process_msg=None,
                written=True,
                write_msg=message,
            )

    def get_raw_output(self, tile: TileLike, _baselevel_readonly: bool = False) -> Any:
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
                out_tile=tile,
            )

        # TODO: cases where tile intersects with multiple process tiles
        process_tile = self.config.process_pyramid.intersecting(tile)[0]

        # get output_tiles that intersect with current tile
        if tile.pixelbuffer > self.config.output.pixelbuffer:
            output_tiles = list(
                self.config.output_pyramid.tiles_from_bounds(tile.bounds, tile.zoom)
            )
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

    def write_stac(self, indent: int = 4) -> None:
        """
        Create or update existing STAC JSON file.

        On TileDirectory outputs, the tiled-assets are updated regarding
        zoom levels and bounds.
        """
        try:
            import pystac
        except ImportError:  # pragma: no cover
            logger.warning("install extra mapchete[stac] to write STAC item files")
            return

        if not self.config.output.use_stac or self.config.mode in [
            "readonly",
            "memory",
        ]:
            return
        # read existing STAC file
        try:
            with self.config.output.stac_path.open("r") as src:
                item = pystac.read_dict(json.loads(src.read()))
        except FileNotFoundError:
            item = None
        try:
            item = update_tile_directory_stac_item(
                item=item,
                item_path=str(self.config.output.stac_path),
                item_id=self.config.output.stac_item_id,
                zoom_levels=self.config.init_zoom_levels,
                bounds=self.config.effective_bounds,
                item_metadata=self.config.output.stac_item_metadata,
                tile_pyramid=self.config.output_pyramid,
                bands_type=self.config.output.stac_asset_type,
                band_asset_template=self.config.output.tile_path_schema,
            )
            logger.debug("write STAC item JSON to %s", self.config.output.stac_path)
            self.config.output.stac_path.parent.makedirs()
            with self.config.output.stac_path.open("w") as dst:
                dst.write(json.dumps(tile_direcotry_item_to_dict(item), indent=indent))
        except ReprojectionFailed:
            logger.warning(
                "cannot create STAC item because footprint cannot be reprojected into EPSG:4326"
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("cannot create or update STAC item: %s", str(exc))

    def _process_and_overwrite_output(self, tile, process_tile):
        if self.with_cache:
            output = self._execute_using_cache(process_tile)
        else:
            output = self.execute(process_tile)

        self.write(process_tile, output)
        return self._extract(in_tile=process_tile, in_data=output, out_tile=tile)

    def _read_existing_output(self, tile, output_tiles):
        return self.config.output.extract_subset(
            input_data_tiles=[
                (output_tile, self.read(output_tile)) for output_tile in output_tiles
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
                        process_event = self.current_processes.get(process_tile.id)
                        del self.current_processes[process_tile.id]
                        process_event.set()

    def _extract(self, in_tile=None, in_data=None, out_tile=None):
        """Extract data from tile."""
        return self.config.output.extract_subset(
            input_data_tiles=[(in_tile, in_data)], out_tile=out_tile
        )

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, exc_type=None, exc_value=None, exc_traceback=None):
        """Cleanup on close."""
        # write/update STAC metadata
        if exc_type is None:
            self.write_stac()
        # run input drivers cleanup
        for ip in self.config.input.values():
            if ip is not None:
                logger.debug(f"running cleanup on {ip}...")
                ip.cleanup()
        # run output driver cleanup
        logger.debug(f"closing output driver {self.config.output}...")
        # HINT: probably cleaner to use the outputs __exit__ function and use a contextmanager interface
        self.config.output.close(
            exc_type=exc_type, exc_value=exc_value, exc_traceback=exc_traceback
        )
        # clean up internal cache
        if self.with_cache:
            self.process_tile_cache = None
            self.current_processes = None
            self.process_lock = None

    def __repr__(self):  # pragma: no cover
        return f"Mapchete <process_name={self.config.process.name}>"


def _task_batches(
    process: Mapchete,
    zoom: Optional[ZoomLevelsLike] = None,
    tile: Optional[TileLike] = None,
    skip_output_check: bool = False,
    propagate_results: bool = True,
    profilers: Optional[List[Profiler]] = None,
) -> Iterator[Union[TaskBatch, TileTaskBatch]]:
    """Create task batches for each processing stage."""
    profilers = profilers or []
    with Timer() as duration:
        # preprocessing tasks
        yield TaskBatch(
            id="preprocessing_tasks",
            tasks=process.config.preprocessing_tasks().values(),
            func=_preprocess_task_wrapper,
            profilers=profilers,
        )
    logger.debug("preprocessing tasks batch generated in %s", duration)
    with Timer() as duration:
        if tile:
            zoom_levels = ZoomLevels.from_inp(tile.zoom)
            skip_output_check = True
            tiles = {tile.zoom: [(tile, False)]}
        else:
            zoom_levels = (
                process.config.zoom_levels
                if zoom is None
                else ZoomLevels.from_inp(zoom)
            )
            tiles = {}

            # here we store the parents of tiles about to be processed so we can update overviews
            # also in "continue" mode in case there were updates at the baselevel
            overview_parents = set()
            for i, zoom in enumerate(zoom_levels.descending()):
                tiles[zoom] = []

                for tile, skip, _ in _filter_skipable(
                    process=process,
                    tiles_batches=process.get_process_tiles(
                        zoom,
                        batch_by=batch_sort_property(
                            process.config.output_reader.tile_path_schema
                        ),
                    ),
                    target_set=(
                        overview_parents if process.config.baselevels and i else None
                    ),
                    skip_output_check=skip_output_check,
                ):
                    tiles[zoom].append((tile, skip))
                    # in case of building overviews from baselevels, remember which parent
                    # tile needs to be updated later on
                    if (
                        not skip_output_check
                        and process.config.baselevels
                        and tile.zoom > 0
                    ):
                        # add parent tile
                        overview_parents.add(tile.get_parent())
                        # we don't need the current tile anymore
                    overview_parents.discard(tile)

        if process.config.output.write_in_parent_process:
            func = func_partial(_execute, profilers=profilers)
            fkwargs = dict(append_data=propagate_results)
        else:
            func = func_partial(_execute_and_write, profilers=profilers)
            fkwargs = dict(
                append_data=propagate_results, output_writer=process.config.output
            )

        # tile tasks
        for zoom in zoom_levels.descending():
            yield TileTaskBatch(
                id=f"zoom_{zoom}",
                tasks=(
                    TileTask(
                        tile=tile,
                        config=process.config,
                        skip=(
                            process.config.mode == "continue"
                            and process.config.output_reader.tiles_exist(tile)
                        )
                        if skip_output_check
                        else skip,
                    )
                    for tile, skip in tiles[zoom]
                ),
                func=func,
                fkwargs=fkwargs,
                profilers=profilers,
            )
    logger.debug("tile task batches generated in %s", duration)
