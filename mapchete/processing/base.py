"""Main module managing processes."""

import json
import logging
import os
import threading
from contextlib import ExitStack
from typing import Any, Generator, Iterator, List, Optional, Tuple, Union

from cachetools import LRUCache
from shapely.geometry import Polygon, base
from shapely.ops import unary_union

from mapchete.config import DaskSettings, MapcheteConfig
from mapchete.enums import Concurrency, ProcessingMode
from mapchete.errors import MapcheteNodataTile, ReprojectionFailed
from mapchete.executor import Executor, ExecutorBase, MFuture
from mapchete.executor.types import Profiler
from mapchete.path import batch_sort_property, tiles_exist
from mapchete.processing.execute import batches, dask_graph, single_batch
from mapchete.processing.tasks import (
    TaskBatch,
    TaskInfo,
    Tasks,
    TileTask,
    TileTaskBatch,
)
from mapchete.stac import tile_direcotry_item_to_dict, update_tile_directory_stac_item
from mapchete.tile import BatchBy, BufferedTile, count_tiles
from mapchete.timer import Timer
from mapchete.types import TileLike, ZoomLevelsLike
from mapchete.validate import validate_tile
from mapchete.zoom_levels import ZoomLevels


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

    def __init__(
        self,
        config: MapcheteConfig,
        with_cache: bool = False,
    ):
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
        self.with_cache = (
            True if self.config.mode == ProcessingMode.MEMORY else with_cache
        )
        if self.with_cache:
            self.process_tile_cache = LRUCache(maxsize=512)
            self.current_processes = {}
            self.process_lock = threading.Lock()
        self._count_tiles_cache = {}

    def get_process_tiles(
        self, zoom: Optional[int] = None
    ) -> Generator[BufferedTile, None, None]:
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
        # if batch_by is None and hasattr(self.config.output_reader, "tile_path_schema"):
        #     batch_by = batch_sort_property(self.config.output_reader.tile_path_schema)
        if zoom or zoom == 0:
            for tile in self.config.process_pyramid.tiles_from_geom(
                self.config.area_at_zoom(zoom),
                zoom=zoom,
                exact=True,
            ):
                yield tile
        else:
            for i in self.config.init_zoom_levels.descending():
                for tile in self.config.process_pyramid.tiles_from_geom(
                    self.config.area_at_zoom(i), zoom=i, exact=True
                ):
                    yield tile

    def get_process_tiles_batches(
        self, zoom: Optional[int] = None, batch_by: BatchBy = BatchBy.row
    ) -> Generator[Generator[BufferedTile, None, None], None, None]:
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
        # if batch_by is None and hasattr(self.config.output_reader, "tile_path_schema"):
        #     batch_by = batch_sort_property(self.config.output_reader.tile_path_schema)
        if zoom or zoom == 0:
            for batch in self.config.process_pyramid.tiles_from_geom_batches(
                self.config.area_at_zoom(zoom),
                zoom=zoom,
                batch_by=batch_by,
                exact=True,
            ):
                yield batch
        else:
            for i in self.config.init_zoom_levels.descending():
                for batch in self.config.process_pyramid.tiles_from_geom_batches(
                    self.config.area_at_zoom(i), zoom=i, batch_by=batch_by, exact=True
                ):
                    yield batch

    def skip_tiles(
        self,
        tiles: Optional[Iterator[BufferedTile]] = None,
        tiles_batches: Optional[Iterator[Iterator[BufferedTile]]] = None,
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
        if self.config.mode == ProcessingMode.CONTINUE:
            yield from tiles_exist(
                config=self.config,
                process_tiles=tiles,
                process_tiles_batches=tiles_batches,
            )
        # otherwise don't skip tiles
        else:
            if tiles_batches:  # pragma: no cover
                for batch in tiles_batches:
                    for tile in batch:
                        yield (tile, False)
            elif tiles:
                for tile in tiles:
                    yield (tile, False)
            else:  # pragma: no cover
                raise TypeError("either tiles or tiles_batches required")

    def tasks(
        self,
        zoom: Optional[ZoomLevelsLike] = None,
        tile: Optional[TileLike] = None,
        profilers: Optional[List[Profiler]] = None,
    ) -> Tasks:
        """
        Generate tasks from preprocessing tasks and tile tasks.
        """
        return Tasks(
            _task_batches(
                self,
                zoom=zoom,
                tile=tile,
                profilers=profilers,
            ),
        )

    def preprocessing_tasks(
        self,
        profilers: Optional[List[Profiler]] = None,
    ) -> Tasks:
        """
        Generate tasks from preprocessing tasks and tile tasks.
        """
        return Tasks(
            _preprocessing_task_batches(
                self,
                profilers=profilers,
            ),
        )

    def execute(
        self,
        tasks: Optional[Tasks] = None,
        zoom: Optional[ZoomLevelsLike] = None,
        tile: Optional[TileLike] = None,
        executor: Optional[ExecutorBase] = None,
        concurrency: Concurrency = Concurrency.none,
        workers: Optional[int] = os.cpu_count(),
        propagate_results: bool = False,
        dask_settings: DaskSettings = DaskSettings(),
        remember_preprocessing_results: bool = False,
    ) -> Iterator[TaskInfo]:
        """
        Execute all tasks on given executor and yield TaskInfo as they finish.
        """
        # determine tasks if not provided extra
        # we have to do this before it can be decided which type of processing can be applied
        tasks = self.tasks(zoom=zoom, tile=tile) if tasks is None else tasks

        if len(tasks) == 0:
            return

        with ExitStack() as exit_stack:
            # create a default executor if not available
            if executor is None:
                executor = exit_stack.enter_context(
                    Executor(concurrency=concurrency, workers=workers),
                )

            # tasks have no dependencies with each other and can be executed in
            # any arbitrary order
            if self.config.preprocessing_tasks_count() == 0 and (
                not self.config.baselevels or len(self.config.init_zoom_levels) == 1
            ):
                logger.debug("decided to process tasks in single batch")
                yield from single_batch(
                    executor,
                    tasks,
                    output_writer=self.config.output,
                    write_in_parent_process=self.config.output.write_in_parent_process,
                    propagate_results=propagate_results,
                )

            # tasks are connected via a dependency graph and will be sent to the
            # executor all at once
            elif dask_settings.process_graph and hasattr(
                executor, "compute_task_graph"
            ):
                logger.debug("decided to use dask graph processing")
                for task_info in dask_graph(
                    executor,
                    tasks,
                    output_writer=self.config.output,
                    write_in_parent_process=self.config.output.write_in_parent_process,
                    propagate_results=propagate_results,
                ):
                    # TODO: is this really necessary?
                    if remember_preprocessing_results and task_info.tile is None:
                        self.config.set_preprocessing_task_result(
                            task_info.id, task_info.output
                        )

                    yield task_info

            # tasks are sorted into batches which have to be executed in a
            # particular order
            else:
                logger.debug("decided to process tasks in batches")
                for task_info in batches(
                    executor,
                    tasks,
                    output_writer=self.config.output,
                    write_in_parent_process=self.config.output.write_in_parent_process,
                    propagate_results=propagate_results,
                ):
                    # TODO: is this really necessary?
                    if remember_preprocessing_results and task_info.tile is None:
                        self.config.set_preprocessing_task_result(
                            task_info.id, task_info.output
                        )

                    yield task_info

    def execute_preprocessing_tasks(
        self,
        executor: Optional[ExecutorBase] = None,
    ) -> Iterator[MFuture]:
        """
        Run all required preprocessing steps.
        """
        # If preprocessing tasks already finished, don't run them again.
        if self.config.preprocessing_tasks_finished:  # pragma: no cover
            return

        for task_info in self.execute(
            tasks=self.preprocessing_tasks(),
            executor=executor,
        ):
            self.config.set_preprocessing_task_result(task_info.id, task_info.output)

        self.config.preprocessing_tasks_finished = True

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

    def execute_tile(
        self, process_tile: BufferedTile, raise_nodata: bool = False
    ) -> Any:
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
        # make sure preprocessing tasks are finished
        self.execute_preprocessing_tasks()
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
        if self.config.mode not in [
            ProcessingMode.READONLY,
            ProcessingMode.CONTINUE,
            ProcessingMode.OVERWRITE,
        ]:
            raise ValueError("process mode must be readonly, continue or overwrite")
        return self.config.output.read(output_tile)

    def write(self, process_tile: TileLike, data: Any) -> TaskInfo:
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
        if self.config.mode not in [ProcessingMode.CONTINUE, ProcessingMode.OVERWRITE]:
            raise ValueError("cannot write output in current process mode")

        if self.config.mode == ProcessingMode.CONTINUE and (
            self.config.output.tiles_exist(process_tile)
        ):
            message = "output exists, not overwritten"
            logger.debug((process_tile.id, message))
            return TaskInfo(
                tile=process_tile,
                processed=False,
                process_msg=None,
                written=False,
                write_msg=message,
            )
        elif data is None:
            message = "output empty, nothing written"
            logger.debug((process_tile.id, message))
            return TaskInfo(
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
            return TaskInfo(
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

        if self.config.mode == ProcessingMode.MEMORY:
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
        if self.config.mode == ProcessingMode.READONLY or _baselevel_readonly:
            if self.config.output.tiles_exist(process_tile):
                return self._read_existing_output(tile, output_tiles)
            else:
                return self.config.output.empty(tile)
        elif self.config.mode == ProcessingMode.CONTINUE and not _baselevel_readonly:
            if self.config.output.tiles_exist(process_tile):
                return self._read_existing_output(tile, output_tiles)
            else:
                return self._process_and_overwrite_output(tile, process_tile)
        elif self.config.mode == ProcessingMode.OVERWRITE and not _baselevel_readonly:
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
            ProcessingMode.READONLY,
            ProcessingMode.MEMORY,
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
            output = self.execute_tile(process_tile)
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
                    output = self.execute_tile(process_tile)
                    self.process_tile_cache[process_tile.id] = output
                    if self.config.mode in [
                        ProcessingMode.CONTINUE,
                        ProcessingMode.OVERWRITE,
                    ]:
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
        return (
            f"<Mapchete process_name={self.config.process.name}, config={self.config}>"
        )


def _task_batches(
    process: Mapchete,
    zoom: Optional[ZoomLevelsLike] = None,
    tile: Optional[TileLike] = None,
    profilers: Optional[List[Profiler]] = None,
) -> Iterator[Union[TaskBatch, TileTaskBatch]]:
    """Create task batches for each processing stage."""
    profilers = profilers or []
    if tile:
        tile = process.config.process_pyramid.tile(*tile)

    # first, materialize tile task batches to determine process AOI
    tile_task_batches = _tile_task_batches(
        process=process,
        zoom=zoom,
        tile=tile,
        profilers=profilers,
    )

    # create processing AOI (i.e. processing area without overviews)
    if process.config.preprocessing_tasks().values():
        zoom_aois = []
        for zoom in process.config.processing_levels:
            for batch in tile_task_batches:
                if batch.id == f"zoom-{zoom}":
                    zoom_aois.append(batch.geometry)
        process_aoi = unary_union(zoom_aois) if zoom_aois else Polygon()
    else:
        process_aoi = None

    yield from _preprocessing_task_batches(
        process=process, profilers=profilers, process_aoi=process_aoi
    )

    yield from tile_task_batches


def _preprocessing_task_batches(
    process: Mapchete,
    profilers: Optional[List[Profiler]] = None,
    process_aoi: Optional[base.BaseGeometry] = None,
) -> Iterator[TaskBatch]:
    with Timer() as duration:
        tasks = []
        for task in process.config.preprocessing_tasks().values():
            if isinstance(process_aoi, base.BaseGeometry) and task.has_geometry():
                if task.geometry.intersects(process_aoi):
                    tasks.append(task)
            else:
                tasks.append(task)

        # preprocessing tasks
        preprocessing_batch = TaskBatch(
            id="preprocessing_tasks",
            tasks=tasks,
            profilers=profilers,
        )
        if len(preprocessing_batch):
            yield preprocessing_batch
    logger.debug("preprocessing tasks batch generated in %s", duration)


def _tile_task_batches(
    process: Mapchete,
    zoom: Optional[ZoomLevelsLike] = None,
    tile: Optional[TileLike] = None,
    profilers: Optional[List[Profiler]] = None,
) -> List[TileTaskBatch]:
    with Timer() as duration:
        batches = []

        if tile:
            # nothing to process here
            if (
                process.config.mode == ProcessingMode.CONTINUE
                and process.config.output_reader.tiles_exist(tile)
            ):
                pass

            # process this tile
            else:
                batches = [
                    TileTaskBatch(
                        id=f"zoom-{tile.zoom}",
                        tasks=[
                            TileTask(
                                tile=tile,
                                config=process.config,
                            )
                        ],
                        profilers=profilers,
                    )
                ]

        else:
            zoom_levels = (
                process.config.init_zoom_levels
                if zoom is None
                else ZoomLevels.from_inp(zoom)
            )
            # here we store the parents of tiles about to be processed so we can update overviews
            # also in "continue" mode in case there were updates at the baselevel
            overview_parents = set()
            for i, zoom in enumerate(zoom_levels.descending()):
                tile_tasks = []
                if hasattr(process.config.output_reader, "tile_path_schema"):
                    batch_by = batch_sort_property(
                        process.config.output_reader.tile_path_schema
                    )
                else:
                    batch_by = "row"
                for tile in _filter_skipable(
                    process=process,
                    tiles_batches=process.get_process_tiles_batches(
                        zoom, batch_by=BatchBy[batch_by]
                    ),
                    overview_tiles=(
                        overview_parents if process.config.baselevels and i else None
                    ),
                ):
                    # we don't need the current tile anymore
                    overview_parents.discard(tile)

                    # add tile task to batch
                    tile_tasks.append(TileTask(tile=tile, config=process.config))

                    # in case of building overviews from baselevels, remember which parent
                    # tile needs to be updated later on
                    if process.config.baselevels and tile.zoom > 0:
                        # add parent tile to be reprocessed
                        overview_parents.add(tile.get_parent())

                batches.append(
                    TileTaskBatch(
                        id=f"zoom-{zoom}",
                        tasks=tile_tasks,
                        profilers=profilers,
                    )
                )
    logger.debug("tile task batches generated in %s", duration)
    return batches


def _filter_skipable(
    process: Mapchete,
    tiles_batches: Iterator[Iterator[BufferedTile]],
    overview_tiles: Optional[set] = None,
) -> Iterator[Tuple[BufferedTile, bool]]:
    # we don't filter here
    if process.config.mode == ProcessingMode.OVERWRITE:
        for batch in tiles_batches:
            yield from batch

    # only yield tiles which don't yet exist or need to be reprocessed
    else:
        overview_tiles = overview_tiles or set()
        for tile, skip in process.skip_tiles(tiles_batches=tiles_batches):
            if skip and tile not in overview_tiles:
                pass
            else:
                yield tile
