"""Execute a process."""

import logging
from contextlib import AbstractContextManager
from multiprocessing import cpu_count
from typing import List, Optional, Tuple, Type, Union

from rasterio.crs import CRS
from shapely.geometry.base import BaseGeometry

import mapchete
from mapchete.commands.observer import ObserverProtocol, Observers
from mapchete.config import DaskSettings
from mapchete.config.parse import bounds_from_opts, raw_conf, raw_conf_process_pyramid
from mapchete.enums import Concurrency, ProcessingMode, Status
from mapchete.errors import JobCancelledError
from mapchete.executor import Executor
from mapchete.executor.types import Profiler
from mapchete.processing.profilers import preconfigured_profilers
from mapchete.processing.profilers.time import measure_time
from mapchete.types import MPathLike, Progress

logger = logging.getLogger(__name__)


def execute(
    mapchete_config: Union[dict, MPathLike],
    zoom: Optional[Union[int, List[int]]] = None,
    area: Optional[Union[BaseGeometry, str, dict]] = None,
    area_crs: Optional[Union[CRS, str]] = None,
    bounds: Optional[Tuple[float]] = None,
    bounds_crs: Optional[Union[CRS, str]] = None,
    point: Optional[Tuple[float, float]] = None,
    point_crs: Optional[Tuple[float, float]] = None,
    tile: Optional[Tuple[int, int, int]] = None,
    overwrite: bool = False,
    mode: ProcessingMode = ProcessingMode.CONTINUE,
    concurrency: Concurrency = Concurrency.none,
    workers: int = None,
    multiprocessing_start_method: str = None,
    dask_settings: DaskSettings = DaskSettings(),
    executor_getter: AbstractContextManager = Executor,
    profiling: bool = False,
    observers: Optional[List[ObserverProtocol]] = None,
    retry_on_exception: Union[Tuple[Type[Exception], ...], Type[Exception]] = Exception,
    cancel_on_exception: Type[Exception] = JobCancelledError,
    retries: int = 0,
):
    """
    Execute a Mapchete process.

    Parameters
    ----------
    mapchete_config : str or dict
        Mapchete configuration as file path or dictionary.
    zoom : integer or list of integers
        Single zoom, minimum and maximum zoom or a list of zoom levels.
    area : str, dict, BaseGeometry
        Geometry to override bounds or area provided in process configuration. Can be either a
        WKT string, a GeoJSON mapping, a shapely geometry or a path to a Fiona-readable file.
    area_crs : CRS or str
        CRS of area (default: process CRS).
    bounds : tuple
        Override bounds or area provided in process configuration.
    bounds_crs : CRS or str
        CRS of area (default: process CRS).
    point : iterable
        X and y coordinates of point whose corresponding process tile bounds will be used.
    point_crs : str or CRS
        CRS of point (defaults to process pyramid CRS).
    tile : tuple
        Zoom, row and column of tile to be processed (cannot be used with zoom)
    overwrite : bool
        Overwrite existing output.
    mode : str
        Set process mode. One of "readonly", "continue" or "overwrite".
    workers : int
        Number of execution workers when processing concurrently.
    multiprocessing_start_method : str
        Method used by multiprocessing module to start child workers. Availability of methods
        depends on OS.
    concurrency : str
        Concurrency to be used. Could either be "processes", "threads" or "dask".
    dask_client : dask.distributed.Client
        Reusable Client instance if required. Otherwise a new client will be created.
    """
    try:
        mode = "overwrite" if overwrite else mode
        all_observers = Observers(observers)

        if not isinstance(retry_on_exception, tuple):
            retry_on_exception = (retry_on_exception,)
        workers = workers or cpu_count()

        all_observers.notify(status=Status.parsing)

        if tile:
            tile = raw_conf_process_pyramid(raw_conf(mapchete_config)).tile(*tile)
            bounds = tile.bounds
            zoom = tile.zoom
        else:
            try:
                bounds = bounds_from_opts(
                    point=point,
                    point_crs=point_crs,
                    bounds=bounds,
                    bounds_crs=bounds_crs,
                    raw_conf=raw_conf(mapchete_config),
                )
            except ValueError:
                bounds = None

        # be careful opening mapchete not as context manager
        with mapchete.open(
            mapchete_config,
            mode=mode,
            bounds=bounds,
            zoom=zoom,
            area=area,
            area_crs=area_crs,
        ) as mp:
            attempt = 0

            # the part below can be retried n times #
            #########################################

            while retries + 1:
                attempt += 1

                # simulating that with every retry, probably less tasks have to be
                # executed
                if attempt > 1:
                    retries_str = "retry" if retries == 1 else "retries"
                    all_observers.notify(
                        message=f"attempt {attempt}, {retries} {retries_str} left"
                    )

                all_observers.notify(status=Status.initializing)

                # determine tasks
                tasks = mp.tasks(zoom=zoom, tile=tile)

                if len(tasks) == 0:
                    all_observers.notify(
                        status=Status.done, message="no tasks to process"
                    )
                    return

                all_observers.notify(
                    message=f"processing {len(tasks)} tasks on {workers} worker(s)"
                )
                all_observers.notify(message="waiting for executor ...")

                try:
                    with executor_getter(
                        concurrency=concurrency,
                        dask_scheduler=dask_settings.scheduler,
                        dask_client=dask_settings.client,
                        multiprocessing_start_method=multiprocessing_start_method,
                        max_workers=workers,
                        preprocessing_tasks=tasks.preprocessing_tasks_count,
                        tile_tasks=tasks.tile_tasks_count,
                    ) as executor:
                        if profiling:
                            for profiler in preconfigured_profilers:
                                executor.add_profiler(profiler)
                        else:
                            executor.add_profiler(
                                Profiler(name="time", decorator=measure_time)
                            )
                        all_observers.notify(
                            status=Status.running,
                            progress=Progress(total=len(tasks)),
                            message=f"sending {len(tasks)} tasks to {executor} ...",
                            executor=executor,
                        )
                        # TODO it would be nice to track the time it took sending tasks to the executor
                        for count, task_info in enumerate(
                            mp.execute(
                                executor=executor,
                                tasks=tasks,
                                dask_settings=dask_settings,
                            ),
                            1,
                        ):
                            all_observers.notify(
                                progress=Progress(total=len(tasks), current=count),
                                task_info=task_info,
                            )
                        all_observers.notify(status=Status.done)
                        return

                except cancel_on_exception:
                    # special exception indicating job was cancelled from the outside
                    all_observers.notify(status=Status.cancelled)
                    return

                except retry_on_exception as exception:
                    if retries:
                        retries -= 1
                        all_observers.notify(
                            status=Status.retrying,
                            message=f"run failed due to {repr(exception)} (remaining retries: {retries})",
                        )
                    else:
                        raise

    except Exception as exception:
        all_observers.notify(status=Status.failed, exception=exception)
        raise
