"""Execute a process."""
import logging
from contextlib import AbstractContextManager
from multiprocessing import cpu_count
from typing import Any, List, Optional, Tuple, Type, Union

from rasterio.crs import CRS
from shapely.geometry.base import BaseGeometry

import mapchete
from mapchete.commands.observer import ObserverProtocol, Observers
from mapchete.config import DaskSettings
from mapchete.config.parse import bounds_from_opts, raw_conf, raw_conf_process_pyramid
from mapchete.enums import Concurrency, ProcessingMode, Status
from mapchete.errors import JobCancelledError
from mapchete.executor import Executor
from mapchete.processing.profilers import preconfigured_profilers, pretty_bytes
from mapchete.processing.types import TaskInfo
from mapchete.types import MPathLike, Progress

logger = logging.getLogger(__name__)


def execute(
    mapchete_config: Union[dict, MPathLike],
    zoom: Union[int, List[int]] = None,
    area: Union[BaseGeometry, str, dict] = None,
    area_crs: Union[CRS, str] = None,
    bounds: Tuple[float] = None,
    bounds_crs: Union[CRS, str] = None,
    point: Tuple[float, float] = None,
    point_crs: Tuple[float, float] = None,
    tile: Tuple[int, int, int] = None,
    overwrite: bool = False,
    mode: ProcessingMode = ProcessingMode.CONTINUE,
    concurrency: Concurrency = Concurrency.processes,
    workers: int = None,
    multiprocessing_start_method: str = None,
    dask_scheduler: str = None,
    dask_client: Optional[Any] = None,
    dask_settings: Optional[DaskSettings] = None,
    executor_getter: AbstractContextManager = Executor,
    profiling: bool = False,
    observers: Optional[List[ObserverProtocol]] = None,
    retry_on_exception: Tuple[Type[Exception], Type[Exception]] = Exception,
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
    dask_scheduler : str
        URL to dask scheduler if required.
    dask_max_submitted_tasks : int
        Make sure that not more tasks are submitted to dask scheduler at once. (default: 500)
    dask_chunksize : int
        Number of tasks submitted to the scheduler at once. (default: 100)
    dask_client : dask.distributed.Client
        Reusable Client instance if required. Otherwise a new client will be created.
    dask_compute_graph : bool
        Build and compute dask graph instead of submitting tasks as preprocessing & zoom tiles
        batches. (default: True)
    dask_propagate_results : bool
        Propagate results between tasks. This helps to minimize read calls when building overviews
        but can lead to a much higher memory consumption on the cluster. Only with effect if
        dask_compute_graph is activated. (default: True)
    """
    print_task_details = True
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
        bounds = bounds_from_opts(
            point=point,
            point_crs=point_crs,
            bounds=bounds,
            bounds_crs=bounds_crs,
            raw_conf=raw_conf(mapchete_config),
        )

    # automatically use dask Executor if dask scheduler is defined
    if dask_scheduler or dask_client or concurrency == "dask":
        concurrency = "dask"

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
            tasks = mp.tasks(zoom=zoom, tile=tile, mode=mode)

            if len(tasks) == 0:
                all_observers.notify(status=Status.done)
                return
            all_observers.notify(
                message=f"processing {len(tasks)} tasks on {workers} worker(s)"
            )

            all_observers.notify(message="waiting for executor ...")
            with executor_getter(
                concurrency=concurrency,
                dask_scheduler=dask_scheduler,
                dask_client=dask_client,
                multiprocessing_start_method=multiprocessing_start_method,
                max_workers=workers,
            ) as executor:
                if profiling:
                    for profiler in preconfigured_profilers:
                        executor.add_profiler(profiler)
                all_observers.notify(
                    status=Status.running,
                    progress=Progress(total=len(tasks)),
                    message=f"sending {len(tasks)} tasks to {executor} ...",
                )
                # TODO it would be nice to track the time it took sending tasks to the executor
                try:
                    count = 0
                    for task_info in mp.execute(
                        executor=executor,
                        tasks=tasks,
                        profiling=profiling,
                        dask_settings=dask_settings,
                    ):
                        count += 1
                        if print_task_details:
                            msg = f"task {task_info.id}: {task_info.process_msg}"
                            if task_info.profiling:  # pragma: no cover
                                max_allocated = task_info.profiling[
                                    "memory"
                                ].max_allocated
                                head_requests = task_info.profiling[
                                    "requests"
                                ].head_count
                                get_requests = task_info.profiling["requests"].get_count
                                requests = head_requests + get_requests
                                transferred = task_info.profiling["requests"].get_bytes
                                msg += (
                                    f" (max memory usage: {pretty_bytes(max_allocated)}"
                                )
                                msg += f", {requests} GET and HEAD requests"
                                msg += f", {pretty_bytes(transferred)} transferred)"
                            all_observers.notify(message=msg)

                        all_observers.notify(
                            progress=Progress(total=len(tasks), current=count),
                            task_result=task_info,
                        )

                    all_observers.notify(status=Status.done)
                    return

                except cancel_on_exception:
                    # special exception indicating job was cancelled from the outside
                    all_observers.notify(status=Status.cancelled)
                    return

                except retry_on_exception as exception:
                    all_observers.notify(status=Status.failed, exception=exception)

                    if retries:
                        retries -= 1
                        all_observers.notify(status=Status.retrying)
                    else:
                        raise
