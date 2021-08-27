import logging
from multiprocessing import cpu_count
from rasterio.crs import CRS
from shapely.geometry.base import BaseGeometry
from typing import Callable, List, Tuple, Union
import warnings

import mapchete
from mapchete.config import bounds_from_opts, raw_conf, raw_conf_process_pyramid

logger = logging.getLogger(__name__)


def execute(
    mapchete_config: Union[str, dict],
    zoom: Union[int, List[int]] = None,
    area: Union[BaseGeometry, str, dict] = None,
    area_crs: Union[CRS, str] = None,
    bounds: Tuple[float] = None,
    bounds_crs: Union[CRS, str] = None,
    point: Tuple[float, float] = None,
    point_crs: Tuple[float, float] = None,
    tile: Tuple[int, int, int] = None,
    overwrite: bool = False,
    mode: str = "continue",
    concurrency: str = "processes",
    workers: int = None,
    multi: int = None,
    multiprocessing_start_method: str = None,
    dask_scheduler: str = None,
    dask_client=None,
    msg_callback: Callable = None,
    as_iterator: bool = False,
) -> mapchete.Job:
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
    dask_client : dask.distributed.Client
        Reusable Client instance if required. Otherwise a new client will be created.
    msg_callback : Callable
        Optional callback function for process messages.
    as_iterator : bool
        Returns as generator but with a __len__() property.

    Returns
    -------
    mapchete.Job instance either with already processed items or a generator with known length.

    Examples
    --------
    >>> execute("foo")

    This will run the whole execute process.

    >>> for i in execute("foo", as_iterator=True):
    >>>     print(i)

    This will return a generator where through iteration, tiles are copied.

    >>> list(tqdm.tqdm(execute("foo", as_iterator=True)))

    Usage within a process bar.
    """
    mode = "overwrite" if overwrite else mode

    def _empty_callback(*args):
        pass

    msg_callback = msg_callback or _empty_callback
    if multi is not None:  # pragma: no cover
        warnings.warn("The 'multi' parameter is deprecated and is now named 'workers'")
    workers = workers or multi or cpu_count()

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

    # be careful opening mapchete not as context manager
    mp = mapchete.open(
        mapchete_config,
        mode=mode,
        bounds=bounds,
        zoom=zoom,
        area=area,
        area_crs=area_crs,
    )
    try:
        if tile:
            tasks_count = 1 + mp.config.preprocessing_tasks_count()
        else:
            tasks_count = mp.count_tasks()
        msg_callback(f"processing {tasks_count} task(s) on {workers} worker(s)")
        # automatically use dask Executor if dask scheduler is defined
        if dask_scheduler or dask_client:  # pragma: no cover
            concurrency = "dask"
        # use sequential Executor if only one tile or only one worker is defined
        elif tasks_count == 1 or workers == 1:
            logger.debug(
                f"using sequential Executor because there is only one {'task' if tasks_count == 1 else 'worker'}"
            )
            concurrency = None
        return mapchete.Job(
            _process_everything,
            fargs=(
                msg_callback,
                mp,
            ),
            fkwargs=dict(
                tile=tile,
                workers=workers,
                zoom=None if tile else zoom,
            ),
            executor_concurrency=concurrency,
            executor_kwargs=dict(
                dask_scheduler=dask_scheduler,
                dask_client=dask_client,
                multiprocessing_start_method=multiprocessing_start_method,
            ),
            as_iterator=as_iterator,
            total=tasks_count,
        )
    # explicitly exit the mp object on failure
    except Exception:  # pragma: no cover
        mp.__exit__(None, None, None)
        raise


def _process_everything(msg_callback, mp, executor=None, workers=None, **kwargs):
    try:
        for preprocessing_task_info in mp.batch_preprocessor(
            executor=executor, workers=workers
        ):  # pragma: no cover
            yield preprocessing_task_info
            msg_callback(preprocessing_task_info)
        for process_info in mp.batch_processor(
            executor=executor, workers=workers, **kwargs
        ):
            yield process_info
            msg_callback(
                f"Tile {process_info.tile.id}: {process_info.process_msg}, {process_info.write_msg}"
            )
    # explicitly exit the mp object on success
    finally:
        mp.__exit__(None, None, None)
