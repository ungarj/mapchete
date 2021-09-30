from collections import namedtuple
from concurrent.futures._base import CancelledError
from itertools import chain
import logging
import multiprocessing
import os
from traceback import format_exc
from typing import Generator

from mapchete.commons import clip as commons_clip
from mapchete.commons import contours as commons_contours
from mapchete.commons import hillshade as commons_hillshade
from mapchete.config import get_process_func
from mapchete._executor import Executor
from mapchete.errors import MapcheteNodataTile, MapcheteProcessException
from mapchete.io import raster
from mapchete._timer import Timer
from mapchete.validate import deprecated_kwargs

logger = logging.getLogger(__name__)


ProcessInfo = namedtuple("ProcessInfo", "tile processed process_msg written write_msg")


class Job:
    """
    Wraps the output of a processing function into a generator with known length.

    This class also exposes the internal Executor.cancel() function in order to cancel all remaining
    tasks/futures.

    Will move into the mapchete core package.
    """

    def __init__(
        self,
        func: Generator,
        fargs: tuple = None,
        fkwargs: dict = None,
        as_iterator: bool = False,
        total: int = None,
        executor_concurrency: str = "processes",
        executor_kwargs: dict = None,
    ):
        self.func = func
        self.fargs = fargs or ()
        self.fkwargs = fkwargs or {}
        self.status = "pending"
        self.executor = None
        self.executor_concurrency = executor_concurrency
        self.executor_kwargs = executor_kwargs or {}
        self._total = total
        self._as_iterator = as_iterator
        if not as_iterator:
            self._results = list(self._run())

    def _run(self):
        if self._total == 0:
            return
        with Executor(
            concurrency=self.executor_concurrency, **self.executor_kwargs
        ) as self.executor:
            self.status = "running"
            yield from self.func(*self.fargs, executor=self.executor, **self.fkwargs)
            self.status = "finished"

    def cancel(self):
        if self._as_iterator:
            # requires client and futures
            if self.executor is None:  # pragma: no cover
                raise ValueError("nothing to cancel because no executor is running")
            self.executor.cancel()
            self.status = "cancelled"

    def __len__(self):
        return self._total

    def __iter__(self):
        if self._as_iterator:
            yield from self._run()
        else:
            return self._results

    def __repr__(self):  # pragma: no cover
        return f"<{self.status} Job with {self._total} tasks.>"


class TileProcess:
    """
    Class to process on a specific process tile.

    If skip is set to True, all attributes will be set to None.
    """

    def __init__(self, tile=None, config=None, skip=False):
        """Set attributes depending on baselevels or not."""
        self.tile = (
            config.process_pyramid.tile(*tile) if isinstance(tile, tuple) else tile
        )
        self.skip = skip
        self.config_zoom_levels = None if skip else config.zoom_levels
        self.config_baselevels = None if skip else config.baselevels
        self.process = None if skip else config.process
        self.config_dir = None if skip else config.config_dir
        if skip or self.tile.zoom not in self.config_zoom_levels:
            self.input, self.process_func_params, self.output_params = {}, {}, {}
        else:
            self.input = config.get_inputs_for_tile(tile)
            self.process_func_params = config.get_process_func_params(tile.zoom)
            self.output_params = config.output_reader.output_params
        self.mode = None if skip else config.mode
        self.output_reader = (
            None if skip or not config.baselevels else config.output_reader
        )

    def execute(self):
        """
        Run the Mapchete process and return the result.

        If baselevels are defined it will generate the result from the other zoom levels
        accordingly.

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
        if self.mode not in ["memory", "continue", "overwrite"]:  # pragma: no cover
            raise ValueError("process mode must be memory, continue or overwrite")

        if self.tile.zoom not in self.config_zoom_levels:
            raise MapcheteNodataTile

        return self._execute()

    def _execute(self):
        # If baselevel is active and zoom is outside of baselevel,
        # interpolate from other zoom levels.
        if self.config_baselevels:
            if self.tile.zoom < min(self.config_baselevels["zooms"]):
                return self._interpolate_from_baselevel("lower")
            elif self.tile.zoom > max(self.config_baselevels["zooms"]):
                return self._interpolate_from_baselevel("higher")
        # Otherwise, execute from process file.
        process_func = get_process_func(
            process=self.process, config_dir=self.config_dir
        )
        try:
            with Timer() as t:
                # Actually run process.
                process_data = process_func(
                    MapcheteProcess(
                        tile=self.tile,
                        params=self.process_func_params,
                        input=self.input,
                        output_params=self.output_params,
                    ),
                    **self.process_func_params,
                )
        except MapcheteNodataTile:
            raise
        except Exception as e:
            # Log process time
            logger.exception((self.tile.id, "exception in user process", e, str(t)))
            new = MapcheteProcessException(format_exc())
            new.old = e
            raise new

        return process_data

    def _interpolate_from_baselevel(self, baselevel=None):
        # This is a special tile derived from a pyramid which has the pixelbuffer setting
        # from the output pyramid but metatiling from the process pyramid. This is due to
        # performance reasons as for the usual case overview tiles do not need the
        # process pyramid pixelbuffers.
        tile = self.config_baselevels["tile_pyramid"].tile(*self.tile.id)

        # get output_tiles that intersect with process tile
        output_tiles = (
            list(self.output_reader.pyramid.tiles_from_bounds(tile.bounds, tile.zoom))
            if tile.pixelbuffer > self.output_reader.pyramid.pixelbuffer
            else self.output_reader.pyramid.intersecting(tile)
        )

        with Timer() as t:
            # resample from parent tile
            if baselevel == "higher":
                parent_tile = self.tile.get_parent()
                process_data = raster.resample_from_array(
                    self.output_reader.read(parent_tile),
                    in_affine=parent_tile.affine,
                    out_tile=self.tile,
                    resampling=self.config_baselevels["higher"],
                    nodata=self.output_reader.output_params["nodata"],
                )
            # resample from children tiles
            elif baselevel == "lower":
                if self.output_reader.pyramid.pixelbuffer:  # pragma: no cover
                    lower_tiles = set(
                        [
                            y
                            for y in chain(
                                *[
                                    self.output_reader.pyramid.tiles_from_bounds(
                                        x.bounds, x.zoom + 1
                                    )
                                    for x in output_tiles
                                ]
                            )
                        ]
                    )
                else:
                    lower_tiles = [
                        y for y in chain(*[x.get_children() for x in output_tiles])
                    ]
                mosaic = raster.create_mosaic(
                    [
                        (lower_tile, self.output_reader.read(lower_tile))
                        for lower_tile in lower_tiles
                    ],
                    nodata=self.output_reader.output_params["nodata"],
                )
                process_data = raster.resample_from_array(
                    in_raster=mosaic.data,
                    in_affine=mosaic.affine,
                    out_tile=self.tile,
                    resampling=self.config_baselevels["lower"],
                    nodata=self.output_reader.output_params["nodata"],
                )
        logger.debug((self.tile.id, "generated from baselevel", str(t)))
        return process_data


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
    tile_pyramid : BufferedTilePyramid
        process tile pyramid
    output_pyramid : BufferedTilePyramid
        output tile pyramid
    params : dictionary
        process parameters
    """

    def __init__(
        self, tile=None, params=None, input=None, output_params=None, config=None
    ):
        """Initialize Mapchete process."""
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""

        self.tile = tile
        self.tile_pyramid = tile.tile_pyramid
        if config is not None:
            input = config.get_inputs_for_tile(tile)
            params = config.params_at_zoom(tile.zoom)
        self.params = dict(params, input=input)
        self.input = input
        self.output_params = output_params

    def write(self, data, **kwargs):
        """Deprecated."""
        raise DeprecationWarning(
            "Please return process output data instead of using self.write()."
        )

    def read(self, **kwargs):
        """
        Read existing output data from a previous run.

        Returns
        -------
        process output : NumPy array (raster) or feature iterator (vector)
        """
        raise DeprecationWarning(
            "Read existing output from within a process is deprecated"
        )

    @deprecated_kwargs
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
        if input_id not in self.input:
            raise ValueError("%s not found in config as input" % input_id)
        return self.input[input_id]

    def hillshade(self, elevation, azimuth=315.0, altitude=45.0, z=1.0, scale=1.0):
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
            elevation, self.tile, azimuth, altitude, z, scale
        )

    def contours(self, elevation, interval=100, field="elev", base=0):
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

    def clip(self, array, geometries, inverted=False, clip_buffer=0):
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
            array,
            self.tile.affine,
            geometries,
            inverted=inverted,
            clip_buffer=clip_buffer * self.tile.pixel_x_size,
        )


#######################
# batch preprocessing #
#######################


def _preprocess_task_wrapper(task_tuple):
    task_key, (func, fargs, fkwargs) = task_tuple
    return task_key, func(*fargs, **fkwargs)


def _preprocess(
    tasks,
    process=None,
    dask_scheduler=None,
    workers=None,
    multiprocessing_module=None,
    multiprocessing_start_method=None,
    executor=None,
):
    # If preprocessing tasks already finished, don't run them again.
    if process.config.preprocessing_tasks_finished:  # pragma: no cover
        return

    # If an Executor is passed on, don't close after processing. If no Executor is passed on,
    # create one and properly close it afterwards.
    create_executor = executor is None
    executor = executor or Executor(
        max_workers=workers,
        concurrency="dask" if dask_scheduler else "processes",
        start_method=multiprocessing_start_method,
        multiprocessing_module=multiprocessing_module,
        dask_scheduler=dask_scheduler,
    )
    try:
        with Timer() as t:
            logger.debug(
                "run preprocessing on %s tasks using %s workers", len(tasks), workers
            )

            # process all remaining tiles using todo list from before
            for i, future in enumerate(
                executor.as_completed(
                    func=_preprocess_task_wrapper,
                    iterable=[(k, v) for k, v in tasks.items()],
                ),
                1,
            ):
                task_key, result = future.result()
                logger.debug(
                    f"preprocessing task {i}/{len(tasks)} {task_key} processed successfully"
                )
                process.config.set_preprocessing_task_result(task_key, result)
                yield f"preprocessing task {task_key} finished"
    finally:
        if create_executor:
            executor.close()

    process.config.preprocessing_tasks_finished = True

    logger.info("%s task(s) iterated in %s", str(len(tasks)), t)


###########################
# batch execution options #
###########################


def _run_on_single_tile(
    executor=None,
    process=None,
    tile=None,
    dask_scheduler=None,
):
    logger.debug("run process on single tile")
    create_executor = executor is None
    executor = executor or Executor(
        concurrency="dask" if dask_scheduler else None,
        dask_scheduler=dask_scheduler,
    )
    try:
        return next(
            executor.as_completed(
                func=_execute_and_write,
                iterable=[
                    TileProcess(
                        tile=tile,
                        config=process.config,
                        skip=(
                            process.config.mode == "continue"
                            and process.config.output_reader.tiles_exist(tile)
                        ),
                    ),
                ],
                fkwargs=dict(output_writer=process.config.output),
            )
        ).result()
    finally:
        if create_executor:
            executor.close()


def _run_area(
    executor=None,
    process=None,
    zoom_levels=None,
    dask_scheduler=None,
    workers=None,
    multiprocessing_module=None,
    multiprocessing_start_method=None,
    skip_output_check=False,
):
    logger.debug("run process on area")
    zoom_levels.sort(reverse=True)

    # for output drivers requiring writing data in parent process
    if process.config.output.write_in_parent_process:
        for process_info in _run_multi(
            executor=executor,
            func=_execute,
            zoom_levels=zoom_levels,
            process=process,
            dask_scheduler=dask_scheduler,
            workers=workers,
            multiprocessing_start_method=multiprocessing_start_method,
            multiprocessing_module=multiprocessing_module,
            write_in_parent_process=True,
            skip_output_check=skip_output_check,
        ):
            yield process_info

    # for output drivers which can write data in child processes
    else:
        for process_info in _run_multi(
            executor=executor,
            func=_execute_and_write,
            fkwargs=dict(output_writer=process.config.output),
            zoom_levels=zoom_levels,
            process=process,
            dask_scheduler=dask_scheduler,
            workers=workers,
            multiprocessing_start_method=multiprocessing_start_method,
            multiprocessing_module=multiprocessing_module,
            write_in_parent_process=False,
            skip_output_check=skip_output_check,
        ):
            yield process_info


def _filter_skipable(process=None, tiles=None, todo=None, target_set=None):
    target_set = target_set or set()
    for tile, skip in process.skip_tiles(tiles=tiles):
        if skip and tile not in target_set:
            yield ProcessInfo(
                tile=tile,
                processed=False,
                process_msg="output already exists",
                written=False,
                write_msg="nothing written",
            )
        else:
            todo.add(tile)


def _run_multi(
    executor=None,
    func=None,
    zoom_levels=None,
    process=None,
    dask_scheduler=None,
    workers=None,
    multiprocessing_start_method=None,
    multiprocessing_module=None,
    write_in_parent_process=False,
    fkwargs=None,
    skip_output_check=False,
):
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    workers = min([workers, total_tiles])
    num_processed = 0

    # here we store the parents of processed tiles so we can update overviews
    # also in "continue" mode in case there were updates at the baselevel
    overview_parents = set()

    # If an Executor is passed on, don't close after processing. If no Executor is passed on,
    # create one and properly close it afterwards.
    create_executor = executor is None
    executor = executor or Executor(
        max_workers=workers,
        concurrency="dask" if dask_scheduler else "processes",
        start_method=multiprocessing_start_method,
        multiprocessing_module=multiprocessing_module,
        dask_scheduler=dask_scheduler,
    )

    try:
        with Timer() as t:
            logger.debug(
                "run process on %s tiles using %s workers", total_tiles, workers
            )

            for i, zoom in enumerate(zoom_levels):

                if skip_output_check:  # pragma: no cover
                    # don't check outputs and simply proceed
                    todo = process.get_process_tiles(zoom)
                else:
                    # check which process output already exists and which process tiles need
                    # to be added to todo list
                    todo = set()
                    logger.debug("check skippable tiles")
                    for process_info in _filter_skipable(
                        process=process,
                        tiles=process.get_process_tiles(zoom),
                        todo=todo,
                        target_set=(
                            overview_parents
                            if process.config.baselevels and i
                            else None
                        ),
                    ):
                        num_processed += 1
                        logger.debug(
                            "tile %s/%s finished: %s, %s, %s",
                            num_processed,
                            total_tiles,
                            process_info.tile,
                            process_info.process_msg,
                            process_info.write_msg,
                        )
                        yield process_info

                # process all remaining tiles using todo list from before
                for future in executor.as_completed(
                    func=func,
                    iterable=(
                        TileProcess(
                            tile=tile,
                            config=process.config,
                            skip=(
                                process.mode == "continue"
                                and process.config.output_reader.tiles_exist(tile)
                            )
                            if skip_output_check
                            else False,
                        )
                        for tile in todo
                    ),
                    fkwargs=fkwargs,
                ):
                    # trigger output write for driver which require parent process for writing
                    if write_in_parent_process:
                        output_data, process_info = future.result()
                        process_info = _write(
                            process_info=process_info,
                            output_data=output_data,
                            output_writer=process.config.output,
                        )

                    # output already has been written, so just use task process info
                    else:
                        process_info = future.result()

                        # in case of building overviews from baselevels, remember which parent
                        # tile needs to be updated later on
                        if (
                            not skip_output_check
                            and process.config.baselevels
                            and process_info.processed
                            and process_info.tile.zoom > 0
                        ):
                            overview_parents.add(process_info.tile.get_parent())

                    num_processed += 1
                    logger.debug(
                        "tile %s/%s finished: %s, %s, %s",
                        num_processed,
                        total_tiles,
                        process_info.tile,
                        process_info.process_msg,
                        process_info.write_msg,
                    )
                    yield process_info

    finally:
        if create_executor:
            executor.close()

    logger.info("%s tile(s) iterated in %s", str(num_processed), t)


###############################
# execute and write functions #
###############################


def _execute(tile_process=None, **kwargs):
    logger.debug(
        (tile_process.tile.id, "running on %s" % multiprocessing.current_process().name)
    )

    # skip execution if overwrite is disabled and tile exists
    if tile_process.skip:
        logger.debug((tile_process.tile.id, "tile exists, skipping"))
        return None, ProcessInfo(
            tile=tile_process.tile,
            processed=False,
            process_msg="output already exists",
            written=False,
            write_msg="nothing written",
        )

    # execute on process tile
    else:
        with Timer() as t:
            try:
                output = tile_process.execute()
            except MapcheteNodataTile:  # pragma: no cover
                output = "empty"
        processor_message = "processed in %s" % t
        logger.debug((tile_process.tile.id, processor_message))
        return output, ProcessInfo(
            tile=tile_process.tile,
            processed=True,
            process_msg=processor_message,
            written=None,
            write_msg=None,
        )


def _write(process_info=None, output_data=None, output_writer=None, **kwargs):
    if process_info.processed:
        try:
            output_data = output_writer.streamline_output(output_data)
        except MapcheteNodataTile:
            output_data = None
        if output_data is None:
            message = "output empty, nothing written"
            logger.debug((process_info.tile.id, message))
            return ProcessInfo(
                tile=process_info.tile,
                processed=process_info.processed,
                process_msg=process_info.process_msg,
                written=False,
                write_msg=message,
            )
        else:
            with Timer() as t:
                output_writer.write(process_tile=process_info.tile, data=output_data)
            message = "output written in %s" % t
            logger.debug((process_info.tile.id, message))
            return ProcessInfo(
                tile=process_info.tile,
                processed=process_info.processed,
                process_msg=process_info.process_msg,
                written=True,
                write_msg=message,
            )
    else:
        return process_info


def _execute_and_write(tile_process=None, output_writer=None, **kwargs):
    output_data, process_info = _execute(tile_process=tile_process)
    return _write(
        process_info=process_info, output_data=output_data, output_writer=output_writer
    )
