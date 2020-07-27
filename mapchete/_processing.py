from collections import namedtuple
from functools import partial
from itertools import chain
import logging
import multiprocessing
import os
from traceback import format_exc

from mapchete.commons import clip as commons_clip
from mapchete.commons import contours as commons_contours
from mapchete.commons import hillshade as commons_hillshade
from mapchete.config import get_process_func
from mapchete.errors import MapcheteNodataTile, MapcheteProcessException
from mapchete.io import raster
from mapchete._timer import Timer
from mapchete.validate import deprecated_kwargs

logger = logging.getLogger(__name__)


ProcessInfo = namedtuple('ProcessInfo', 'tile processed process_msg written write_msg')


class TileProcess():
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
        self.process_path = None if skip else config.process_path
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
        if self.mode not in ["memory", "continue", "overwrite"]:
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
            process_path=self.process_path, config_dir=self.config_dir
        )
        try:
            with Timer() as t:
                # Actually run process.
                process_data = process_func(
                    MapcheteProcess(
                        tile=self.tile,
                        params=self.process_func_params,
                        input=self.input,
                        output_params=self.output_params
                    ),
                    **self.process_func_params
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
                    nodata=self.output_reader.output_params["nodata"]
                )
            # resample from children tiles
            elif baselevel == "lower":
                if self.output_reader.pyramid.pixelbuffer:  # pragma: no cover
                    lower_tiles = set([
                        y for y in chain(*[
                            self.output_reader.pyramid.tiles_from_bounds(
                                x.bounds, x.zoom + 1
                            )
                            for x in output_tiles
                        ])
                    ])
                else:
                    lower_tiles = [
                        y for y in chain(*[x.get_children() for x in output_tiles])
                    ]
                mosaic = raster.create_mosaic(
                    [
                        (lower_tile, self.output_reader.read(lower_tile))
                        for lower_tile in lower_tiles
                    ],
                    nodata=self.output_reader.output_params["nodata"]
                )
                process_data = raster.resample_from_array(
                    in_raster=mosaic.data,
                    in_affine=mosaic.affine,
                    out_tile=self.tile,
                    resampling=self.config_baselevels["lower"],
                    nodata=self.output_reader.output_params["nodata"]
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
        self,
        tile=None,
        params=None,
        input=None,
        output_params=None,
        config=None
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
            elevation, self.tile, azimuth, altitude, z, scale
        )

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
            inverted=inverted, clip_buffer=clip_buffer * self.tile.pixel_x_size
        )


#############################################################
# wrappers helping to abstract multiprocessing and billiard #
#############################################################
class Executor():
    """Wrapper class to be used with multiprocessing or billiard."""

    def __init__(
        self,
        start_method="spawn",
        max_workers=None,
        multiprocessing_module=multiprocessing
    ):
        """Set attributes."""
        self.start_method = start_method
        self.max_workers = max_workers or os.cpu_count()
        self.multiprocessing_module = multiprocessing_module
        logger.debug(
            "init %s Executor with start_method %s and %s workers",
            self.multiprocessing_module, self.start_method, self.max_workers
        )
        self._pool = self.multiprocessing_module.get_context(self.start_method).Pool(
            self.max_workers
        )

    def as_completed(
        self,
        func=None,
        iterable=None,
        fargs=None,
        fkwargs=None,
        chunksize=1
    ):
        """Yield finished tasks."""
        fargs = fargs or []
        fkwargs = fkwargs or {}
        if self.max_workers == 1:
            for i in iterable:
                yield _exception_wrapper(func, fargs, fkwargs, i)
        else:
            logger.debug(
                "open multiprocessing.Pool and %s %s workers",
                self.start_method, self.max_workers
            )
            for finished_task in self._pool.imap_unordered(
                partial(_exception_wrapper, func, fargs, fkwargs),
                iterable,
                chunksize=chunksize or 1
            ):
                yield finished_task

    def __enter__(self):
        """Enter context manager."""
        self._pool.__enter__()
        return self

    def __exit__(self, *args):
        """Exit context manager."""
        logger.debug("closing %s and workers", self._pool)
        self._pool.__exit__(*args)
        logger.debug("%s closed", self._pool)


class FinishedTask():
    """Wrapper class to encapsulate exceptions."""

    def __init__(self, func, fargs=None, fkwargs=None):
        """Set attributes."""
        fargs = fargs or []
        fkwargs = fkwargs or {}
        try:
            self._result, self._exception = func(*fargs, **fkwargs), None
        except Exception as e:  # pragma: no cover
            self._result, self._exception = None, e

    def result(self):
        """Return task result."""
        if self._exception:
            logger.exception(self._exception)
            raise self._exception
        else:
            return self._result

    def exception(self):
        """Raise task exception if any."""
        return self._exception

    def __repr__(self):
        """Return string representation."""
        return "FinishedTask(result=%s, exception=%s)" % (self._result, self._exception)


def _exception_wrapper(func, fargs, fkwargs, i):
    """Wrap function around FinishedTask object."""
    return FinishedTask(func, list(chain([i], fargs)), fkwargs)


###########################
# batch execution options #
###########################

def _run_on_single_tile(process=None, tile=None):
    logger.debug("run process on single tile")
    return _execute_and_write(
        tile_process=TileProcess(
            tile=tile,
            config=process.config,
            skip=(
                process.config.mode == "continue" and
                process.config.output_reader.tiles_exist(tile)
            )
        ),
        output_writer=process.config.output
    )


def _run_area(
    process=None,
    zoom_levels=None,
    multi=None,
    max_chunksize=None,
    multiprocessing_module=None,
    multiprocessing_start_method=None,
    skip_output_check=False
):
    logger.debug("run process on area")
    zoom_levels.sort(reverse=True)

    # for output drivers requiring writing data in parent process
    if process.config.output.write_in_parent_process:
        for process_info in _run_multi(
            func=_execute,
            zoom_levels=zoom_levels,
            process=process,
            multi=multi,
            multiprocessing_start_method=multiprocessing_start_method,
            multiprocessing_module=multiprocessing_module,
            max_chunksize=max_chunksize,
            write_in_parent_process=True,
            skip_output_check=skip_output_check
        ):
            yield process_info

    # for output drivers which can write data in child processes
    else:
        for process_info in _run_multi(
            func=_execute_and_write,
            fkwargs=dict(output_writer=process.config.output),
            zoom_levels=zoom_levels,
            process=process,
            multi=multi,
            multiprocessing_start_method=multiprocessing_start_method,
            multiprocessing_module=multiprocessing_module,
            max_chunksize=max_chunksize,
            write_in_parent_process=False,
            skip_output_check=skip_output_check
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
                write_msg="nothing written"
            )
        else:
            todo.add(tile)


def _run_multi(
    func=None,
    zoom_levels=None,
    process=None,
    multi=None,
    multiprocessing_start_method=None,
    multiprocessing_module=None,
    max_chunksize=None,
    write_in_parent_process=False,
    fkwargs=None,
    skip_output_check=False
):
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    workers = min([multi, total_tiles])
    num_processed = 0
    logger.debug("run process on %s tiles using %s workers", total_tiles, workers)

    # here we store the parents of processed tiles so we can update overviews
    # also in "continue" mode in case there were updates at the baselevel
    overview_parents = set()

    with Timer() as t, Executor(
        max_workers=workers,
        start_method=multiprocessing_start_method,
        multiprocessing_module=multiprocessing_module
    ) as executor:

        for i, zoom in enumerate(zoom_levels):

            if skip_output_check:  # pragma: no cover
                # don't check outputs and simply proceed
                todo = process.get_process_tiles(zoom)
            else:
                # check which process output already exists and which process tiles need
                # to be added to todo list
                todo = set()
                for process_info in _filter_skipable(
                    process=process,
                    tiles=process.get_process_tiles(zoom),
                    todo=todo,
                    target_set=(
                        overview_parents if process.config.baselevels and i else None
                    ),
                ):
                    num_processed += 1
                    logger.info(
                        "tile %s/%s finished: %s, %s, %s",
                        num_processed,
                        total_tiles,
                        process_info.tile,
                        process_info.process_msg,
                        process_info.write_msg
                    )
                    yield process_info

            # process all remaining tiles using todo list from before
            for task in executor.as_completed(
                func=func,
                iterable=(
                    TileProcess(
                        tile=tile,
                        config=process.config,
                        skip=(
                            process.mode == "continue" and
                            process.config.output_reader.tiles_exist(tile)
                        ) if skip_output_check else False
                    ) for tile in todo
                ),
                fkwargs=fkwargs,
                chunksize=max_chunksize
            ):
                # trigger output write for driver which require parent process for writing
                if write_in_parent_process:
                    output_data, process_info = task.result()
                    process_info = _write(
                        process_info=process_info,
                        output_data=output_data,
                        output_writer=process.config.output,
                    )

                # output already has been written, so just use task process info
                else:
                    process_info = task.result()

                    # in case of building overviews from baselevels, remember which parent
                    # tile needs to be updated later on
                    if (
                        not skip_output_check and
                        process.config.baselevels and
                        process_info.processed and
                        process_info.tile.zoom > 0
                    ):
                        overview_parents.add(process_info.tile.get_parent())

                num_processed += 1
                logger.info(
                    "tile %s/%s finished: %s, %s, %s",
                    num_processed,
                    total_tiles,
                    process_info.tile,
                    process_info.process_msg,
                    process_info.write_msg
                )
                yield process_info

    logger.debug("%s tile(s) iterated in %s", str(num_processed), t)


###############################
# execute and write functions #
###############################

def _execute(tile_process=None):
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
            write_msg="nothing written"
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
            write_msg=None
        )


def _write(process_info=None, output_data=None, output_writer=None):
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
                write_msg=message
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
                write_msg=message
            )
    else:
        return process_info


def _execute_and_write(tile_process=None, output_writer=None):
    output_data, process_info = _execute(tile_process=tile_process)
    return _write(
        process_info=process_info,
        output_data=output_data,
        output_writer=output_writer
    )
