from collections import namedtuple
from functools import partial
from itertools import chain
import logging
import multiprocessing
import os
from traceback import format_exc
import warnings

from mapchete.commons import clip as commons_clip
from mapchete.commons import contours as commons_contours
from mapchete.commons import hillshade as commons_hillshade
from mapchete.config import get_process_func
from mapchete.errors import MapcheteNodataTile, MapcheteProcessException
from mapchete.io import raster
from mapchete.tile import BufferedTile
from mapchete._timer import Timer

logger = logging.getLogger(__name__)


ProcessInfo = namedtuple('ProcessInfo', 'tile processed process_msg written write_msg')


class TileProcess():

    def __init__(self, tile=None, config=None):
        if isinstance(tile, tuple):
            tile = config.process_pyramid.tile(*tile)
        elif isinstance(tile, BufferedTile):
            pass
        else:
            raise TypeError("process_tile must be tuple or BufferedTile")
        self.tile = tile
        self.config_zoom_levels = config.zoom_levels
        self.config_baselevels = config.baselevels
        self.process_path = config.process_path
        self.config_dir = config.config_dir
        if self.tile.zoom in self.config_zoom_levels:
            self.input = config.get_inputs_for_tile(tile)
            self.process_func_params = config.get_process_func_params(tile.zoom)
        else:
            self.input, self.process_func_params = {}, {}
        self.mode = config.mode
        self.output_reader = config.output_reader
        self.skip = config.mode == "continue" and self.output_reader.tiles_exist(tile)

    def execute(self):
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
        if self.mode not in ["memory", "continue", "overwrite"]:
            raise ValueError("process mode must be memory, continue or overwrite")

        if self.tile.zoom not in self.config_zoom_levels:
            return self.output_reader.empty(self.tile)

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
                        output_reader=self.output_reader,
                        tile=self.tile,
                        params=self.process_func_params,
                        input=self.input
                    ),
                    # only pass on kwargs which are defined in execute()
                    **self.process_func_params
                )
        except Exception as e:
            # Log process time
            logger.exception((self.tile.id, "exception in user process", e, str(t)))
            new = MapcheteProcessException(format_exc())
            new.old = e
            raise new

        return process_data

    def _interpolate_from_baselevel(self, baselevel=None):
        with Timer() as t:
            # resample from parent tile
            if baselevel == "higher":
                parent_tile = self.tile.get_parent()
                process_data = raster.resample_from_array(
                    self.output_reader.read(parent_tile),
                    in_affine=parent_tile.affine,
                    out_tile=self.tile,
                    resampling=self.config_baselevels["higher"],
                    nodataval=self.output_reader.nodata
                )
            # resample from children tiles
            elif baselevel == "lower":
                mosaic = raster.create_mosaic([
                    (child_tile, self.output_reader.read(child_tile), )
                    for child_tile in self.config_baselevels["tile_pyramid"].tile(
                        *self.tile.id
                    ).get_children()
                ])
                process_data = raster.resample_from_array(
                    in_raster=mosaic.data,
                    in_affine=mosaic.affine,
                    out_tile=self.tile,
                    resampling=self.config_baselevels["lower"],
                    nodataval=self.output_reader.nodata
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
        self, tile=None, output_reader=None, params=None, input=None
    ):
        """Initialize Mapchete process."""
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""
        self.tile = tile
        self.tile_pyramid = tile.tile_pyramid
        self.output_reader = output_reader
        self.output_pyramid = output_reader.pyramid
        self.params = params
        self.input = input

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
        if self.tile.pixelbuffer > self.output_pyramid.pixelbuffer:
            output_tiles = list(self.output_pyramid.tiles_from_bounds(
                self.tile.bounds, self.tile.zoom
            ))
        else:
            output_tiles = self.output_pyramid.intersecting(self.tile)
        return self.output_reader.extract_subset(
            input_data_tiles=[
                (output_tile, self.output_reader.read(output_tile))
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
        if kwargs:
            warnings.warn(
                'Using kwargs such as "resampling" in open() is deprecated.'
                'Such options should be passed on in the respective read() functions'
            )
        if input_id not in self.input:
            raise ValueError("%s not found in config as input file" % input_id)
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


#############################################################
# wrappers helping to abstract multiprocessing and billiard #
#############################################################

class Executor():
    """
    Wrapper class to be used with multiprocessing or billiard.
    """
    def __init__(
        self,
        start_method="spawn",
        max_workers=None,
        multiprocessing_module=multiprocessing
    ):
        self.start_method = start_method
        self.max_workers = max_workers or os.cpu_count()
        self.multiprocessing_module = multiprocessing_module
        logger.debug(
            "init %s Executor with start_method %s and %s workers",
            self.multiprocessing_module, self.start_method, self.max_workers
        )

    def as_completed(
        self,
        func=None,
        iterable=None,
        fargs=None,
        fkwargs=None,
        chunksize=1
    ):
        fargs = fargs or []
        fkwargs = fkwargs or {}
        logger.debug(
            "open multiprocessing.Pool and %s %s workers",
            self.start_method, self.max_workers
        )
        with self.multiprocessing_module.get_context(self.start_method).Pool(
            self.max_workers
        ) as pool:
            for finished_task in pool.imap_unordered(
                partial(_exception_wrapper, func, fargs, fkwargs),
                iterable,
                chunksize=chunksize
            ):
                yield finished_task
            logger.debug("closing %s and workers", pool)
            pool.close()
            pool.join()
        logger.debug("%s closed", pool)


class FinishedTask():
    """
    Wrapper class to encapsulate exceptions.
    """
    def __init__(self, func, fargs=None, fkwargs=None):
        fargs = fargs or []
        fkwargs = fkwargs or {}
        try:
            self._result = func(*fargs, **fkwargs)
            self._exception = None
        except Exception as e:
            self._result = None
            self._exception = e

    def result(self):
        if self._exception:
            raise self._exception
        else:
            return self._result

    def exception(self):
        return self._exception

    def __repr__(self):
        return "FinishedTask(result=%s, exception=%s)" % (self._result, self._exception)


def _exception_wrapper(func, fargs, fkwargs, i):
    """Wraps function around FinishedTask object."""
    return FinishedTask(func, list(chain([i], fargs)), fkwargs)


###########################
# batch execution options #
###########################

def _run_on_single_tile(process=None, tile=None):
    logger.debug("run process on single tile")
    return _execute_and_write(
        tile_process=TileProcess(tile=tile, config=process.config),
        output_writer=process.config.output
    )


def _run_with_multiprocessing(
    process=None,
    zoom_levels=None,
    multi=None,
    max_chunksize=None,
    multiprocessing_module=None,
    multiprocessing_start_method=None
):
    logger.debug("run concurrently")
    num_processed = 0
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    logger.debug("run process on %s tiles using %s workers", total_tiles, multi)
    with Timer() as t:
        executor = Executor(
            max_workers=multi,
            start_method=multiprocessing_start_method,
            multiprocessing_module=multiprocessing_module
        )
        write_in_parent = True

        # for output drivers requiring writing data in parent process
        if write_in_parent:
            for zoom in zoom_levels:
                for task in executor.as_completed(
                    func=_execute,
                    iterable=(
                        TileProcess(tile=process_tile, config=process.config)
                        for process_tile in process.get_process_tiles(zoom)
                    )
                ):
                    output_data, process_info = task.result()
                    process_info = _write(
                        process_info=process_info,
                        output_data=output_data,
                        output_writer=process.config.output,
                    )
                    num_processed += 1
                    logger.info("tile %s/%s finished", num_processed, total_tiles)
                    yield process_info

        # for output drivers which can write data in child processes
        else:
            for zoom in zoom_levels:
                for task in executor.as_completed(
                    func=_execute_and_write,
                    iterable=(
                        TileProcess(tile=process_tile, config=process.config)
                        for process_tile in process.get_process_tiles(zoom)
                    ),
                    fkwargs=dict(output_writer=process.config.output)
                ):
                    num_processed += 1
                    logger.info("tile %s/%s finished", num_processed, total_tiles)
                    yield task.result()
    logger.debug("%s tile(s) iterated in %s", str(num_processed), t)


def _run_without_multiprocessing(process=None, zoom_levels=None):
    logger.debug("run sequentially")
    num_processed = 0
    total_tiles = process.count_tiles(min(zoom_levels), max(zoom_levels))
    logger.debug("run process on %s tiles using 1 worker", total_tiles)
    with Timer() as t:
        for zoom in zoom_levels:
            for process_tile in process.get_process_tiles(zoom):
                process_info = _execute_and_write(
                    tile_process=TileProcess(tile=process_tile, config=process.config),
                    output_writer=process.config.output
                )
                num_processed += 1
                logger.info("tile %s/%s finished", num_processed, total_tiles)
                yield process_info
    logger.info("%s tile(s) iterated in %s", str(num_processed), t)


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
            output = tile_process.execute()
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
