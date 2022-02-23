from collections import namedtuple
from itertools import chain
import logging
from shapely.geometry import box, mapping
from traceback import format_exc
from uuid import uuid4

from mapchete._timer import Timer
from mapchete.config import get_process_func
from mapchete.errors import (
    MapcheteNodataTile,
    NoTaskGeometry,
    MapcheteProcessException,
    MapcheteProcessOutputError,
)
from mapchete.io import raster
from mapchete.io._geometry_operations import to_shape
from mapchete.io.vector import IndexedFeatures
from mapchete.validate import validate_bounds
from mapchete._user_process import MapcheteProcess


logger = logging.getLogger(__name__)


TaskResult = namedtuple(
    "TaskResult",
    "task_id tile processed process_msg result",
    defaults=(None, None, None, None, None),
)


class Task:
    """Generic processing task.

    Can optionally have spatial properties attached which helps building up dependencies
    between tasks.
    """

    def __init__(
        self,
        id=None,
        func=None,
        fargs=None,
        fkwargs=None,
        geometry=None,
        bounds=None,
        dependencies=None,
    ):
        self.id = id or uuid4().hex
        self.func = func
        self.fargs = fargs or ()
        self.fkwargs = fkwargs or {}
        self.dependencies = dependencies or {}
        if geometry and bounds:
            raise ValueError("only provide one of either 'geometry' or 'bounds'")
        elif geometry:
            self.geometry = to_shape(geometry)
            self.bounds = self.geometry.bounds
        elif bounds:
            self.bounds = validate_bounds(bounds)
            self.geometry = box(*self.bounds)
        else:
            self.bounds, self.geometry = None, None

    def __repr__(self):  # pragma: no cover
        return f"Task(id={self.id}, bounds={self.bounds})"

    def to_dict(self):
        return {
            "id": self.id,
            "bounds": self.bounds,
            "geometry": mapping(self.geometry) if self.geometry else None,
            "properties": {},
        }

    def add_dependencies(self, dependencies):
        dependencies = dependencies or {}
        if not isinstance(dependencies, dict):
            raise TypeError(
                f"dependencies must be a dictionary, not {type(dependencies)}"
            )
        self.dependencies.update(dependencies)

    def execute(self, dependencies=None):
        return self.func(*self.fargs, **self.fkwargs)

    def has_geometry(self):
        return self.geometry is not None

    @property
    def __geo_interface__(self):
        if self.has_geometry():
            return mapping(self.geometry)
        else:
            raise NoTaskGeometry(f"{self} has no geo information assigned")


def _execute_task_wrapper(task, **kwargs):
    return task.execute(**kwargs)


class TaskBatch:
    def __init__(self, tasks=None, id=None, func=None, fkwargs=None):
        if tasks is None:
            raise TypeError("TaskBatch requires at least one Task")
        self.id = id or uuid4().hex
        self.tasks = IndexedFeatures(
            (self._validate(t) for t in tasks), allow_non_geo_objects=True
        )
        self.bounds = self.tasks.bounds
        self.func = func or _execute_task_wrapper
        self.fkwargs = fkwargs or {}

    def __repr__(self):  # pragma: no cover
        return f"TaskBatch(id={self.id}, bounds={self.bounds})"

    def __iter__(self):
        return iter(self.tasks.values())

    def __len__(self):
        return len(self.tasks)

    def items(self):
        return self.tasks.items()

    def keys(self):
        return self.tasks.keys()

    def values(self):
        return self.tasks.values()

    def intersection(self, other):
        if isinstance(other, Task):
            return self.tasks.filter(other.bounds)
        elif isinstance(other, (tuple, list)):
            return self.tasks.filter(validate_bounds(other))
        else:
            raise TypeError(
                f"intersection only works with other Task instances or bounds, not {other}"
            )

    def _validate(self, item):
        if isinstance(item, Task):
            return item
        else:
            raise TypeError("TaskBatch items must be Taskss, not %s", type(item))


class TileTask(Task):
    """
    Class to process on a specific process tile.

    If skip is set to True, all attributes will be set to None.
    """

    def __init__(self, tile=None, id=None, config=None, skip=False, dependencies=None):
        """Set attributes depending on baselevels or not."""
        self.tile = (
            config.process_pyramid.tile(*tile) if isinstance(tile, tuple) else tile
        )
        self.id = id or f"tile_task_{self.tile.zoom}-{self.tile.row}-{self.tile.col}"
        self.skip = skip
        self.config_zoom_levels = None if skip else config.zoom_levels
        self.config_baselevels = None if skip else config.baselevels
        self.process = None if skip else config.process
        self.config_dir = None if skip else config.config_dir
        if (
            skip
            or self.tile.zoom not in self.config_zoom_levels
            or self.tile.zoom in self.config_baselevels
        ):
            self.input, self.process_func_params, self.output_params = {}, {}, {}
        else:
            self.input = config.get_inputs_for_tile(tile)
            self.process_func_params = config.get_process_func_params(tile.zoom)
            self.output_params = config.output_reader.output_params
        self.mode = None if skip else config.mode
        self.output_reader = (
            None if skip or not config.baselevels else config.output_reader
        )
        super().__init__(
            id=f"tile_task_{tile.zoom}-{tile.row}-{tile.col}", geometry=tile.bbox
        )

    def execute(self, dependencies=None):
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

        dependencies = dependencies or {}
        process_output = self._execute(dependencies=dependencies)
        if isinstance(process_output, str) and process_output == "empty":
            raise MapcheteNodataTile
        elif process_output is None:
            raise MapcheteProcessOutputError("process output is empty")
        return process_output

    def _execute(self, dependencies=None):
        # If baselevel is active and zoom is outside of baselevel,
        # interpolate from other zoom levels.
        if self.config_baselevels:
            if self.tile.zoom < min(self.config_baselevels["zooms"]):
                return self._interpolate_from_baselevel("lower", dependencies)
            elif self.tile.zoom > max(self.config_baselevels["zooms"]):
                return self._interpolate_from_baselevel("higher", dependencies)
        # Otherwise, execute from process file.
        process_func = get_process_func(
            process=self.process, config_dir=self.config_dir
        )
        try:
            with Timer() as duration:
                # append dependent preprocessing task results to input objects
                if dependencies:
                    for task_key, task_result in dependencies.items():
                        if not task_key.startswith("tile_task"):
                            inp_key, task_key = task_key.split(":")[0], ":".join(
                                task_key.split(":")[1:]
                            )
                            if task_key in [None, ""]:  # pragma: no cover
                                raise KeyError(f"malformed task key: {inp_key}")
                            for inp in self.input.values():
                                if inp.input_key == inp_key:
                                    inp.set_preprocessing_task_result(
                                        task_key=task_key, result=task_result.data
                                    )
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
            logger.exception(e)
            logger.error((self.tile.id, "exception in user process", e, str(duration)))
            new = MapcheteProcessException(format_exc())
            new.old = e
            raise new

        return process_data

    def _interpolate_from_baselevel(self, baselevel=None, dependencies=None):
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

        with Timer() as duration:
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
                src_tiles = {}
                for process_info in dependencies.values():
                    logger.debug("reading output from dependend tasks")
                    process_tile = process_info.tile
                    for output_tile in self.output_reader.pyramid.intersecting(
                        process_tile
                    ):
                        if process_info.data is not None:
                            src_tiles[output_tile] = raster.extract_from_array(
                                in_raster=process_info.data,
                                in_affine=process_tile.affine,
                                out_tile=output_tile,
                            )
                if self.output_reader.pyramid.pixelbuffer:  # pragma: no cover
                    # if there is a pixelbuffer around the output tiles, we need to read more child tiles
                    child_tiles = [
                        child_tile
                        for output_tile in output_tiles
                        for child_tile in self.output_reader.pyramid.tiles_from_bounds(
                            output_tile.bounds, output_tile.zoom + 1
                        )
                    ]
                else:
                    child_tiles = [
                        child_tile
                        for output_tile in output_tiles
                        for child_tile in output_tile.get_children()
                    ]
                for child_tile in child_tiles:
                    if child_tile not in src_tiles:
                        src_tiles[child_tile] = self.output_reader.read(child_tile)

                process_data = raster.resample_from_array(
                    in_raster=raster.create_mosaic(
                        [(src_tile, data) for src_tile, data in src_tiles.items()],
                        nodata=self.output_reader.output_params["nodata"],
                    ),
                    out_tile=self.tile,
                    resampling=self.config_baselevels["lower"],
                    nodata=self.output_reader.output_params["nodata"],
                )
        logger.debug((self.tile.id, "generated from baselevel", str(duration)))
        return process_data


class TileTaskBatch(TaskBatch):
    """Combines TileTask instances of same pyramid and zoom level into one batch."""

    def __init__(self, tasks=None, id=None, func=None, fkwargs=None):
        self.id = id or uuid4().hex
        self.bounds = None, None, None, None
        self.tasks = {tile: item for tile, item in self._validate(tasks)}
        self._update_bounds()
        self.func = func or _execute_task_wrapper
        self.fkwargs = fkwargs or {}

    def _update_bounds(self):
        for tile in self.tasks.keys():
            left, bottom, right, top = self.bounds
            self.bounds = (
                tile.left if left is None else min(left, tile.left),
                tile.bottom if bottom is None else min(bottom, tile.bottom),
                tile.right if right is None else max(right, tile.right),
                tile.top if top is None else max(top, tile.top),
            )

    def intersection(self, other):
        if isinstance(other, TileTask):
            if other.tile.zoom + 1 != self._zoom:  # pragma: no cover
                raise ValueError("intersecting tile has to be from zoom level above")
            return [
                self.tasks[child]
                for child in other.tile.get_children()
                if child in self.tasks
            ]

        if isinstance(other, Task):
            return self.intersection(other.bounds)

        elif isinstance(other, (tuple, list)):
            return [
                self.tasks[tile]
                for tile in self._tp.tiles_from_bounds(bounds=other, zoom=self._zoom)
                if tile in self.tasks
            ]

        else:
            raise TypeError(
                "intersections only works with other Task instances or bounds"
            )

    def _validate(self, items):
        self._tp = None
        self._zoom = None
        for item in items:
            if not isinstance(item, TileTask):  # pragma: no cover
                raise TypeError(
                    "TileTaskBatch items must be TileTasks, not %s", type(item)
                )
            if self._tp is None:
                self._tp = item.tile.buffered_tp
            elif item.tile.buffered_tp != self._tp:  # pragma: no cover
                raise TypeError("all TileTasks must derive from the same pyramid.")
            if self._zoom is None:
                self._zoom = item.tile.zoom
            elif item.tile.zoom != self._zoom:  # pragma: no cover
                raise TypeError("all TileTasks must lie on the same zoom level")
            yield item.tile, item


def to_dask_collection(batches):
    from dask.delayed import delayed

    tasks = {}
    previous_batch = None
    for batch in batches:
        logger.debug("converting batch %s", batch)
        if previous_batch:
            logger.debug("previous batch had %s tasks", len(previous_batch))
        for task in batch.values():
            if previous_batch:
                dependencies = {
                    child.id: tasks[child]
                    for child in previous_batch.intersection(task)
                }
                logger.debug(
                    "found %s dependencies from last batch for task %s",
                    len(dependencies),
                    task,
                )
            else:
                dependencies = {}
            tasks[task] = delayed(batch.func)(
                task, dependencies=dependencies, **batch.fkwargs
            )
        previous_batch = batch
    return list(tasks.values())
