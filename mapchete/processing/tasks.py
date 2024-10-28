import logging
from abc import ABC
from enum import Enum
from functools import cached_property
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)
from uuid import uuid4

import numpy.ma as ma
from dask.delayed import Delayed, DelayedLeaf, delayed
from shapely.geometry import Polygon, base, box, mapping, shape
from shapely.ops import unary_union

from mapchete.bounds import Bounds
from mapchete.config import MapcheteConfig
from mapchete.config.process_func import ProcessFunc
from mapchete.errors import (
    MapcheteNodataTile,
    MapcheteProcessOutputError,
    NoTaskGeometry,
)
from mapchete.executor.base import Profiler
from mapchete.geometry import to_shape
from mapchete.io import raster
from mapchete.io.vector import IndexedFeatures
from mapchete.path import MPath
from mapchete.processing.mp import MapcheteProcess
from mapchete.processing.types import TaskInfo, default_tile_task_id
from mapchete.tile import BufferedTile
from mapchete.timer import Timer
from mapchete.types import BoundsLike, TileLike
from mapchete.validate import validate_bounds
from mapchete.zoom_levels import ZoomLevels

logger = logging.getLogger(__name__)


class Task(ABC):
    """Generic processing task.

    Can optionally have spatial properties attached which helps building up dependencies
    between tasks.
    """

    id: str
    func: Callable
    fargs: Tuple
    fkwargs: dict
    dependencies: Dict[str, TaskInfo]
    result_key_name: str
    geometry: Optional[Union[base.BaseGeometry, dict]] = None
    bounds: Optional[Bounds] = None
    tile: Optional[BufferedTile] = None

    def __init__(
        self,
        func: Callable,
        id: Optional[str] = None,
        fargs: Optional[Tuple] = None,
        fkwargs: Optional[dict] = None,
        dependencies: Optional[Dict[str, TaskInfo]] = None,
        result_key_name: Optional[str] = None,
        geometry: Optional[Union[base.BaseGeometry, dict]] = None,
        bounds: Optional[BoundsLike] = None,
    ):
        self.func = func
        self.id = id or uuid4().hex
        self.fargs = fargs or ()
        self.fkwargs = fkwargs or {}
        self.dependencies = dependencies or {}
        self.result_key_name = result_key_name or f"{self.id}_result"
        if geometry and bounds:
            raise ValueError("only provide one of either 'geometry' or 'bounds'")
        elif geometry:
            self.geometry = to_shape(geometry)
            self.bounds = validate_bounds(self.geometry.bounds)
        elif bounds:
            self.bounds = validate_bounds(bounds)
            self.geometry = box(*self.bounds)
        else:
            self.bounds, self.geometry = None, None

    def __repr__(self):  # pragma: no cover
        return f"Task(id={self.id}, bounds={self.bounds})"

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "bounds": self.bounds,
            "geometry": mapping(self.geometry) if self.geometry else None,
            "properties": {},
        }

    def add_dependencies(self, dependencies: Dict[str, TaskInfo]) -> None:
        dependencies = dependencies or {}
        if not isinstance(dependencies, dict):
            raise TypeError(
                f"dependencies must be a dictionary, not {type(dependencies)}"
            )
        self.dependencies.update(dependencies)

    def execute(self, dependencies: Optional[Dict[str, TaskInfo]] = None) -> Any:
        return self.func(*self.fargs, **self.fkwargs)

    def has_geometry(self) -> bool:
        return self.geometry is not None

    @property
    def __geo_interface__(self) -> mapping:
        if self.has_geometry():
            return mapping(self.geometry)
        else:
            raise NoTaskGeometry(f"{self} has no geo information assigned")


def _execute_task_wrapper(task, **kwargs) -> Any:  # pragma: no cover
    return task.execute(**kwargs)


class TaskBatch(ABC):
    tasks: IndexedFeatures
    id: str
    func: Callable
    fkwargs: dict
    profilers: List[Profiler]

    def __init__(
        self,
        tasks: Iterator[Task],
        id: Optional[str] = None,
        func: Optional[Callable] = None,
        fkwargs: Optional[dict] = None,
        profilers: Optional[List[Profiler]] = None,
    ):
        if tasks is None:  # pragma: no cover
            raise TypeError("TaskBatch requires at least one Task")
        self.id = id or uuid4().hex
        self.tasks = IndexedFeatures(
            (self._validate(t) for t in tasks), allow_non_geo_objects=True
        )
        self.bounds = self.tasks.bounds
        self.func = func or _execute_task_wrapper
        self.fkwargs = fkwargs or {}
        self.profilers = profilers or []

    @cached_property
    def geometry(self) -> base.BaseGeometry:
        if self.tasks:
            return unary_union([shape(task) for task in self.tasks])
        else:
            return Polygon()

    @property
    def __geo_interface__(self) -> mapping:
        return mapping(self.geometry)

    def __repr__(self):  # pragma: no cover
        return f"TaskBatch(id={self.id}, bounds={self.bounds}, tasks={len(self.tasks)})"

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

    def intersection(self, other: Union[Task, Tuple, List]):
        if isinstance(other, Task):
            return self.tasks.filter(other.bounds)
        elif isinstance(other, (tuple, list)):
            return self.tasks.filter(validate_bounds(other))
        else:
            raise TypeError(
                f"intersection only works with other Task instances or bounds, not {other}"
            )

    def _validate(self, item: Task):
        if isinstance(item, Task):
            return item
        else:
            raise TypeError("TaskBatch items must be Taskss, not %s", type(item))


class InterpolateFrom(str, Enum):
    lower = "lower"
    higher = "higher"


def _execute_tile_task_wrapper(task, **kwargs) -> Any:  # pragma: no cover
    return task.execute(**kwargs)


class TileTask(Task):
    """
    Class to process on a specific process tile.
    """

    config_zoom_levels: ZoomLevels
    config_baselevels: ZoomLevels
    process = Optional[ProcessFunc]
    config_dir = Optional[MPath]
    tile: BufferedTile
    _dependencies: dict

    def __init__(
        self,
        tile: TileLike,
        id: Optional[str] = None,
        config: Optional[MapcheteConfig] = None,
        func: Optional[Callable] = None,
        dependencies: Optional[dict] = None,
    ):
        """Set attributes depending on baselevels or not."""
        self.tile = (
            config.process_pyramid.tile(*tile) if isinstance(tile, tuple) else tile
        )
        _default_id = default_tile_task_id(tile)
        self.id = id or _default_id
        self.func = func or _execute_tile_task_wrapper
        self.config_zoom_levels = config.zoom_levels
        self.config_baselevels = config.baselevels
        self.process = config.process
        self.config_dir = config.config_dir
        if (
            self.tile.zoom not in self.config_zoom_levels
            or self.tile.zoom in self.config_baselevels
        ):
            self.input, self.process_func_params, self.output_params = {}, {}, {}
        else:
            self.input = config.get_inputs_for_tile(tile)
            self.process_func_params = config.get_process_func_params(tile.zoom)
            self.output_params = config.output_reader.output_params
        self.mode = config.mode
        self.output_reader = config.output_reader if config.baselevels else None
        self._dependencies = dict()
        super().__init__(self.func, id=self.id, geometry=tile.bbox)

    def __repr__(self):  # pragma: no cover
        return f"TileTask(id={self.id}, tile={self.tile}, bounds={self.bounds})"

    def add_dependency(self, task_key: str, result: Any, raise_error: bool = True):
        """Append preprocessing task result to input."""
        # if dependency has geo information, only add if it intersects with task!
        try:
            if not shape(result).intersects(shape(self)):
                logger.debug("dependency does not intersect with task")
                return
        except AttributeError:
            pass

        if ":" in task_key:
            inp_key, inp_task_key = task_key.split(":")[:2]
        else:  # pragma: no cover
            raise KeyError(
                "preprocessing task cannot be assigned to an input "
                f"because of a malformed task key: {task_key}"
            )

        input_keys = {inp.input_key for inp in self.input.values()}
        if inp_key not in input_keys:  # pragma: no cover
            if raise_error:
                raise KeyError(
                    f"task {inp_task_key} cannot be assigned to input with key {inp_key} "
                    f"(available keys: {input_keys})"
                )
            else:
                return

        logger.debug("remember preprocessing task (%s) result for execution", task_key)
        self._dependencies[task_key] = result

    def execute(self, dependencies: Optional[dict] = None) -> Any:
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
            raise ValueError(
                f"process mode must be memory, continue or overwrite, not {self.mode}"
            )

        if self.tile.zoom not in self.config_zoom_levels:
            raise MapcheteNodataTile

        dependencies = dependencies or {}
        process_output = self._execute(dependencies=dependencies)
        if isinstance(process_output, str) and process_output == "empty":
            raise MapcheteNodataTile
        elif process_output is None:
            raise MapcheteProcessOutputError("process output is empty")
        return process_output

    def _execute(self, dependencies: Optional[Dict[str, TaskInfo]] = None) -> Any:
        # If baselevel is active and zoom is outside of baselevel,
        # interpolate from other zoom levels.
        if self.config_baselevels:
            if self.tile.zoom < min(self.config_baselevels["zooms"]):
                return self._interpolate_from_baselevel("lower", dependencies)
            elif self.tile.zoom > max(self.config_baselevels["zooms"]):
                return self._interpolate_from_baselevel("higher", dependencies)
        # Otherwise, execute from process file.
        try:
            with Timer() as duration:
                if self._dependencies:
                    dependencies.update(self._dependencies)
                # append dependent preprocessing task results to input objects
                if dependencies:
                    for task_key, task_result in dependencies.items():
                        if isinstance(task_result, TaskInfo):
                            task_result = task_result.output
                        if not task_key.startswith("tile_task"):
                            inp_key, task_key = (
                                task_key.split(":")[0],
                                ":".join(task_key.split(":")[1:]),
                            )
                            if task_key in [None, ""]:  # pragma: no cover
                                raise KeyError(f"malformed task key: {inp_key}")
                            for inp in self.input.values():
                                if inp.input_key == inp_key:
                                    inp.set_preprocessing_task_result(
                                        task_key=task_key, result=task_result
                                    )
                # Actually run process.
                mp = MapcheteProcess(
                    tile=self.tile,
                    params=self.process_func_params,
                    input=self.input,
                    output_params=self.output_params,
                )
                # this contains key: params mapping, where under param.annotation we can inspect for target type
                extended_kwargs = dict(
                    self.process_func_params,
                    mp=mp,
                    **{k: v for k, v in self.input.items()},
                )
                process_data = self.process(
                    **extended_kwargs,
                )
        except MapcheteNodataTile:
            raise
        except Exception as e:
            # Log process time and tile
            logger.error((self.tile.id, "exception in user process", e, str(duration)))
            raise

        return process_data

    def _interpolate_from_baselevel(
        self,
        baselevel: InterpolateFrom,
        dependencies: Optional[Dict[str, TaskInfo]] = None,
    ) -> ma.MaskedArray:
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
            if baselevel == InterpolateFrom.higher:
                parent_tile = self.tile.get_parent()
                process_data = raster.resample_from_array(
                    self.output_reader.read(parent_tile),
                    in_affine=parent_tile.affine,
                    out_tile=self.tile,
                    resampling=self.config_baselevels["higher"],
                    nodata=self.output_reader.output_params["nodata"],
                )
            # resample from children tiles
            elif baselevel == InterpolateFrom.lower:
                src_tiles = {}
                for task_info in dependencies.values():
                    logger.debug("reading output from dependend tasks")
                    for output_tile in self.output_reader.pyramid.intersecting(
                        task_info.tile
                    ):
                        if task_info.output is not None:
                            src_tiles[output_tile] = raster.extract_from_array(
                                array=task_info.output,
                                in_affine=task_info.tile.affine,
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
                    array=raster.create_mosaic(
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

    def __init__(
        self,
        tasks=None,
        id=None,
        func=None,
        fkwargs=None,
        profilers: Optional[List[Profiler]] = None,
    ):
        self.id = id or uuid4().hex
        self.bounds = None, None, None, None
        self.tasks = {item.tile: item for item in self._validate(tasks)}
        self._update_bounds()
        self.func = func or _execute_tile_task_wrapper
        self.fkwargs = fkwargs or {}
        self.profilers = profilers or []

    @cached_property
    def geometry(self) -> base.BaseGeometry:
        if self.tasks:
            return unary_union([task.bbox for task in self.tasks])
        else:  # pragma: no cover
            return Polygon()

    def __repr__(self):  # pragma: no cover
        return f"TileTaskBatch(id={self.id}, bounds={self.bounds}, tasks={len(self.tasks)})"

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

    def _validate(self, items: Iterator[TileTask]) -> Iterator[TileTask]:
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
            yield item


class Tasks:
    preprocessing_batches: List[TaskBatch]
    tile_batches: List[TileTaskBatch]

    def __init__(
        self,
        task_batches: Iterator[Union[TaskBatch, TileTaskBatch]],
    ):
        with Timer() as tt:
            self.preprocessing_batches = []
            self.tile_batches = []
            for batch in task_batches:
                if isinstance(batch, TileTaskBatch):
                    self.tile_batches.append(batch)
                else:
                    self.preprocessing_batches.append(batch)
        logger.debug("task batches materialized in %s", tt)

    def __len__(self):
        return sum([len(batch) for batch in self])

    def __iter__(self) -> Generator[Union[TaskBatch, TileTaskBatch], None, None]:
        for phase in (self.preprocessing_batches, self.tile_batches):
            for batch in phase:
                yield batch

    @property
    def preprocessing_tasks_count(self):
        return sum([len(batch) for batch in self.preprocessing_batches])

    @property
    def tile_tasks_count(self):
        return sum([len(batch) for batch in self.tile_batches])

    def to_dask_graph(
        self,
        preprocessing_task_wrapper: Optional[Callable] = None,
        tile_task_wrapper: Optional[Callable] = None,
    ) -> List[Union[Delayed, DelayedLeaf]]:
        """Return task graph to use with dask Executor."""
        tasks = {}
        previous_batch = None
        for batch in self:
            logger.debug("converting batch %s", batch)

            if isinstance(batch, TileTaskBatch):
                task_func = tile_task_wrapper or batch.func
            else:
                task_func = preprocessing_task_wrapper or batch.func

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

                tasks[task] = delayed(
                    task_func,
                    pure=True,
                    name=f"{task.id}",
                    traverse=len(dependencies) > 0,
                )(
                    task,
                    dependencies=dependencies,
                    **batch.fkwargs,
                    dask_key_name=f"{task.result_key_name}",
                )

            previous_batch = batch

        return list(tasks.values())

    def to_batch(self) -> Iterator[Task]:
        """Return all tasks as one batch."""
        for batch in self.to_batches():
            for task in batch:
                yield task

    def to_batches(self) -> Iterator[Iterator[Task]]:
        """Return batches of tasks."""
        return list(self)
