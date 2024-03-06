import logging
import warnings
from typing import Any, Optional

from mapchete.config.base import get_hash
from mapchete.path import MPath
from mapchete.processing.tasks import Task
from mapchete.tile import BufferedTile, BufferedTilePyramid
from mapchete.types import CRSLike

logger = logging.getLogger(__name__)


class InputTile:
    """
    Target Tile representation of input data.

    Parameters
    ----------
    tile : ``Tile``
    kwargs : keyword arguments
        driver specific parameters
    """

    preprocessing_tasks_results: dict
    input_key: str
    tile: BufferedTile

    def __init__(self, tile: BufferedTile, input_key: str, **kwargs):
        """Initialize."""
        self.tile = tile
        self.input_key = input_key
        self.preprocessing_tasks_results = {}

    def set_preprocessing_task_result(self, task_key: str, result: Any = None) -> None:
        """
        Adds a preprocessing task result.
        """
        self.preprocessing_tasks_results[task_key] = result

    def __enter__(self):
        """Required for 'with' statement."""
        return self

    def __exit__(self, *args):
        """Clean up."""


class InputData:
    """
    Base functionality for every driver.
    """

    # the input_key is used internally to identify the input
    input_key: str
    # process pyramid
    pyramid: BufferedTilePyramid
    # CRS of input
    crs: CRSLike
    # stores preprocessing tasks for collection later on
    preprocessing_tasks: dict
    # storage for results
    preprocessing_tasks_results: dict
    # driver metadata dictionary
    METADATA = {"driver_name": None, "data_type": None, "mode": "r"}

    def __init__(self, input_params: dict, input_key: Optional[str] = None, **kwargs):
        """Initialize relevant input information."""
        self.input_key = input_key or get_hash(input_params)
        self.pyramid = input_params.get("pyramid")

        # collect preprocessing tasks to be run by the Executor
        self.preprocessing_tasks = {}
        # storage location of all preprocessing tasks
        self.preprocessing_tasks_results = {}

        self.storage_options = input_params.get("abstract", {}).get(
            "storage_options", {}
        )

    def cleanup(self) -> None:
        """Optional cleanup function called when Mapchete exits."""

    def add_preprocessing_task(
        self, func, fargs=None, fkwargs=None, key=None, geometry=None, bounds=None
    ):
        """
        Add longer running preprocessing function to be called right before processing.

        Applied correctly this will speed up process initialization and if multiple tasks
        are required they run in parallel as they are being passed on the Executor.
        """
        fargs = fargs or ()
        if not isinstance(fargs, (tuple, list)):
            fargs = (fargs,)
        fkwargs = fkwargs or {}
        key = f"{func}-{get_hash((func, fargs, fkwargs))}" if key is None else key
        if self.input_key:
            key = f"{self.input_key}:{key}"
        if key in self.preprocessing_tasks:  # pragma: no cover
            raise KeyError(f"preprocessing task with key {key} already exists")
        logger.debug(f"add preprocessing task {key, func}")
        self.preprocessing_tasks[key] = Task(
            id=f"{key}",
            result_key_name=f"preprocessing_task-{key}_result",
            func=func,
            fargs=fargs,
            fkwargs=fkwargs,
            geometry=geometry,
            bounds=bounds,
        )

    def get_preprocessing_task_result(self, task_key):
        """
        Get result of preprocessing task.
        """
        if self.input_key and not task_key.startswith(f"{self.input_key}:"):
            task_key = f"{self.input_key}:{task_key}"
        if task_key not in self.preprocessing_tasks:
            raise KeyError(f"task {task_key} is not a task for current input")
        if task_key not in self.preprocessing_tasks_results:
            raise ValueError(f"task {task_key} has not yet been executed")
        return self.preprocessing_tasks_results[task_key]

    def set_preprocessing_task_result(self, task_key, result):
        """
        Set result of preprocessing task.
        """
        if self.input_key and not task_key.startswith(
            f"{self.input_key}:"
        ):  # pragma: no cover
            task_key = f"{self.input_key}:{task_key}"
        if task_key not in self.preprocessing_tasks:  # pragma: no cover
            raise KeyError(f"task {task_key} is not a task for current input")
        # The following part was commented out because on some rare occasions a
        # mapchete Hub job would fail because of this.
        # if task_key in self.preprocessing_tasks_results:  # pragma: no cover
        #     raise KeyError(f"task {task_key} has already been set")
        self.preprocessing_tasks_results[task_key] = result

    def preprocessing_task_finished(self, task_key):
        """
        Return whether preprocessing task already ran.
        """
        if self.input_key and not task_key.startswith(f"{self.input_key}:"):
            task_key = f"{self.input_key}:{task_key}"
        if task_key not in self.preprocessing_tasks:  # pragma: no cover
            raise KeyError(f"task {task_key} is not a task for current input")
        return task_key in self.preprocessing_tasks_results


class OutputData:
    # indicate whether output must be written in parent process
    write_in_parent_process = False
    # output pyramid
    pyramid: BufferedTilePyramid
    # output crs
    crs: CRSLike
    # output path
    path: MPath
    # parameters used to initialize output
    output_params: dict

    def __init__(self, output_params: dict, readonly: bool = False, **kwargs):
        """Initialize."""
        pixelbuffer = output_params.get("pixelbuffer", 0)
        metatiling = output_params.get("metatiling", 1)
        if "type" in output_params:  # pragma: no cover
            warnings.warn(
                DeprecationWarning("'type' is deprecated and should be 'grid'")
            )
            if "grid" not in output_params:
                output_params["grid"] = output_params.pop("type")
        self.pyramid = BufferedTilePyramid(
            grid=output_params["grid"],
            metatiling=metatiling,
            pixelbuffer=pixelbuffer,
        )
        self.crs = self.pyramid.crs
        self.storage_options = output_params.get("storage_options")
        path = output_params.get("path")
        self.path = path
        self.output_params = output_params

    def prepare(self, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass

    @property
    def stac_path(self) -> MPath:
        """Return path to STAC JSON file."""
        if self.path is None:
            raise ValueError("so output path set")
        return self.path / f"{self.stac_item_id}.json"

    @property
    def stac_item_id(self) -> str:
        """
        Return STAC item ID according to configuration.

        Defaults to path basename.
        """
        return self.output_params.get("stac", {}).get("id") or self.path.stem

    @property
    def stac_item_metadata(self):
        """Custom STAC metadata."""
        return self.output_params.get("stac", {})

    @property
    def stac_asset_type(self):  # pragma: no cover
        """Asset MIME type."""
        raise ValueError("no MIME type set for this output")


class OutputDataWriter(OutputData):
    pass
