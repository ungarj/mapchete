import importlib
import inspect
import logging
import py_compile
import sys
import warnings
from tempfile import NamedTemporaryFile
from typing import Any, Dict

from mapchete.config.models import ZoomParameters
from mapchete.errors import (
    MapcheteConfigError,
    MapcheteProcessImportError,
    MapcheteProcessSyntaxError,
)
from mapchete.log import add_module_logger
from mapchete.path import MPath, MPathLike, absolute_path

logger = logging.getLogger(__name__)


class ProcessFunc:
    """Abstraction class for a user process function.

    The user process can either be provided as a python module path, a file path
    or the source code as a list of strings.
    """

    path: MPathLike = None
    name: str = None

    def __init__(self, src, config_dir=None, run_compile=True):
        self._src = src
        # for module paths and file paths
        if isinstance(src, (str, MPath)):
            if src.endswith(".py"):
                self.path = MPath.from_inp(src)
                self.name = self.path.name.split(".")[0]
            else:
                self.path = src
                self.name = self.path.split(".")[-1]

        # for process code within configuration
        else:
            self.name = "custom_process"

        self._run_compile = run_compile
        self._root_dir = config_dir

        # this also serves as a validation step for the function
        logger.debug("validate process function")
        func = self._load_func()
        self.function_parameters = dict(**inspect.signature(func).parameters)

    def __call__(self, *args, **kwargs: Any) -> Any:
        kwargs = self.filter_parameters(kwargs)
        return self._load_func()(*args, **kwargs)

    def analyze_parameters(
        self, parameters_per_zoom: Dict[int, ZoomParameters]
    ) -> None:
        for zoom, config_parameters in parameters_per_zoom.items():
            # make sure parameters with no defaults are present in configuration, except of magical "mp" object
            for name, param in self.function_parameters.items():
                if param.default == inspect.Parameter.empty and name not in [
                    "mp",
                    "kwargs",
                    "__",
                ]:
                    if (
                        name not in config_parameters.input
                        and name not in config_parameters.process_parameters
                    ):
                        raise MapcheteConfigError(
                            f"zoom {zoom}: parameter '{name}' is required by process function but not provided in the process configuration"
                        )
            # make sure there is no intersection between process parameters and input keys
            param_intersection = set(config_parameters.input.keys()).intersection(
                set(config_parameters.process_parameters.keys())
            )
            if param_intersection:
                raise MapcheteConfigError(
                    f"zoom {zoom}: parameters {', '.join(list(param_intersection))} are provided as both input names as well as process parameter names"
                )
            # warn if there are process parameters not available in the process
            for param_name in config_parameters.process_parameters.keys():
                if param_name not in self.function_parameters:
                    warnings.warn(
                        f"zoom {zoom}: parameter '{param_name}' is set in the process configuration but not a process function parameter"
                    )

    def filter_parameters(self, kwargs):
        """Return function kwargs."""
        return {
            k: v
            for k, v in kwargs.items()
            if k in self.function_parameters and v is not None
        }

    def _load_func(self):
        """Import and return process function."""
        logger.debug(f"get process function from {self.name}")
        process_module = self._load_module()
        try:
            if hasattr(process_module, "execute"):
                return process_module.execute
            else:
                raise ImportError("No execute() function found in %s" % self._src)
        except ImportError as e:
            raise MapcheteProcessImportError(e)

    def _load_module(self):
        # path to python file or python module path
        if self.path:
            return self._import_module_from_path(self.path)
        # source code as list of strings
        else:
            with NamedTemporaryFile(suffix=".py") as tmpfile:
                logger.debug(f"writing process code to temporary file {tmpfile.name}")
                with open(tmpfile.name, "w") as dst:
                    for line in self._src:
                        dst.write(line + "\n")
                return self._import_module_from_path(
                    MPath.from_inp(tmpfile.name),
                )

    def _import_module_from_path(self, path):
        if path.endswith(".py"):
            module_path = absolute_path(path=path, base_dir=self._root_dir)
            if not module_path.exists():
                raise MapcheteConfigError(f"{module_path} is not available")
            try:
                if self._run_compile:
                    py_compile.compile(module_path, doraise=True)
                module_name = module_path.stem
                # load module
                spec = importlib.util.spec_from_file_location(
                    module_name, str(module_path)
                )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                # required to make imported module available using multiprocessing
                sys.modules[module_name] = module
                # configure process file logger
                add_module_logger(module.__name__)
            except py_compile.PyCompileError as e:
                raise MapcheteProcessSyntaxError(e)
            except ImportError as e:
                raise MapcheteProcessImportError(e)
        else:
            try:
                module = importlib.import_module(str(path))
            except ImportError as e:
                raise MapcheteProcessImportError(e)

        logger.debug(f"return process func: {module}")

        return module
