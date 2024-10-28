from __future__ import annotations

import operator
import os
import warnings
from collections import OrderedDict
from typing import Any, List, Optional, Tuple, Type, Union

from distributed import Client
from pydantic import BaseModel, Field, NonNegativeInt
from shapely.geometry.base import BaseGeometry

from mapchete.bounds import Bounds
from mapchete.errors import MapcheteConfigError
from mapchete.path import MPath
from mapchete.types import BoundsLike, MPathLike, ZoomLevelsLike
from mapchete.validate import validate_values
from mapchete.zoom_levels import ZoomLevels
from mapchete.tile import MetatilingValue


class OutputConfigBase(BaseModel):
    format: str
    metatiling: MetatilingValue = 1
    pixelbuffer: NonNegativeInt = 0


class PyramidConfig(BaseModel):
    grid: Union[str, dict]
    metatiling: MetatilingValue = 1
    pixelbuffer: NonNegativeInt = 0


class DaskAdaptOptions(BaseModel):
    minimum: int = 0
    maximum: int = 20
    active: bool = True


class DaskSpecs(BaseModel):
    worker_cores: float = 1.0
    worker_cores_limit: float = 2.0
    worker_memory: float = 2.1
    worker_memory_limit: float = 12.0
    worker_threads: int = 2
    worker_environment: dict = Field(default_factory=dict)
    scheduler_cores: int = 1
    scheduler_cores_limit: float = 1.0
    scheduler_memory: float = 1.0
    image: Optional[str] = None
    adapt_options: DaskAdaptOptions = DaskAdaptOptions()


class DaskSettings(BaseModel):
    process_graph: bool = True
    max_submitted_tasks: int = 500
    chunksize: int = 100
    scheduler: Optional[str] = None
    client: Optional[Type[Client]] = None


class ProcessConfig(BaseModel, arbitrary_types_allowed=True):
    pyramid: PyramidConfig
    output: dict
    zoom_levels: Union[ZoomLevels, ZoomLevelsLike]
    process: Optional[Union[MPathLike, List[str]]] = None
    baselevels: Optional[dict] = None
    input: Optional[dict] = None
    config_dir: Optional[MPathLike] = None
    mapchete_file: Optional[MPathLike] = None
    area: Optional[Union[MPathLike, BaseGeometry]] = None
    area_crs: Optional[Union[dict, str]] = None
    bounds: Optional[Union[Bounds, BoundsLike]] = None
    bounds_crs: Optional[Union[dict, str]] = None
    process_parameters: dict = Field(default_factory=dict)
    dask_specs: Optional[DaskSpecs] = None

    @classmethod
    def reserved_parameters(cls) -> Tuple[str, ...]:
        return tuple(cls.model_fields.keys())

    @classmethod
    def map_to_new_config_dict(cls, config: dict) -> dict:
        """Takes an older style configuration and tries to convert it to a current version."""
        try:
            validate_values(config, [("output", dict)])
        except Exception as e:
            raise MapcheteConfigError(e)

        if "type" in config["output"]:  # pragma: no cover
            warnings.warn(
                DeprecationWarning("'type' is deprecated and should be 'grid'")
            )
            if "grid" not in config["output"]:
                config["output"]["grid"] = config["output"].pop("type")

        if "pyramid" not in config:
            warnings.warn(
                DeprecationWarning(
                    "'pyramid' needs to be defined in root config element."
                )
            )
            config["pyramid"] = dict(
                grid=config["output"]["grid"],
                metatiling=config.get("metatiling", 1),
                pixelbuffer=config.get("pixelbuffer", 0),
            )

        if "zoom_levels" not in config:
            warnings.warn(
                DeprecationWarning(
                    "use new config element 'zoom_levels' instead of 'process_zoom', "
                    "'process_minzoom' and 'process_maxzoom'"
                )
            )
            if "process_zoom" in config:
                config["zoom_levels"] = config["process_zoom"]
            elif all([i in config for i in ["process_minzoom", "process_maxzoom"]]):
                config["zoom_levels"] = dict(
                    min=config["process_minzoom"], max=config["process_maxzoom"]
                )
            else:
                raise MapcheteConfigError("process zoom levels not provided in config")

        if "bounds" not in config:
            if "process_bounds" in config:
                warnings.warn(
                    DeprecationWarning(
                        "'process_bounds' are deprecated and renamed to 'bounds'"
                    )
                )
                config["bounds"] = config["process_bounds"]
            else:
                config["bounds"] = None

        if "input" not in config:
            if "input_files" in config:
                warnings.warn(
                    DeprecationWarning(
                        "'input_files' are deprecated and renamed to 'input'"
                    )
                )
                config["input"] = config["input_files"]
            else:
                raise MapcheteConfigError("no 'input' found")

        elif "input_files" in config:
            raise MapcheteConfigError(
                "'input' and 'input_files' are not allowed at the same time"
            )

        if "process_file" in config:
            warnings.warn(
                DeprecationWarning(
                    "'process_file' is deprecated and renamed to 'process'"
                )
            )
            config["process"] = config.pop("process_file")

        process_parameters = config.get("process_parameters", {})
        for key in list(config.keys()):
            if key in cls.reserved_parameters():
                continue
            warnings.warn(
                "it puts the process parameter in the 'process_parameters' section, or it gets the warning again"
            )
            process_parameters[key] = config.pop(key)
        config["process_parameters"] = process_parameters

        return config

    @staticmethod
    def parse(
        input_config: Union[dict, MPathLike], strict: bool = False
    ) -> ProcessConfig:
        """Read config from file or dictionary and return validated configuration"""

        def _include_env(d: dict) -> OrderedDict:
            """Search for environment variables and add their values."""
            out = OrderedDict()
            for k, v in d.items():
                if isinstance(v, dict):
                    out[k] = _include_env(v)
                elif isinstance(v, str) and v.startswith("${") and v.endswith("}"):
                    envvar = v.lstrip("${").rstrip("}")
                    out[k] = os.environ.get(envvar)
                else:
                    out[k] = v
            return out

        def _config_to_dict(input_config: Union[dict, MPathLike]) -> dict:
            """Convert a file or dictionary to a config dictionary."""
            if isinstance(input_config, dict):
                if "config_dir" not in input_config:
                    raise MapcheteConfigError("config_dir parameter missing")
                return OrderedDict(_include_env(input_config), mapchete_file=None)

            elif isinstance(input_config, (MPath, str)):
                config_path = MPath.from_inp(input_config)
                # from Mapchete file
                if config_path.suffix == ".mapchete":
                    config_dict = _include_env(config_path.read_yaml())
                    return OrderedDict(
                        config_dict,
                        config_dir=config_dict.get(
                            "config_dir",
                            config_path.absolute_path().dirname or os.getcwd(),
                        ),
                        mapchete_file=config_path,
                    )
            # throw error if unknown object
            raise MapcheteConfigError(
                "Configuration has to be a dictionary or a .mapchete file."
            )  # pragma: no cover

        if strict:  # pragma: no cover
            return ProcessConfig(**_config_to_dict(input_config))
        else:
            return ProcessConfig(
                **ProcessConfig.map_to_new_config_dict(_config_to_dict(input_config))
            )

    def to_dict(self) -> OrderedDict:
        return OrderedDict(self.model_dump())

    def raw_conf_per_zooms(self, zooms: ZoomLevels) -> OrderedDict:
        """Return parameter dictionary per zoom level."""
        return OrderedDict([(zoom, self.raw_conf_at_zoom(zoom)) for zoom in zooms])

    def raw_conf_at_zoom(self, zoom: int) -> OrderedDict:
        """Return parameter dictionary per zoom level."""

        def _yield_items():
            for name, element in self.to_dict().items():
                # input and process_parameters can be zoom dependent
                if name in ["input", "process_parameters"]:
                    out_element = _element_at_zoom(name, element, zoom)
                else:
                    out_element = element
                if out_element is not None:
                    yield name, out_element

        return OrderedDict(list(_yield_items()))

    def zoom_parameters(self, zoom: int) -> ZoomParameters:
        """Return parameter dictionary per zoom level."""
        return ZoomParameters(**self.raw_conf_at_zoom(zoom))


class ZoomParameters(BaseModel):
    input: OrderedDict = Field(default_factory=OrderedDict)
    process_parameters: OrderedDict = Field(default_factory=OrderedDict)


def _element_at_zoom(name: str, element: Any, zoom: int) -> Any:
    """
    Return the element filtered by zoom level.

    - An input integer or float gets returned as is.
    - An input string is checked whether it starts with "zoom". Then, the
      provided zoom level gets parsed and compared with the actual zoom
      level. If zoom levels match, the element gets returned.
    TODOs/gotchas:
    - Provided zoom levels for one element in config file are not allowed
      to "overlap", i.e. there is not yet a decision mechanism implemented
      which handles this case.
    """

    def _filter_by_zoom(element: Any, conf_string: str, zoom: int) -> Any:
        """Return element only if zoom condition matches with config string."""

        def _strip_zoom(input_string: str, strip_string: str) -> int:
            """Return zoom level as integer or throw error."""
            try:
                return int(input_string.strip(strip_string))
            except Exception as e:
                raise MapcheteConfigError("zoom level could not be determined: %s" % e)

        for op_str, op_func in [
            # order of operators is important:
            # prematurely return in cases of "<=" or ">=", otherwise
            # _strip_zoom() cannot parse config strings starting with "<"
            # or ">"
            ("=", operator.eq),
            ("<=", operator.le),
            (">=", operator.ge),
            ("<", operator.lt),
            (">", operator.gt),
        ]:
            if conf_string.startswith(op_str):
                return (
                    element if op_func(zoom, _strip_zoom(conf_string, op_str)) else None
                )

    # If element is a dictionary, analyze subitems.
    if isinstance(element, dict):
        zoom_keys = [key.startswith("zoom") for key in element.keys()]

        # we have a zoom level dependent structure here!
        if any(zoom_keys):
            if not all(zoom_keys):
                raise MapcheteConfigError(
                    f"when using zoom level dependent settings, all possible keys ({','.join(element.keys())}) must start with 'zoom'"
                )
            # iterate through sub elements
            values = []
            for sub_name, sub_element in element.items():
                out_element = _element_at_zoom(sub_name, sub_element, zoom)
                if out_element is not None:
                    values.append(out_element)

            if len(values) == 0:
                return None
            elif len(values) == 1:
                return values[0]

            raise MapcheteConfigError(
                f"multiple possible values configured for element '{name}' on zoom {zoom}"
            )

        # we have an input or output driver here
        elif "format" in element:
            return element

        # recoursively handle all other dict elements
        out_elements = OrderedDict()
        for sub_name, sub_element in element.items():
            out_element = _element_at_zoom(sub_name, sub_element, zoom)
            if out_element is not None:
                out_elements[sub_name] = out_element

        # If subelement is empty, return None
        return None if len(out_elements) == 0 else out_elements

    # If element is a zoom level statement, filter element.
    # filter out according to zoom filter definition
    elif isinstance(name, str) and name.startswith("zoom"):
        return _filter_by_zoom(
            conf_string=name.strip("zoom").strip(), zoom=zoom, element=element
        )

    # Return all other types as they are.
    return element
