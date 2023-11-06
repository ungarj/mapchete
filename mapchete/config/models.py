from typing import List, Tuple, Union

from pydantic import BaseModel, NonNegativeInt, field_validator
from shapely.geometry.base import BaseGeometry

from mapchete.types import Bounds, MPathLike, ZoomLevels


class OutputConfigBase(BaseModel):
    format: str
    metatiling: Union[int, None] = 1
    pixelbuffer: Union[NonNegativeInt, None] = 0

    @field_validator("metatiling", mode="before")
    def _metatiling(cls, value: int) -> int:  # pragma: no cover
        _metatiling_opts = [2**x for x in range(10)]
        if value not in _metatiling_opts:
            raise ValueError(f"metatling must be one of {_metatiling_opts}")
        return value


class PyramidConfig(BaseModel):
    grid: Union[str, dict]
    metatiling: Union[int, None] = 1
    pixelbuffer: Union[NonNegativeInt, None] = 0

    @field_validator("metatiling", mode="before")
    def _metatiling(cls, value: int) -> int:  # pragma: no cover
        _metatiling_opts = [2**x for x in range(10)]
        if value not in _metatiling_opts:
            raise ValueError(f"metatling must be one of {_metatiling_opts}")
        return value


class ProcessConfig(BaseModel, arbitrary_types_allowed=True):
    pyramid: PyramidConfig
    output: dict
    zoom_levels: Union[ZoomLevels, dict, list, int]
    process: Union[MPathLike, List[str], None] = None
    baselevels: Union[dict, None] = None
    input: Union[dict, None] = None
    config_dir: Union[MPathLike, None] = None
    area: Union[MPathLike, BaseGeometry, None] = None
    area_crs: Union[dict, str, None] = None
    bounds: Union[Bounds, Tuple[float, float, float, float], None] = None
    bounds_crs: Union[dict, str, None] = None
    process_parameters: Union[dict, None] = None
