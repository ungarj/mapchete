from typing import Any, Callable, Dict, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field


class Profiler(BaseModel):
    name: str
    decorator: Callable
    args: Tuple = Field(default_factory=tuple)
    kwargs: Dict[str, Any] = Field(default_factory=dict)


class Result(BaseModel):
    output: Any
    profiling: Dict = Field(default_factory=dict)
    exception: Optional[Exception] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)
