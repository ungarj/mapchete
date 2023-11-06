from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Profiler:
    name: str
    ctx: AbstractContextManager
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)


@dataclass
class Result:
    output: Any
    profiling: dict = field(default_factory=dict)
    exception: Optional[Exception] = None
