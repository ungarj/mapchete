from contextlib import contextmanager
from typing import Generator

import fiona

from mapchete.io.vector.read import fiona_read
from mapchete.io.vector.write import (
    fiona_write,
)
from mapchete.path import MPath
from mapchete.types import MPathLike


@contextmanager
def fiona_open(
    path: MPathLike, mode: str = "r", **kwargs
) -> Generator[
    fiona.Collection,
    None,
    None,
]:
    """Call fiona.open but set environment correctly and return custom writer if needed."""
    path = MPath.from_inp(path)

    if "w" in mode:
        with fiona_write(path, mode=mode, **kwargs) as dst:
            yield dst
    else:
        with fiona_read(path, mode=mode, **kwargs) as src:
            yield src
