import logging

from mapchete.path import MPath
from mapchete.types import MPathLike

logger = logging.getLogger(__name__)


def write_json(path: MPathLike, params: dict, **kwargs) -> None:  # pragma: no cover
    """Write local or remote."""
    return MPath.from_inp(path, **kwargs).write_json(params)


def read_json(path: MPathLike, **kwargs) -> dict:
    """Read local or remote."""
    return MPath.from_inp(path, **kwargs).read_json()
