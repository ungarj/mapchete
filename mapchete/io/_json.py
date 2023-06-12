import json
import logging

from mapchete.path import MPath

logger = logging.getLogger(__name__)


def write_json(path, params, fs=None, **kwargs):
    """Write local or remote."""
    logger.debug(f"write {params} to {path}")
    path = MPath.from_inp(path, fs=fs, **kwargs)
    path.parent.makedirs()
    with path.open(mode="w") as dst:
        json.dump(params, dst, sort_keys=True, indent=4)


def read_json(path, fs=None, **kwargs):
    """Read local or remote."""
    path = MPath.from_inp(path, fs=fs, **kwargs)
    try:
        with path.open(mode="r") as src:
            return json.loads(src.read())
    except Exception as e:
        if path.exists():  # pragma: no cover
            logger.exception(e)
            raise e
        else:
            raise FileNotFoundError(f"{str(path)} not found")
