import logging
import os
from typing import Optional, Union

from fsspec import AbstractFileSystem

from mapchete.config import MapcheteConfig
from mapchete.executor import Executor, MFuture
from mapchete.formats import read_output_metadata
from mapchete.path import MPath, fs_from_path
from mapchete.processing import Mapchete, MapcheteProcess
from mapchete.tile import count_tiles
from mapchete.timer import Timer
from mapchete.types import MPathLike

__all__ = [
    "count_tiles",
    "Mapchete",
    "MapcheteProcess",
    "Timer",
    "Executor",
    "MFuture",
]
__version__ = "2024.2.0"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def open(
    some_input: Union[MPathLike, dict, MapcheteConfig],
    with_cache: bool = False,
    fs: Optional[AbstractFileSystem] = None,
    fs_kwargs: Optional[dict] = None,
    **kwargs,
) -> Mapchete:
    """
    Open a Mapchete process.

    Parameters
    ----------
    some_input : MapcheteConfig object, config dict, path to mapchete file or path to
        TileDirectory
        Mapchete process configuration
    mode : string
        * ``memory``: Generate process output on demand without reading
          pre-existing data or writing new data.
        * ``readonly``: Just read data without processing new data.
        * ``continue``: (default) Don't overwrite existing output.
        * ``overwrite``: Overwrite existing output.
    zoom : list or integer
        process zoom level or a pair of minimum and maximum zoom level
    bounds : tuple
        left, bottom, right, top process boundaries in output pyramid
    single_input_file : string
        single input file if supported by process
    with_cache : bool
        process output data cached in memory
    fs : fsspec FileSystem
        Any FileSystem object for the mapchete output.
    fs_kwargs : dict
        Special configuration parameters if FileSystem object has to be created.

    Returns
    -------
    Mapchete
        a Mapchete process object
    """
    # convert to MPath object if possible
    if isinstance(some_input, str):
        some_input = MPath.from_inp(some_input)
    # for TileDirectory inputs
    if isinstance(some_input, MPath) and some_input.suffix == "":
        logger.debug("assuming TileDirectory")
        metadata_json = MPath.from_inp(some_input).joinpath("metadata.json")
        fs_kwargs = fs_kwargs or {}
        fs = fs or fs_from_path(metadata_json, **fs_kwargs)
        logger.debug("read metadata.json")
        metadata = read_output_metadata(metadata_json, fs=fs)
        config = dict(
            process=None,
            input=None,
            pyramid=metadata["pyramid"].to_dict(),
            output=dict(
                {
                    k: v
                    for k, v in metadata["driver"].items()
                    if k not in ["delimiters", "mode"]
                },
                path=some_input,
                fs=fs,
                fs_kwargs=fs_kwargs,
                **kwargs,
            ),
            config_dir=os.getcwd(),
            zoom_levels=kwargs.get("zoom"),
        )
        kwargs.update(mode="readonly")
        return Mapchete(MapcheteConfig(config, **kwargs))
    # for dicts, .mapchete file paths or MpacheteConfig objects
    elif (
        isinstance(some_input, dict)
        or isinstance(some_input, MPath)
        and some_input.suffix == ".mapchete"
        or isinstance(some_input, MapcheteConfig)
    ):
        return Mapchete(MapcheteConfig(some_input, **kwargs), with_cache=with_cache)
    else:  # pragma: no cover
        raise TypeError(
            "can only open input in form of a mapchete file path, a TileDirectory path, "
            f"a dictionary or a MapcheteConfig object, not {type(some_input)}"
        )
