"""
Useful tools to facilitate testing.
"""

import logging
from typing import Optional
import uuid
from collections import OrderedDict

import oyaml as yaml
from shapely.ops import unary_union

import mapchete
from mapchete.config.base import initialize_inputs, open_inputs
from mapchete.executor import SequentialExecutor
from mapchete.path import MPath
from mapchete.tile import BufferedTilePyramid
from mapchete.types import MPathLike, TileLike

logger = logging.getLogger(__name__)


# helper functions
def dict_from_mapchete(path: MPathLike) -> dict:
    """
    Read mapchete configuration from file and return as dictionary.
    """
    path = MPath.from_inp(path)
    with path.open() as src:
        out = dict(yaml.safe_load(src.read()), config_dir=path.dirname)
        if "config_dir" in out:
            out["config_dir"] = MPath.from_inp(out["config_dir"])
        elif "path" in out.get("output", {}):  # pragma: no cover
            out["output"]["path"] = MPath.from_inp(out["output"])
    return out


def clear_dict(dictionary: dict) -> dict:
    def _convert(vv):
        out = OrderedDict()
        for k, v in vv.items():
            if isinstance(v, MPath):
                v = str(v)
            elif isinstance(v, dict):
                v = _convert(v)
            out[k] = v
        return out

    return _convert(dictionary)


class ProcessFixture:
    path: MPath
    basepath: MPath

    def __init__(
        self,
        path: MPathLike,
        tempdir: Optional[MPathLike] = None,
        inp_cache_tempdir: Optional[MPathLike] = None,
        output_suffix: str = "",
        **kwargs,
    ):
        self.path = MPath.from_inp(path)
        self.basepath = MPath.parent  # type: ignore
        self.output_path = None
        self.dict = None
        tempdir = tempdir or kwargs.get("output_tempdir")
        if tempdir:
            self._tempdir = MPath.from_inp(tempdir) / uuid.uuid4().hex
        else:  # pragma: no cover
            self._tempdir = None
        if inp_cache_tempdir:
            self._inp_cache_tempdir = (
                MPath.from_inp(inp_cache_tempdir) / uuid.uuid4().hex
            )
        else:
            self._inp_cache_tempdir = None
        self._mp = None

    def __enter__(self, *args):
        self.dict = dict_from_mapchete(self.path)

        # move all input/foo/cache/path paths to inp_cache_tempdir
        if self._inp_cache_tempdir:
            for key, val in self.dict.get("input", {}).items():
                if isinstance(val, dict):
                    if "cache" in val:
                        if "path" in val["cache"]:
                            path = MPath.from_inp(val["cache"])
                            temp_path = (
                                self._inp_cache_tempdir / key / "cache" / path.name
                            )
                            temp_path.parent.makedirs()
                            val["cache"]["path"] = temp_path
        # replace output path with temporary path
        if self._tempdir:
            # set output directory
            current_output_path = MPath.from_inp(self.dict["output"])
            if current_output_path.suffix:
                self.dict["output"]["path"] = self._tempdir / current_output_path.name
            else:
                self.dict["output"]["path"] = self._tempdir / "out"

            self.path = self._tempdir / self.path.name

            # dump modified mapchete config to temporary directory
            self.path.write_yaml(clear_dict(self.dict))

        # shortcut to output path
        self.output_path = self.dict["output"]["path"]
        return self

    def __exit__(self, *args):
        # properly close mapchete
        try:
            if self._mp:
                self._mp.__exit__(*args)
        finally:
            self.clear_output()
        if self._tempdir:
            self._tempdir.rm(recursive=True, ignore_errors=True)

    def clear_output(self):
        # delete written output if any
        out_dir = (
            self._tempdir
            or MPath.from_inp(self.dict["config_dir"]) / self.dict["output"]["path"]
        )
        out_dir.rm(recursive=True, ignore_errors=True)

    def process_mp(
        self,
        tile: Optional[TileLike] = None,
        tile_zoom: Optional[int] = None,
        batch_preprocess: bool = True,
    ):
        """
        Return MapcheteProcess object used to test processes.
        """
        mp = self.mp(batch_preprocess=batch_preprocess)
        if tile:
            tile = mp.config.process_pyramid.tile(*tile)
        else:
            # just use first process tile from lowest zoom level
            tile = self.first_process_tile(zoom=tile_zoom)
        return mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )

    def mp(self, batch_preprocess=True, bounds=None, zoom=None):
        """
        Return Mapchete object from mapchete.open().
        """
        with SequentialExecutor() as executor:
            if not self._mp:
                self._mp = mapchete.open(self.dict, bounds=bounds, zoom=zoom)
                if batch_preprocess:
                    self._mp.execute_preprocessing_tasks(executor=executor)

            return self._mp

    def first_process_tile(self, zoom=None):
        zoom = zoom or max(self.mp().config.zoom_levels)
        return next(self.mp().get_process_tiles(zoom))


def get_process_mp(tile=None, zoom=0, input=None, params=None, metatiling=1, **kwargs):
    pyramid = BufferedTilePyramid("geodetic", metatiling=metatiling)
    initialized_inputs = initialize_inputs(
        input,
        config_dir=None,
        pyramid=pyramid,
        delimiters=None,
        readonly=False,
    )
    if tile:
        tile = pyramid.tile(*tile)
    else:
        if zoom is None:  # pragma: no cover
            raise ValueError("either tile or tile_zoom have to be provided")
        tile = next(
            pyramid.tiles_from_geom(
                unary_union(
                    [v.bbox(out_crs=pyramid.crs) for v in initialized_inputs.values()]
                ),
                zoom,
            )
        )
    logger.debug(f"tile is {tile}")
    inputs = OrderedDict(open_inputs(initialized_inputs, tile))
    return mapchete.MapcheteProcess(
        tile=tile, input=inputs, params=params or {}, **kwargs
    )
