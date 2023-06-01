"""
Useful tools to facilitate testing.
"""
import logging
import uuid
from collections import OrderedDict

import oyaml as yaml
from shapely.ops import unary_union

import mapchete
from mapchete.config import initialize_inputs, open_inputs
from mapchete.io import MPath, fs_from_path
from mapchete.tile import BufferedTilePyramid

logger = logging.getLogger(__name__)


# helper functions
def dict_from_mapchete(path):
    """
    Read mapchete configuration from file and return as dictionary.
    """
    path = MPath(path)
    with path.open() as src:
        out = dict(yaml.safe_load(src.read()), config_dir=path.dirname)
        if "config_dir" in out:
            out["config_dir"] = MPath(out["config_dir"])
        elif "path" in out.get("output", {}):
            out["output"]["path"] = MPath(out["output"]["path"])
    return out


class ProcessFixture:
    def __init__(
        self, path=None, output_tempdir=None, inp_cache_tempdir=None, output_suffix=""
    ):
        self.path = MPath(path)
        self.dict = None
        if output_tempdir:
            self._output_tempdir = MPath(output_tempdir) / uuid.uuid4().hex
        else:
            self._output_tempdir = None
        if inp_cache_tempdir:
            self._inp_cache_tempdir = MPath(output_tempdir).joinpath(uuid.uuid4().hex)
        else:
            self._inp_cache_tempdir = None
        self._out_fs = None
        self._mp = None

    def __enter__(self, *args):
        self.dict = dict_from_mapchete(self.path)
        if self._output_tempdir:
            # set output directory
            current_output_path = MPath(self.dict["output"]["path"])
            if current_output_path.suffix:
                self._output_tempdir = self._output_tempdir + current_output_path.suffix
            self.dict["output"]["path"] = self._output_tempdir
        if self._inp_cache_tempdir:
            for key, val in self.dict.get("input", {}).items():
                if isinstance(val, dict):
                    if "cache" in val:
                        if "path" in val["cache"]:
                            path = MPath(val["cache"]["path"])
                            val["cache"]["path"] = (
                                self._inp_cache_tempdir / key / "cache" / path.name
                            )
        return self

    def __exit__(self, *args):
        # properly close mapchete
        try:
            if self._mp:
                self._mp.__exit__(*args)
        finally:
            self.clear_output()

    def clear_output(self):
        # delete written output if any
        if self._output_tempdir:
            out_dir = self._output_tempdir
        else:
            out_dir = MPath(self.dict["config_dir"]) / self.dict["output"]["path"]
        try:
            fs_from_path(out_dir).rm(str(out_dir), recursive=True)
        except FileNotFoundError:
            pass

    def process_mp(self, tile=None, tile_zoom=None, batch_preprocess=True):
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

    def mp(self, batch_preprocess=True):
        """
        Return Mapchete object from mapchete.open().
        """
        if not self._mp:
            self._mp = mapchete.open(self.dict)
            if batch_preprocess:
                self._mp.batch_preprocess()
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
