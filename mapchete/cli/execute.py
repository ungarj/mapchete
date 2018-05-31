"""Command line utility to execute a Mapchete process."""

import logging
from multiprocessing import cpu_count
import os
from shapely import wkt
import sys
import tqdm
import yaml

import mapchete
from mapchete.config import get_zoom_levels, _map_to_new_config
from mapchete.tile import BufferedTilePyramid


# workaround for https://github.com/tqdm/tqdm/issues/481
tqdm.monitor_interval = 0

# lower stream output log level
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.ERROR)
logging.getLogger().addHandler(stream_handler)


def main(args=None):
    """Execute a Mapchete process."""
    parsed = args
    multi = parsed.multi if parsed.multi else cpu_count()
    mode = "overwrite" if parsed.overwrite else "continue"
    # send verbose output to /dev/null if not activated
    if parsed.debug or not parsed.verbose:
        verbose_dst = open(os.devnull, 'w')
    else:
        verbose_dst = sys.stdout

    if parsed.logfile:
        file_handler = logging.FileHandler(parsed.logfile)
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)
        logging.getLogger("mapchete").setLevel(logging.DEBUG)
    if parsed.debug:
        logging.getLogger("mapchete").setLevel(logging.DEBUG)
        stream_handler.setLevel(logging.DEBUG)
        # logging.getLogger().addHandler(stream_handler)

    tqdm.tqdm.write("preparing process", file=verbose_dst)

    def _raw_conf():
        return _map_to_new_config(
            yaml.load(open(parsed.mapchete_file, "r").read())
        )

    def _tp():
        return BufferedTilePyramid(
            _raw_conf()["pyramid"]["grid"],
            metatiling=_raw_conf()["pyramid"].get("metatiling", 1),
            pixelbuffer=_raw_conf()["pyramid"].get("pixelbuffer", 0)
        )

    # process single tile
    if parsed.tile:
        tile = _tp().tile(*parsed.tile)
        with mapchete.open(
            parsed.mapchete_file, mode=mode, bounds=tile.bounds,
            zoom=tile.zoom, single_input_file=parsed.input_file
        ) as mp:
            tqdm.tqdm.write("processing 1 tile", file=verbose_dst)
            for result in mp.batch_processor(tile=parsed.tile):
                if parsed.verbose:
                    _write_verbose_msg(result, dst=verbose_dst)

    # initialize and run process
    else:
        if parsed.wkt_geometry:
            bounds = wkt.loads(parsed.wkt_geometry).bounds
        elif parsed.point:
            x, y = parsed.point
            zoom_levels = get_zoom_levels(
                process_zoom_levels=_raw_conf()["zoom_levels"],
                init_zoom_levels=parsed.zoom
            )
            bounds = _tp().tile_from_xy(x, y, max(zoom_levels)).bounds
        else:
            bounds = parsed.bounds
        with mapchete.open(
            parsed.mapchete_file, bounds=bounds, zoom=parsed.zoom,
            mode=mode, single_input_file=parsed.input_file
        ) as mp:
            tiles_count = mp.count_tiles(
                min(mp.config.init_zoom_levels),
                max(mp.config.init_zoom_levels))
            tqdm.tqdm.write("processing %s tile(s) on %s worker(s)" % (
                tiles_count, multi
            ), file=verbose_dst)
            for result in tqdm.tqdm(
                mp.batch_processor(
                    multi=multi, zoom=parsed.zoom,
                    max_chunksize=parsed.max_chunksize),
                total=tiles_count,
                unit="tile",
                disable=parsed.debug or parsed.no_pbar
            ):
                _write_verbose_msg(result, dst=verbose_dst)

    tqdm.tqdm.write("process finished", file=verbose_dst)


def _write_verbose_msg(result, dst):
    msg = "Tile %s: %s, %s" % (
        tuple(result["process_tile"].id), result["process"],
        result["write"])
    tqdm.tqdm.write(msg, file=dst)
