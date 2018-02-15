#!/usr/bin/env python
"""Command line utility to execute a Mapchete process."""

import logging
from multiprocessing import cpu_count
import tqdm
import yaml

import mapchete
from mapchete.config import _map_to_new_config
from mapchete.tile import BufferedTilePyramid


# lower stream output log level
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.ERROR)
logging.getLogger().addHandler(stream_handler)
logger = logging.getLogger(__name__)


def main(args=None):
    """Execute a Mapchete process."""
    parsed = args
    multi = parsed.multi if parsed.multi else cpu_count()
    mode = "overwrite" if parsed.overwrite else "continue"

    if parsed.logfile:
        file_handler = logging.FileHandler(parsed.logfile)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s %(message)s')
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)
        logging.getLogger("mapchete").setLevel(logging.DEBUG)

    # process single tile
    if parsed.tile:
        conf = _map_to_new_config(
            yaml.load(open(parsed.mapchete_file, "r").read()))
        tile = BufferedTilePyramid(
            conf["pyramid"]["grid"],
            metatiling=conf["pyramid"].get("metatiling", 1),
            pixelbuffer=conf["pyramid"].get("pixelbuffer", 0)
        ).tile(*parsed.tile)
        tqdm.tqdm.write("preparing process")
        with mapchete.open(
            parsed.mapchete_file, mode=mode, bounds=tile.bounds,
            zoom=tile.zoom, single_input_file=parsed.input_file
        ) as mp:
            tqdm.tqdm.write("processing 1 tile")
            for result in mp.batch_processor(tile=parsed.tile):
                if parsed.verbose:
                    _write_verbose_msg(result)

    # initialize and run process
    else:
        tqdm.tqdm.write("preparing process")
        with mapchete.open(
            parsed.mapchete_file, bounds=parsed.bounds, zoom=parsed.zoom,
            mode=mode, single_input_file=parsed.input_file
        ) as mp:
            tiles_count = mp.count_tiles(
                min(mp.config.init_zoom_levels),
                max(mp.config.init_zoom_levels))
            tqdm.tqdm.write("processing %s tile(s) on %s worker(s)" % (
                tiles_count, multi
            ))
            for result in tqdm.tqdm(
                mp.batch_processor(
                    multi=multi, zoom=parsed.zoom,
                    max_chunksize=parsed.max_chunksize),
                total=tiles_count,
                unit="tile"
            ):
                if parsed.verbose:
                    _write_verbose_msg(result)
    tqdm.tqdm.write("process finished")


def _write_verbose_msg(result):
    msg = "Tile %s: %s, %s" % (
        tuple(result["process_tile"].id), result["process"],
        result["write"])
    tqdm.tqdm.write(msg)
