#!/usr/bin/env python
"""Command line utility to execute a Mapchete process."""

import yaml
from multiprocessing import cpu_count

import mapchete
from mapchete.errors import MapcheteConfigError
from mapchete.tile import BufferedTilePyramid


def main(args=None):
    """Execute a Mapchete process."""
    parsed = args
    multi = parsed.multi if parsed.multi else cpu_count()
    mode = "overwrite" if parsed.overwrite else "continue"
    # process single tile
    if parsed.tile:
        conf = yaml.load(open(parsed.mapchete_file, "r").read())
        if "output" not in conf:
            raise MapcheteConfigError("output definition missing")
        if "pyramid" not in conf or "grid" not in conf["pyramid"]:
            raise MapcheteConfigError("pyramid definition missing")
        tile = BufferedTilePyramid(
            conf["pyramid"]["grid"],
            metatiling=conf["pyramid"].get("metatiling", 1),
            pixelbuffer=conf["pyramid"].get("pixelbuffer", 0)
        ).tile(*parsed.tile)
        with mapchete.open(
            parsed.mapchete_file, mode=mode, bounds=tile.bounds,
            zoom=tile.zoom, single_input_file=parsed.input_file,
            debug=parsed.debug
        ) as mp:
            mp.batch_process(
                tile=parsed.tile, quiet=parsed.quiet, debug=parsed.debug
            )
    # initialize and run process
    else:
        with mapchete.open(
            parsed.mapchete_file, bounds=parsed.bounds, zoom=parsed.zoom,
            mode=mode, single_input_file=parsed.input_file, debug=parsed.debug
        ) as mp:
            mp.batch_process(
                multi=multi, quiet=parsed.quiet, debug=parsed.debug,
                zoom=parsed.zoom)
