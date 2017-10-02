#!/usr/bin/env python
"""Command line utility to execute a Mapchete process."""

from multiprocessing import cpu_count

import mapchete


def main(args=None):
    """Execute a Mapchete process."""
    parsed = args
    multi = parsed.multi if parsed.multi else cpu_count()
    mode = "overwrite" if parsed.overwrite else "continue"
    zoom = parsed.zoom if parsed.zoom else None

    # process single tile
    if parsed.tile:
        with mapchete.open(
            parsed.mapchete_file, mode=mode,
            single_input_file=parsed.input_file, debug=parsed.debug
        ) as mp:
            mp.batch_process(
                tile=parsed.tile, quiet=parsed.quiet, debug=parsed.debug
            )
    # initialize and run process
    else:
        with mapchete.open(
            parsed.mapchete_file, bounds=parsed.bounds, mode=mode,
            single_input_file=parsed.input_file, debug=parsed.debug
        ) as mp:
            mp.batch_process(
                multi=multi, quiet=parsed.quiet, debug=parsed.debug, zoom=zoom
            )
