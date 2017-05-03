#!/usr/bin/env python
"""Command line utility to execute a Mapchete process."""

import os
from multiprocessing import cpu_count

import mapchete


def main(args=None):
    """Execute a Mapchete process."""
    parsed = args

    if parsed.input_file and not (
        os.path.isfile(parsed.input_file) or os.path.isdir(parsed.input_file)
    ):
        raise IOError("input_file not found")

    multi = parsed.multi if parsed.multi else cpu_count()
    mode = "overwrite" if parsed.overwrite else "continue"

    # initialize and run process
    with mapchete.open(
        parsed.mapchete_file, bounds=parsed.bounds, mode=mode,
        single_input_file=parsed.input_file
    ) as mp:
        mp.batch_process(
            parsed.zoom, parsed.tile, multi, parsed.quiet, parsed.debug)
