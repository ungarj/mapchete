#!/usr/bin/env python
"""Command line utility to execute a Mapchete process."""

import os
from multiprocessing import cpu_count
import logging
import logging.config
from py_compile import PyCompileError

from mapchete import Mapchete, batch_process
from mapchete.config import MapcheteConfig
from mapchete.log import get_log_config


LOGGER = logging.getLogger("mapchete")


def main(args=None):
    """Execute a Mapchete process."""
    parsed = args

    if parsed.input_file and not (
        os.path.isfile(parsed.input_file) or os.path.isdir(parsed.input_file)
    ):
        raise IOError("input_file not found")

    multi = parsed.multi if parsed.multi else cpu_count()
    mode = "overwrite" if parsed.overwrite else "continue"

    # Initialize process.
    try:
        process = Mapchete(
            MapcheteConfig(
                parsed.mapchete_file, bounds=parsed.bounds, mode=mode,
                single_input_file=parsed.input_file))
    except PyCompileError as e:
        print e
        return
    except Exception:
        raise
    logging.config.dictConfig(get_log_config(process))

    batch_process(
        process, parsed.zoom, parsed.tile, multi, parsed.quiet, parsed.debug)
