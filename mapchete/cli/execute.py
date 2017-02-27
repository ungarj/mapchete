#!/usr/bin/env python
"""Command line utility to execute a Mapchete process."""

import os
import argparse
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
import logging
import logging.config
from py_compile import PyCompileError
import re
from datetime import datetime
import warnings
from tilematrix import Tile

from mapchete import Mapchete
from mapchete.config import MapcheteConfig
from mapchete.log import get_log_config

LOGGER = logging.getLogger("mapchete")


def main(args=None):
    """Execute a Mapchete process."""
    if isinstance(args, argparse.Namespace):
        parsed = args
    else:
        raise RuntimeError("invalid arguments for mapchete execute")

    input_file = parsed.input_file
    if input_file and not (
        os.path.isfile(input_file) or os.path.isdir(input_file)
    ):
        raise IOError("input_file not found")

    multi = parsed.multi
    if not multi:
        multi = cpu_count()
    zoom = parsed.zoom

    mode = "overwrite" if parsed.overwrite else "continue"

    # Initialize process.
    try:
        process = Mapchete(
            MapcheteConfig(
                parsed.mapchete_file, bounds=parsed.bounds,
                mode=mode, single_input_file=parsed.input_file
            ),
        )
    except PyCompileError as e:
        print e
        return
    except:
        raise
    logging.config.dictConfig(get_log_config(process))

    if parsed.quiet:
        LOGGER.setLevel(logging.WARNING)

    if zoom is None:
        zoom_levels = reversed(process.config.zoom_levels)
    elif len(zoom) == 2:
        zoom_levels = reversed(range(min(zoom), max(zoom)+1))
    elif len(zoom) == 1:
        zoom_levels = zoom

    if parsed.tile:
        tile = process.config.process_pyramid.tile(*tuple(parsed.tile))
        try:
            assert tile.is_valid()
        except AssertionError:
            raise ValueError("tile index provided is invalid")
        try:
            output = _process_worker(process, tile)
            _write_worker(process, output)
            LOGGER.info("1 tile iterated")
        except:
            raise
        return

    process_tiles = []
    num_processed = 0
    if parsed.failed_from_log:
        LOGGER.info("parsing log file ...")
        process_tiles = _failed_tiles_from_log(
            parsed.failed_from_log,
            process,
            failed_since_str=parsed.failed_since
        )

    LOGGER.info("starting process using %s worker(s)", multi)
    f = partial(_process_worker, process)

    for zoom in zoom_levels:
        if not process_tiles:
            process_tiles = process.get_process_tiles(zoom)
        pool = Pool(multi)
        try:
            for output in pool.imap_unordered(
                f, process_tiles, chunksize=1):
                _write_worker(process, output)
                num_processed += 1
        except KeyboardInterrupt:
            LOGGER.info("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
            break
        except:
            raise
        finally:
            pool.close()
            pool.join()
            process_tiles = None

    LOGGER.info("%s tile(s) iterated", (str(num_processed)))

    # TODO VRT creation


def _failed_tiles_from_log(logfile, process, failed_since_str='1980-01-01'):
    """
    Get previously failed tiles.

    Reads logfile line by line and returns tile indexes filtered by timestamp
    and failed tiles.
    """
    if not os.path.isfile(logfile):
        raise IOError("input log file not found")
    try:
        failed_since = datetime.strptime(failed_since_str, '%Y-%m-%d')
    except:
        raise ValueError("bad timestamp given")

    with open(logfile) as logs:
        for line in logs.readlines():
            if "failed" in line:
                t = re.search(
                    '\[.*[0-9]\]',
                    line
                ).group(0).replace('[', '').replace(']', '')
                timestamp = datetime.strptime(t, '%Y-%m-%d %H:%M:%S,%f')
                if timestamp > failed_since:
                    try:
                        tile = map(
                            int,
                            re.search(
                                '\([0-9].*[0-9]\),',
                                line
                            ).group(0).replace('(', '').replace('),', '').split(
                                ', '
                            )
                        )
                    except:
                        warnings.warn("log line could not be parsed")
                        continue
                    yield process.tile(
                        Tile(
                            process.tile_pyramid,
                            *tuple(tile)
                        )
                    )


def _process_worker(process, process_tile):
    """Worker function running the process."""
    # Skip execution if overwrite is disabled and tile exists
    if process.config.mode == "continue" and (
        process.config.output.tiles_exist(process_tile)
    ):
        process_tile.message = "exists"
        LOGGER.info((
            process.process_name, process_tile.id, process_tile.message,
            None, None))
        return process_tile
    else:
        try:
            return process.execute(process_tile)
        except Exception as e:
            process_tile.message = "error"
            process_tile.error = e
            return process_tile


def _write_worker(process, process_tile):
    """Worker function writing process outputs."""
    if process_tile.message == "exists":
        return
    process.write(process_tile)


if __name__ == "__main__":
    main()
