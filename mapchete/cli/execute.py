#!/usr/bin/env python
"""Command line utility to execute a Mapchete process."""

import os
import argparse
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
import time
import logging
import logging.config
import traceback
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
    if parsed.tile:
        zoom = [parsed.tile[0]]
    else:
        zoom = parsed.zoom

    # Initialize process.
    try:
        mapchete = Mapchete(
            MapcheteConfig(
                parsed.mapchete_file,
                zoom=zoom,
                bounds=parsed.bounds,
                overwrite=parsed.overwrite,
                single_input_file=parsed.input_file
            ),
        )
    except PyCompileError as e:
        print e
        return
    except:
        raise
    logging.config.dictConfig(get_log_config(mapchete))

    if parsed.tile:
        tile = mapchete.tile(
            Tile(
                mapchete.tile_pyramid,
                *tuple(parsed.tile)
                )
            )
        try:
            assert tile.is_valid()
        except AssertionError:
            raise ValueError("tile index provided is invalid")
        try:
            process_output, process_tile = mapchete.execute(
                tile, mapchete, parsed.overwrite)
            LOGGER.info("1 tile iterated")
        except:
            raise

        return

    process_tiles = []
    if parsed.failed_from_log:
        LOGGER.info("parsing log file ...")
        process_tiles = failed_tiles_from_log(
            parsed.failed_from_log,
            mapchete,
            failed_since_str=parsed.failed_since
        )

    LOGGER.info("starting process using %s worker(s)", multi)
    f = partial(
        mapchete.raw_output, overwrite=parsed.overwrite, return_metadata=True)
    for zoom in reversed(mapchete.config.zoom_levels):
        if not process_tiles:
            process_tiles = mapchete.get_process_tiles(zoom)
        pool = Pool(multi)
        try:
            for raw_output in pool.imap_unordered(
                f, process_tiles, chunksize=8):
                mapchete.write(raw_output)
                raise NotImplementedError
        except KeyboardInterrupt:
            LOGGER.info("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
            break
        except:
            raise
        finally:
            pool.close()
            pool.join()
        process_tiles = []

    # TODO LOGGER.info("%s tile(s) iterated", (len(collected_output)))

    # TODO VRT creation


def failed_tiles_from_log(logfile, mapchete, failed_since_str='1980-01-01'):
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
                    yield mapchete.tile(
                        Tile(
                            mapchete.tile_pyramid,
                            *tuple(tile)
                        )
                    )


def worker(tile, mapchete, overwrite):
    """
    Worker function running the process depending on the overwrite flag and
    whether the tile exists.
    """
    starttime = time.time()
    try:
        log_message = mapchete.execute(tile, overwrite=overwrite)
    except Exception:
        log_message = mapchete.process_name, (
            tile.id,
            "failed",
            traceback.print_exc()
        )
    endtime = time.time()
    elapsed = "%ss" % (round((endtime - starttime), 3))

    LOGGER.info((mapchete.process_name, log_message, elapsed))
    return log_message


if __name__ == "__main__":
    main()
