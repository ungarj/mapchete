#!/usr/bin/env python
"""Command line utility to execute a Mapchete process."""

import os
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
import logging
import logging.config
from py_compile import PyCompileError

from mapchete import Mapchete
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
    except:
        raise
    logging.config.dictConfig(get_log_config(process))

    zoom_levels = _get_zoom_level(parsed.zoom, process)

    if parsed.quiet:
        LOGGER.setLevel(logging.WARNING)

    if parsed.tile:
        tile = process.config.process_pyramid.tile(*tuple(parsed.tile))
        assert tile.is_valid()
        _write_worker(process, _process_worker(process, tile))
        LOGGER.info("1 tile iterated")
        return

    num_processed = 0

    LOGGER.info("starting process using %s worker(s)", multi)
    f = partial(_process_worker, process)

    for zoom in zoom_levels:
        process_tiles = process.get_process_tiles(zoom)
        pool = Pool(multi)
        try:
            for output in pool.imap_unordered(f, process_tiles, chunksize=1):
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


def _get_zoom_level(zoom, process):
    """Determine zoom levels."""
    if zoom is None:
        return reversed(process.config.zoom_levels)
    elif len(zoom) == 2:
        return reversed(range(min(zoom), max(zoom)+1))
    elif len(zoom) == 1:
        return zoom


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
        except ImportError:
            raise
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
