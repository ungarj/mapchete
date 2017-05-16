"""Processing of larger batches."""

import logging
import tqdm
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def batch_process(
    process, zoom=None, tile=None, multi=cpu_count(), quiet=False, debug=False
):
    """
    Process a large batch of tiles.

    Parameters
    ----------
    process : MapcheteProcess
        process to be run
    zoom : list or int
        either single zoom level or list of minimum and maximum zoom level;
        None processes all (default: None)
    tile : tuple
        zoom, row and column of tile to be processed (cannot be used with zoom)
    multi : int
        number of workers (default: number of CPU cores)
    quiet : bool
        set log level to "warning" and disable progress bar
    debug : bool
        set log level to "debug" and disable progress bar (cannot be used with
        quiet)
    """
    if zoom and tile:
        raise ValueError("use either zoom or tile")
    if quiet and debug:
        raise ValueError("use either quiet or debug")
    if quiet:
        LOGGER.setLevel(logging.ERROR)
    if debug:
        LOGGER.setLevel(logging.DEBUG)

    # process single tile
    if tile:
        _run_on_single_tile(process, tile)
        return

    # prepare batch
    zoom_levels = list(_get_zoom_level(zoom, process))

    # TODO quicker tile number estimation
    if (quiet or debug):
        total_tiles = 0
    else:
        total_tiles = sum(
            len(list(process.get_process_tiles(z))) for z in zoom_levels)

    # run using multiprocessing
    if multi > 1:
        _run_with_multiprocessing(
            process, total_tiles, zoom_levels, multi, quiet, debug)

    # run without multiprocessing
    if multi == 1:
        _run_without_multiprocessing(
            process, total_tiles, zoom_levels, quiet, debug)


def _run_on_single_tile(process, tile):
    tile = process.config.process_pyramid.tile(*tuple(tile))
    assert tile.is_valid()
    output = _process_worker(process, tile)
    if output:
        _write_worker(process, output)
    LOGGER.info("1 tile iterated")


def _run_with_multiprocessing(
    process, total_tiles, zoom_levels, multi, quiet, debug
):
    num_processed = 0
    LOGGER.info("run process using %s workers", multi)
    f = partial(_process_worker, process)
    with tqdm.tqdm(
        total=total_tiles, unit="tiles", disable=(quiet or debug)
    ) as pbar:
        for zoom in zoom_levels:
            process_tiles = process.get_process_tiles(zoom)
            pool = Pool(multi)
            try:
                for output in pool.imap_unordered(
                    f, process_tiles, chunksize=1
                ):
                    pbar.update()
                    num_processed += 1
                    if output:
                        _write_worker(process, output)
            except KeyboardInterrupt:
                LOGGER.info(
                    "Caught KeyboardInterrupt, terminating workers")
                pool.terminate()
                break
            except Exception:
                pool.terminate()
                raise
            finally:
                pool.close()
                pool.join()
                process_tiles = None
    LOGGER.info("%s tile(s) iterated", (str(num_processed)))


def _run_without_multiprocessing(
    process, total_tiles, zoom_levels, quiet, debug
):
    num_processed = 0
    LOGGER.info("run process using 1 worker")
    with tqdm.tqdm(
        total=total_tiles, unit="tiles", disable=(quiet or debug)
    ) as pbar:
        for zoom in zoom_levels:
            for process_tile in process.get_process_tiles(zoom):
                pbar.update()
                output = _process_worker(process, process_tile)
                if output:
                    _write_worker(process, output)
                num_processed += 1
    LOGGER.info("%s tile(s) iterated", (str(num_processed)))


def _get_zoom_level(zoom, process):
    """Determine zoom levels."""
    if zoom is None:
        return reversed(process.config.zoom_levels)
    if isinstance(zoom, int):
        return [zoom]
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
        LOGGER.debug((process_tile.id, "tile exists"))
    else:
        return process.execute(process_tile)


def _write_worker(process, process_tile):
    """Worker function writing process outputs."""
    process.write(process_tile)
