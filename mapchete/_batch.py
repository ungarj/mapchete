"""Processing of larger batches."""

import logging
import time
from functools import partial
from itertools import product
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from tilematrix import TilePyramid


logger = logging.getLogger(__name__)


def batch_process(
    process, zoom=None, tile=None, multi=cpu_count(), max_chunksize=16
):
    """
    Process a large batch of tiles quietly.

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
    max_chunksize : int
        maximum number of process tiles to be queued for each worker;
        (default: 16)
    """
    list(batch_processor(process, zoom, tile, multi, max_chunksize))


def batch_processor(
    process, zoom=None, tile=None, multi=cpu_count(),
    max_chunksize=16
):
    """
    Process a large batch of tiles and yield report messages per tile.

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
    max_chunksize : int
        maximum number of process tiles to be queued for each worker;
        (default: 16)
    """
    if zoom and tile:
        raise ValueError("use either zoom or tile")

    # run single tile
    if tile:
        yield _run_on_single_tile(process, tile)
    # run using multiprocessing
    elif multi > 1:
        for result in _run_with_multiprocessing(
            process, list(_get_zoom_level(zoom, process)), multi, max_chunksize
        ):
            yield result
    # run without multiprocessing
    elif multi == 1:
        for result in _run_without_multiprocessing(
            process, list(_get_zoom_level(zoom, process))
        ):
            yield result


def _run_on_single_tile(process, tile):
    logger.debug("run on single tile")
    tile, message = _process_worker(
        process, process.config.process_pyramid.tile(*tuple(tile)))
    return dict(process_tile=tile, **message)


def _run_with_multiprocessing(process, zoom_levels, multi, max_chunksize):
    logger.debug("run with multiprocessing")
    num_processed = 0
    logger.info("run process using %s workers", multi)
    f = partial(_process_worker, process)
    for zoom in zoom_levels:
        process_tiles = process.get_process_tiles(zoom)
        pool = Pool(multi)
        try:
            for tile, message in pool.imap_unordered(
                f,
                process_tiles,
                # set chunksize to between 1 and max_chunksize
                chunksize=min([
                    max([process.count_tiles(
                        min(zoom_levels), max(zoom_levels)) // multi, 1]),
                    max_chunksize])
            ):
                num_processed += 1
                yield dict(process_tile=tile, **message)
        except KeyboardInterrupt:
            logger.info(
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
    logger.info("%s tile(s) iterated", (str(num_processed)))


def _run_without_multiprocessing(process, zoom_levels):
    logger.debug("run without multiprocessing")
    num_processed = 0
    logger.info("run process using 1 worker")
    for zoom in zoom_levels:
        for process_tile in process.get_process_tiles(zoom):
            tile, message = _process_worker(process, process_tile)
            num_processed += 1
            yield dict(process_tile=tile, **message)
    logger.info("%s tile(s) iterated", (str(num_processed)))


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
        logger.debug((process_tile.id, "tile exists, skipping"))
        return process_tile, dict(
            process="output already exists",
            write="nothing written")
    else:
        start = time.time()
        output = process.execute(process_tile)
        processor_message = "processed in %ss" % round(time.time() - start, 3)
        logger.debug((process_tile.id, processor_message))
        writer_message = _write_worker(process, process_tile, output)
        return process_tile, dict(
            process=processor_message,
            write=writer_message)


def _write_worker(process, process_tile, data):
    """Worker function writing process outputs."""
    if data is None:
        logger.debug("%s nothing written, tile data empty", process_tile.id)
        return "output empty, nothing written"
    else:
        start = time.time()
        process.write(process_tile, data)
        message = "output written in %ss" % round(time.time() - start, 3)
        logger.debug((process_tile.id, message))
        return message


def count_tiles(geometry, pyramid, minzoom, maxzoom, init_zoom=0):
    """
    Count number of tiles intersecting with geometry.

    Parameters
    ----------
    geometry : shapely geometry
    pyramid : TilePyramid
    minzoom : int
    maxzoom : int
    init_zoom : int

    Returns
    -------
    number of tiles
    """
    if not 0 <= init_zoom <= minzoom <= maxzoom:
        raise ValueError("invalid zoom levels given")
    # tile buffers are not being taken into account
    unbuffered_pyramid = TilePyramid(
        pyramid.grid, tile_size=pyramid.tile_size,
        metatiling=pyramid.metatiling
    )
    # make sure no rounding errors occur
    geometry = geometry.buffer(-0.000000001)
    return _count_tiles(
        [
            unbuffered_pyramid.tile(*tile_id)
            for tile_id in product(
                [init_zoom],
                range(pyramid.matrix_height(init_zoom)),
                range(pyramid.matrix_width(init_zoom))
            )
        ], geometry, minzoom, maxzoom
    )


def _count_tiles(tiles, geometry, minzoom, maxzoom):
    count = 0
    for tile in tiles:
        # determine data covered by tile
        tile_intersection = tile.bbox().intersection(geometry)
        # skip if there is no data
        if tile_intersection.is_empty:
            continue
        # increase counter as tile contains data
        elif tile.zoom >= minzoom:
            count += 1
        # if there are further zoom levels, analyze descendants
        if tile.zoom < maxzoom:
            # if tile is full, all of its descendants will be full as well
            if tile.zoom >= minzoom and tile_intersection.equals(tile.bbox()):
                # sum up tiles for each remaining zoom level
                count += sum([
                     4**z_diff
                     for z_diff in range(1, (maxzoom - tile.zoom) + 1)
                ])
            # if tile is half full, analyze each descendant
            else:
                count += _count_tiles(
                    tile.get_children(), tile_intersection, minzoom, maxzoom
                )
    return count
