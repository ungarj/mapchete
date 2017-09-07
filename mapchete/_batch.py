"""Processing of larger batches."""

import logging
import tqdm
import time
from functools import partial
from itertools import product
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from tilematrix import TilePyramid


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

    # estimate process tiles
    if (quiet or debug):
        total_tiles = 0
    else:
        total_tiles = count_tiles(
            process.config.process_area(),
            process.config.process_pyramid,
            min(zoom_levels),
            max(zoom_levels),
            init_zoom=0
        )

    # run using multiprocessing
    if multi > 1:
        _run_with_multiprocessing(
            process, total_tiles, zoom_levels, multi, quiet, debug)

    # run without multiprocessing
    if multi == 1:
        _run_without_multiprocessing(
            process, total_tiles, zoom_levels, quiet, debug)


def _run_on_single_tile(process, tile):
    LOGGER.debug("run on single tile")
    tile = process.config.process_pyramid.tile(*tuple(tile))
    output = _process_worker(process, tile)
    if output:
        _write_worker(process, output)
    LOGGER.info("1 tile iterated")


def _run_with_multiprocessing(
    process, total_tiles, zoom_levels, multi, quiet, debug
):
    LOGGER.debug("run with multiprocessing")
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
                    if output:
                        _write_worker(process, output)
                    pbar.update()
                    num_processed += 1
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
    LOGGER.debug("run without multiprocessing")
    num_processed = 0
    LOGGER.info("run process using 1 worker")
    with tqdm.tqdm(
        total=total_tiles, unit="tiles", disable=(quiet or debug)
    ) as pbar:
        for zoom in zoom_levels:
            for process_tile in process.get_process_tiles(zoom):
                output = _process_worker(process, process_tile)
                if output:
                    _write_worker(process, output)
                pbar.update()
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
        LOGGER.debug((process_tile.id, "tile exists, skipping"))
    else:
        start = time.time()
        output = process.execute(process_tile)
        LOGGER.debug((
            process_tile.id, "processed in %ss" % (
                round(time.time() - start, 3)
            )
        ))
        return output


def _write_worker(process, process_tile):
    """Worker function writing process outputs."""
    if process_tile and (
        process_tile.data is not None) and (
        process_tile.message != "empty"
    ):
        start = time.time()
        process.write(process_tile)
        LOGGER.debug((
            process_tile.id, "output written in %ss" % (
                round(time.time() - start, 3)
            )
        ))


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
        pyramid.type, tile_size=pyramid.tile_size,
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
