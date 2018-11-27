"""Command line utility to execute a Mapchete process."""

import click
import click_spinner
import logging
from multiprocessing import cpu_count
import os
from shapely import wkt
import sys
import tqdm
import yaml

import mapchete
from mapchete.cli import utils
from mapchete.config import get_zoom_levels, _map_to_new_config
from mapchete.index import zoom_index_gen
from mapchete.tile import BufferedTilePyramid


# workaround for https://github.com/tqdm/tqdm/issues/481
tqdm.monitor_interval = 0

logger = logging.getLogger(__name__)


@click.command(help="Execute a process.")
@utils.arg_mapchete_files
@utils.opt_zoom
@utils.opt_bounds
@utils.opt_point
@utils.opt_wkt_geometry
@utils.opt_tile
@utils.opt_overwrite
@utils.opt_multi
@utils.opt_input_file
@utils.opt_logfile
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_max_chunksize
@utils.opt_vrt
@utils.opt_idx_out_dir
def execute(
    mapchete_files,
    zoom=None,
    bounds=None,
    point=None,
    wkt_geometry=None,
    tile=None,
    overwrite=False,
    multi=None,
    input_file=None,
    logfile=None,
    verbose=False,
    no_pbar=False,
    debug=False,
    max_chunksize=None,
    vrt=False,
    idx_out_dir=None
):
    """Execute a Mapchete process."""
    multi = multi if multi else cpu_count()
    mode = "overwrite" if overwrite else "continue"
    # send verbose output to /dev/null if not activated
    if debug or not verbose:
        verbose_dst = open(os.devnull, 'w')
    else:
        verbose_dst = sys.stdout

    for mapchete_file in mapchete_files:

        tqdm.tqdm.write("preparing to process %s" % mapchete_file, file=verbose_dst)

        with click_spinner.spinner(disable=debug) as spinner:

            def _raw_conf():
                return _map_to_new_config(
                    yaml.load(open(mapchete_file, "r").read())
                )

            def _tp():
                return BufferedTilePyramid(
                    _raw_conf()["pyramid"]["grid"],
                    metatiling=_raw_conf()["pyramid"].get("metatiling", 1),
                    pixelbuffer=_raw_conf()["pyramid"].get("pixelbuffer", 0)
                )

            # process single tile
            if tile:
                tile = _tp().tile(*tile)
                with mapchete.open(
                    mapchete_file, mode=mode, bounds=tile.bounds,
                    zoom=tile.zoom, single_input_file=input_file
                ) as mp:
                    spinner.stop()
                    tqdm.tqdm.write("processing 1 tile")
                    for result in mp.batch_processor(tile=tile):
                        utils.write_verbose_msg(result, dst=verbose_dst)

                    if vrt:
                        tqdm.tqdm.write("creating VRT")
                        for tile in tqdm.tqdm(
                            zoom_index_gen(
                                mp=mp,
                                zoom=tile.zoom,
                                out_dir=(
                                    idx_out_dir if idx_out_dir else mp.config.output.path
                                ),
                                vrt=vrt,
                            ),
                            total=mp.count_tiles(tile.zoom, tile.zoom),
                            unit="tile",
                            disable=debug
                        ):
                            logger.debug(tile)

            # initialize and run process
            else:
                if wkt_geometry:
                    bounds = wkt.loads(wkt_geometry).bounds
                elif point:
                    x, y = point
                    zoom_levels = get_zoom_levels(
                        process_zoom_levels=_raw_conf()["zoom_levels"],
                        init_zoom_levels=zoom
                    )
                    bounds = _tp().tile_from_xy(x, y, max(zoom_levels)).bounds
                else:
                    bounds = bounds
                with mapchete.open(
                    mapchete_file, bounds=bounds, zoom=zoom,
                    mode=mode, single_input_file=input_file
                ) as mp:
                    spinner.stop()
                    tiles_count = mp.count_tiles(
                        min(mp.config.init_zoom_levels),
                        max(mp.config.init_zoom_levels))
                    tqdm.tqdm.write(
                        "processing %s tile(s) on %s worker(s)" % (tiles_count, multi)
                    )
                    for process_info in tqdm.tqdm(
                        mp.batch_processor(
                            multi=multi, zoom=zoom,
                            max_chunksize=max_chunksize),
                        total=tiles_count,
                        unit="tile",
                        disable=debug or no_pbar
                    ):
                        utils.write_verbose_msg(process_info, dst=verbose_dst)

                    if vrt:
                        tqdm.tqdm.write("creating VRT(s)")
                        for z in mp.config.init_zoom_levels:
                            logger.debug("zoom %s", z)
                            for tile in tqdm.tqdm(
                                zoom_index_gen(
                                    mp=mp,
                                    zoom=z,
                                    out_dir=(
                                        idx_out_dir if idx_out_dir
                                        else mp.config.output.path
                                    ),
                                    vrt=vrt
                                ),
                                total=mp.count_tiles(z, z),
                                unit="tile",
                                disable=debug
                            ):
                                logger.debug(tile)

        tqdm.tqdm.write("process finished", file=verbose_dst)
