"""Command line utility to execute a Mapchete process."""

import click
import click_spinner
import logging
from multiprocessing import cpu_count
import os
import sys
import tqdm

import mapchete
from mapchete.cli import utils
from mapchete.config import raw_conf, raw_conf_process_pyramid, bounds_from_opts
from mapchete.index import zoom_index_gen


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
    mode = "overwrite" if overwrite else "continue"
    # send verbose messages to /dev/null if not activated
    if debug or not verbose:
        verbose_dst = open(os.devnull, 'w')
    else:
        verbose_dst = sys.stdout

    for mapchete_file in mapchete_files:
        tqdm.tqdm.write("preparing to process %s" % mapchete_file, file=verbose_dst)
        # process single tile
        if tile:
            _process_single_tile(
                raw_conf_process_pyramid=raw_conf_process_pyramid,
                mapchete_file=mapchete_file,
                tile=tile,
                mode=mode,
                input_file=input_file,
                debug=debug,
                verbose_dst=verbose_dst,
                vrt=vrt,
                idx_out_dir=idx_out_dir,
                no_pbar=no_pbar
            )
        # process area
        else:
            _process_area(
                debug=debug,
                mapchete_file=mapchete_file,
                mode=mode,
                zoom=zoom,
                wkt_geometry=wkt_geometry,
                point=point,
                bounds=bounds,
                input_file=input_file,
                multi=multi or cpu_count(),
                verbose_dst=verbose_dst,
                max_chunksize=max_chunksize,
                no_pbar=no_pbar,
                vrt=vrt,
                idx_out_dir=idx_out_dir,
            )


def _process_single_tile(
    debug=None,
    raw_conf_process_pyramid=None,
    mapchete_file=None,
    tile=None,
    mode=None,
    input_file=None,
    verbose_dst=None,
    vrt=None,
    idx_out_dir=None,
    no_pbar=None
):
    with click_spinner.spinner(disable=debug) as spinner:
        with mapchete.Timer() as t:
            tile = raw_conf_process_pyramid(raw_conf(mapchete_file)).tile(*tile)
            with mapchete.open(
                mapchete_file,
                mode=mode,
                bounds=tile.bounds,
                zoom=tile.zoom,
                single_input_file=input_file
            ) as mp:
                spinner.stop()
                tqdm.tqdm.write("processing 1 tile", file=verbose_dst)

                # run process on tile
                for result in mp.batch_processor(tile=tile):
                    utils.write_verbose_msg(result, dst=verbose_dst)

                tqdm.tqdm.write(
                    "processing %s finished in %s" % (mapchete_file, t),
                    file=verbose_dst
                )

                # write VRT index
                if vrt:
                    with mapchete.Timer() as t_vrt:
                        tqdm.tqdm.write("creating VRT", file=verbose_dst)
                        for tile in tqdm.tqdm(
                            zoom_index_gen(
                                mp=mp,
                                zoom=tile.zoom,
                                out_dir=idx_out_dir or mp.config.output.path,
                                vrt=vrt,
                            ),
                            total=mp.count_tiles(tile.zoom, tile.zoom),
                            unit="tile",
                            disable=debug or no_pbar
                        ):
                            logger.debug("%s indexed", tile)

                        tqdm.tqdm.write(
                            "VRT(s) creation for %s finished in %s" % (
                                mapchete_file, t_vrt
                            ),
                            file=verbose_dst
                        )


def _process_area(
    debug=None,
    mapchete_file=None,
    mode=None,
    zoom=None,
    wkt_geometry=None,
    point=None,
    bounds=None,
    input_file=None,
    multi=None,
    verbose_dst=None,
    max_chunksize=None,
    no_pbar=None,
    vrt=None,
    idx_out_dir=None,
):
    with click_spinner.spinner(disable=debug) as spinner:
        with mapchete.Timer() as t:
            with mapchete.open(
                mapchete_file,
                mode=mode,
                zoom=zoom,
                bounds=bounds_from_opts(
                    wkt_geometry=wkt_geometry,
                    point=point,
                    bounds=bounds,
                    raw_conf=raw_conf(mapchete_file)
                ),
                single_input_file=input_file
            ) as mp:
                spinner.stop()
                tiles_count = mp.count_tiles(
                    min(mp.config.init_zoom_levels),
                    max(mp.config.init_zoom_levels)
                )

                tqdm.tqdm.write(
                    "processing %s tile(s) on %s worker(s)" % (tiles_count, multi),
                    file=verbose_dst
                )

                # run process on tiles
                for process_info in tqdm.tqdm(
                    mp.batch_processor(
                        multi=multi,
                        zoom=zoom,
                        max_chunksize=max_chunksize
                    ),
                    total=tiles_count,
                    unit="tile",
                    disable=debug or no_pbar
                ):
                    utils.write_verbose_msg(process_info, dst=verbose_dst)

                tqdm.tqdm.write(
                    "processing %s finished in %s" % (mapchete_file, t),
                    file=verbose_dst
                )

            # write VRT index
            if vrt:
                with mapchete.Timer() as t_vrt:
                    tqdm.tqdm.write("creating VRT(s)", file=verbose_dst)
                    for tile in tqdm.tqdm(
                        zoom_index_gen(
                            mp=mp,
                            zoom=mp.config.init_zoom_levels,
                            out_dir=idx_out_dir or mp.config.output.path,
                            vrt=vrt
                        ),
                        total=mp.count_tiles(
                            min(mp.config.init_zoom_levels),
                            max(mp.config.init_zoom_levels)
                        ),
                        unit="tile",
                        disable=debug or no_pbar
                    ):
                        logger.debug("%s indexed", tile)

                    tqdm.tqdm.write(
                        "VRT(s) creation for %s finished in %s" % (mapchete_file, t_vrt),
                        file=verbose_dst
                    )
