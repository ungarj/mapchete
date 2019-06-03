"""Command line utility to execute a Mapchete process."""

import click
import logging
from multiprocessing import cpu_count
import os
import sys
import tqdm

from mapchete.cli import utils
from mapchete.config import raw_conf_process_pyramid


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
    verbose_dst = open(os.devnull, 'w') if debug or not verbose else sys.stdout

    for mapchete_file in mapchete_files:
        tqdm.tqdm.write("preparing to process %s" % mapchete_file, file=verbose_dst)
        # process single tile
        if tile:
            utils._process_single_tile(
                raw_conf_process_pyramid=raw_conf_process_pyramid,
                mapchete_config=mapchete_file,
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
            utils._process_area(
                debug=debug,
                mapchete_config=mapchete_file,
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
