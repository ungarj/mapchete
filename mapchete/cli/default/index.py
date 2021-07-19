"""Create index for process output."""

import click
import click_spinner
import logging
import os
import sys
import tqdm

import mapchete
from mapchete.cli import utils
from mapchete import commands


# workaround for https://github.com/tqdm/tqdm/issues/481
tqdm.monitor_interval = 0

logger = logging.getLogger(__name__)


@click.command(help="Create index of output tiles.")
@utils.arg_tiledir
@utils.opt_idx_out_dir
@utils.opt_geojson
@utils.opt_gpkg
@utils.opt_shp
@utils.opt_vrt
@utils.opt_txt
@utils.opt_fieldname
@utils.opt_basepath
@utils.opt_for_gdal
@utils.opt_zoom
@utils.opt_bounds
@utils.opt_bounds_crs
@utils.opt_area
@utils.opt_area_crs
@utils.opt_point
@utils.opt_point_crs
@utils.opt_tile
@utils.opt_http_username
@utils.opt_http_password
@utils.opt_fs_opts
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_logfile
def index(*args, debug=False, no_pbar=False, verbose=False, logfile=None, **kwargs):
    """Create various index files from process output."""
    # handle deprecated options
    for x in ["password", "username"]:
        if kwargs.get(x):  # pragma: no cover
            raise click.BadOptionUsage(
                x,
                f"'--{x} foo' is deprecated. You should use '--fs-opts {x}=foo' instead.",
            )
        kwargs.pop(x)
    list(
        tqdm.tqdm(
            commands.index(
                *args,
                as_iterator=True,
                msg_callback=tqdm.tqdm.write if verbose else None,
                **kwargs,
            ),
            unit="tile",
            disable=debug or no_pbar,
        )
    )
    tqdm.tqdm.write(f"index(es) creation for {kwargs.get('tiledir')} finished")
