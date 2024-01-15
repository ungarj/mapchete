"""Create index for process output."""

import logging

import click
import tqdm

from mapchete import commands
from mapchete.cli import options
from mapchete.cli.progress_bar import PBar

# workaround for https://github.com/tqdm/tqdm/issues/481
tqdm.monitor_interval = 0

logger = logging.getLogger(__name__)


@click.command(help="Create index of output tiles.")
@options.arg_tiledir
@options.opt_idx_out_dir
@options.opt_geojson
@options.opt_gpkg
@options.opt_shp
@options.opt_fgb
@options.opt_vrt
@options.opt_txt
@options.opt_fieldname
@options.opt_basepath
@options.opt_for_gdal
@options.opt_zoom
@options.opt_bounds
@options.opt_bounds_crs
@options.opt_area
@options.opt_area_crs
@options.opt_point
@options.opt_point_crs
@options.opt_tile
@options.opt_http_username
@options.opt_http_password
@options.opt_fs_opts
@options.opt_verbose
@options.opt_no_pbar
@options.opt_debug
@options.opt_logfile
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

    with PBar(
        total=100, desc="tiles", disable=debug or no_pbar, print_messages=verbose
    ) as pbar:
        kwargs.update(some_input=kwargs.pop("tiledir"))
        commands.index(
            *args,
            observers=[pbar],
            **kwargs,
        )
    tqdm.tqdm.write(f"index(es) creation for {kwargs.get('tiledir')} finished")
