import click
import tqdm

from mapchete import commands
from mapchete.cli import utils


@click.command(help="Copy TileDirectory from one source to another.")
@utils.arg_src_tiledir
@utils.arg_dst_tiledir
@utils.opt_zoom
@utils.opt_area
@utils.opt_area_crs
@utils.opt_bounds
@utils.opt_bounds_crs
@utils.opt_overwrite
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_logfile
@utils.opt_multi
@utils.opt_http_username
@utils.opt_http_password
@utils.opt_src_fs_opts
@utils.opt_dst_fs_opts
def cp(*args, debug=False, no_pbar=False, verbose=False, logfile=None, **kwargs):
    """Copy TileDirectory."""
    # handle deprecated options
    for x in ["password", "username"]:
        if kwargs.get(x):  # pragma: no cover
            raise click.BadOptionUsage(
                x,
                f"'--{x} foo' is deprecated. You should use '--src-fs-opts {x}=foo' instead.",
            )
        kwargs.pop(x)
    # copy
    list(
        tqdm.tqdm(
            commands.cp(
                *args,
                as_iterator=True,
                msg_callback=tqdm.tqdm.write if verbose else None,
                **kwargs,
            ),
            unit="tile",
            disable=debug or no_pbar,
        )
    )
