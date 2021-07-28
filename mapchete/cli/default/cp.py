import click
import tqdm

from mapchete import commands
from mapchete.cli import options


@click.command(help="Copy TileDirectory from one source to another.")
@options.arg_src_tiledir
@options.arg_dst_tiledir
@options.opt_zoom
@options.opt_area
@options.opt_area_crs
@options.opt_bounds
@options.opt_bounds_crs
@options.opt_overwrite
@options.opt_verbose
@options.opt_no_pbar
@options.opt_debug
@options.opt_logfile
@options.opt_workers
@options.opt_dask_scheduler
@options.opt_concurrency
@options.opt_http_username
@options.opt_http_password
@options.opt_src_fs_opts
@options.opt_dst_fs_opts
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
