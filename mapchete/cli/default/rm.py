import click
import tqdm

from mapchete import commands
from mapchete.cli import options


@click.command(help="Remove tiles from TileDirectory.")
@options.arg_tiledir
@options.opt_zoom
@options.opt_area
@options.opt_area_crs
@options.opt_bounds
@options.opt_bounds_crs
@options.opt_multi
@options.opt_verbose
@options.opt_no_pbar
@options.opt_debug
@options.opt_logfile
@options.opt_force
@options.opt_fs_opts
def rm(
    *args,
    force=False,
    debug=False,
    no_pbar=False,
    verbose=False,
    logfile=None,
    **kwargs,
):
    """Remove tiles from TileDirectory."""
    tiles_to_delete = commands.rm(
        *args,
        as_iterator=True,
        msg_callback=tqdm.tqdm.write if verbose else None,
        **kwargs,
    )
    if len(tiles_to_delete):
        if force or click.confirm(
            f"Do you want to delete {len(tiles_to_delete)} tiles?", abort=True
        ):
            list(
                tqdm.tqdm(
                    tiles_to_delete,
                    unit="tile",
                    disable=debug or no_pbar,
                )
            )
    else:  # pragma: no cover
        tqdm.tqdm.write("No tiles found to delete.")
