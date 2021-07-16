import click
import tqdm

from mapchete import commands
from mapchete.cli import utils


@click.command(help="Remove tiles from TileDirectory.")
@utils.arg_tiledir
@utils.opt_zoom
@utils.opt_area
@utils.opt_area_crs
@utils.opt_bounds
@utils.opt_bounds_crs
@utils.opt_multi
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_logfile
@utils.opt_force
@utils.opt_fs_opts
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
    else:
        tqdm.tqdm.write("No tiles found to delete.")
