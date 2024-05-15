import click
import tqdm

from mapchete import commands
from mapchete.cli import options
from mapchete.cli.progress_bar import PBar
from mapchete.commands.rm import existing_paths


@click.command(help="Remove tiles from TileDirectory.")
@options.arg_tiledir
@options.opt_zoom
@options.opt_area
@options.opt_area_crs
@options.opt_bounds
@options.opt_bounds_crs
@options.opt_workers
@options.opt_verbose
@options.opt_no_pbar
@options.opt_debug
@options.opt_logfile
@options.opt_force
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
    tiles_to_delete = existing_paths(
        *args,
        **kwargs,
    )
    if len(tiles_to_delete):
        if force or click.confirm(
            f"Do you want to delete {len(tiles_to_delete)} tiles?", abort=True
        ):
            with PBar(
                total=len(tiles_to_delete),
                desc="tiles",
                disable=debug or no_pbar,
                print_messages=verbose,
            ) as pbar:
                commands.rm(paths=tiles_to_delete, observers=[pbar], **kwargs)
    else:  # pragma: no cover
        tqdm.tqdm.write("No tiles found to delete.")
