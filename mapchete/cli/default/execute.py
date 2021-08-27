import click
import tqdm

import mapchete
from mapchete import commands
from mapchete.cli import options


@click.command(help="Execute a process.")
@options.arg_mapchete_files
@options.opt_zoom
@options.opt_bounds
@options.opt_bounds_crs
@options.opt_area
@options.opt_area_crs
@options.opt_point
@options.opt_point_crs
@options.opt_tile
@options.opt_overwrite
@options.opt_concurrency
@options.opt_workers
@options.opt_multi
@options.opt_dask_scheduler
@options.opt_input_file
@options.opt_logfile
@options.opt_verbose
@options.opt_no_pbar
@options.opt_debug
@options.opt_vrt
@options.opt_idx_out_dir
def execute(
    mapchete_files,
    *args,
    vrt=False,
    idx_out_dir=None,
    debug=False,
    no_pbar=False,
    verbose=False,
    logfile=None,
    input_file=None,
    **kwargs,
):
    if input_file is not None:  # pragma: no cover
        raise click.BadOptionUsage(
            "input-file",
            "'--input-file' is deprecated.",
        )
    for mapchete_file in mapchete_files:
        tqdm.tqdm.write(f"preparing to process {mapchete_file}")
        with mapchete.Timer() as t:
            list(
                tqdm.tqdm(
                    commands.execute(
                        mapchete_file,
                        *args,
                        as_iterator=True,
                        msg_callback=tqdm.tqdm.write if verbose else None,
                        **kwargs,
                    ),
                    unit="task",
                    disable=debug or no_pbar,
                )
            )
            tqdm.tqdm.write(f"processing {mapchete_file} finished in {t}")

        if vrt:
            tqdm.tqdm.write("creating VRT(s)")
            list(
                tqdm.tqdm(
                    commands.index(
                        mapchete_file,
                        *args,
                        vrt=vrt,
                        idx_out_dir=idx_out_dir,
                        as_iterator=True,
                        msg_callback=tqdm.tqdm.write if verbose else None,
                        **kwargs,
                    ),
                    unit="tile",
                    disable=debug or no_pbar,
                )
            )
            tqdm.tqdm.write(f"index(es) creation for {mapchete_file} finished")
