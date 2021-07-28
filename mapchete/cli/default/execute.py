import click
import tqdm

import mapchete
from mapchete import commands
from mapchete.cli import utils


@click.command(help="Execute a process.")
@utils.arg_mapchete_files
@utils.opt_zoom
@utils.opt_bounds
@utils.opt_bounds_crs
@utils.opt_area
@utils.opt_area_crs
@utils.opt_point
@utils.opt_point_crs
@utils.opt_tile
@utils.opt_overwrite
@utils.opt_concurrency
@utils.opt_multi
@utils.opt_dask_scheduler
@utils.opt_input_file
@utils.opt_logfile
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_max_chunksize
@utils.opt_multiprocessing_start_method
@utils.opt_vrt
@utils.opt_idx_out_dir
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
                    unit="tile",
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
