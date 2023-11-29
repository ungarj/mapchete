import click
import tqdm

import mapchete
from mapchete import commands
from mapchete.cli import options
from mapchete.cli.progress_bar import PBar
from mapchete.config import DaskSettings


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
@options.opt_dask_scheduler
@options.opt_dask_no_task_graph
@options.opt_input_file
@options.opt_logfile
@options.opt_verbose
@options.opt_no_pbar
@options.opt_debug
@options.opt_profiling
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
    input_file=None,
    dask_no_task_graph=False,
    profiling=False,
    logfile=None,
    dask_scheduler=None,
    **kwargs,
):
    # activate verbose if profiling is active
    verbose = verbose or profiling

    if input_file is not None:  # pragma: no cover
        raise click.BadOptionUsage(
            "input-file",
            "'--input-file' is deprecated.",
        )
    for mapchete_file in mapchete_files:
        tqdm.tqdm.write(f"preparing to process {mapchete_file}")
        with mapchete.Timer() as t:
            with PBar(
                total=100,
                desc="tasks",
                disable=debug or no_pbar,
                print_messages=verbose,
            ) as pbar:
                commands.execute(
                    mapchete_file,
                    *args,
                    observers=[pbar],
                    dask_settings=DaskSettings(
                        process_graph=not dask_no_task_graph, scheduler=dask_scheduler
                    ),
                    profiling=profiling,
                    **kwargs,
                )
            tqdm.tqdm.write(f"processing {mapchete_file} finished in {t}")

            if vrt:
                tqdm.tqdm.write("creating VRT(s)")
                commands.index(
                    mapchete_file,
                    *args,
                    vrt=vrt,
                    idx_out_dir=idx_out_dir,
                    observers=[pbar],
                    **kwargs,
                )
                tqdm.tqdm.write(f"index(es) creation for {mapchete_file} finished")
