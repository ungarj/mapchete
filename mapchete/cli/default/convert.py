import click
import tqdm
from rasterio.dtypes import dtype_ranges
from rasterio.enums import Resampling
from rasterio.rio.options import creation_options

import mapchete
from mapchete import commands
from mapchete.cli import options
from mapchete.cli.progress_bar import PBar
from mapchete.config import DaskSettings
from mapchete.formats import available_output_formats

OUTPUT_FORMATS = available_output_formats()


def _validate_bidx(ctx, param, bidx):
    if bidx:
        try:
            return list(map(int, bidx.split(",")))
        except ValueError:
            raise click.BadParameter("band indexes must be positive integer values")


@click.command(help="Convert outputs or other geodata.")
@options.arg_tiledir
@options.arg_output
@options.opt_zoom
@options.opt_bounds
@options.opt_bounds_crs
@options.opt_area
@options.opt_area_crs
@options.opt_point
@options.opt_point_crs
@click.option(
    "--clip-geometry",
    "-c",
    type=click.Path(),
    help="Clip output by geometry.",
)
@click.option("--bidx", callback=_validate_bidx, help="Band indexes to copy.")
@click.option(
    "--output-pyramid",
    type=click.STRING,
    help="Output pyramid to write to.",
)
@click.option(
    "--output-metatiling",
    type=click.INT,
    help="Output metatiling.",
)
@click.option(
    "--output-format",
    type=click.Choice(OUTPUT_FORMATS),
    help="Output format.",
)
@click.option(
    "--output-dtype",
    type=click.Choice(dtype_ranges.keys()),
    help="Output data type (for raster output only).",
)
@click.option(
    "--output-geometry-type",
    type=click.STRING,
    help="Output geometry type (for vector output only).",
)
@creation_options
@click.option(
    "--scale-ratio",
    type=click.FLOAT,
    default=1.0,
    help="Scaling factor (for raster output only).",
)
@click.option(
    "--scale-offset",
    type=click.FLOAT,
    default=0.0,
    help="Scaling offset (for raster output only).",
)
@options.opt_resampling_method
@click.option(
    "--overviews", is_flag=True, help="Generate overviews (single GTiff output only)."
)
@click.option(
    "--overviews-resampling-method",
    type=click.Choice([it.name for it in Resampling if it.value in range(8)]),
    default="cubic_spline",
    help="Resampling method used for overviews. (default: cubic_spline)",
)
@click.option(
    "--cog",
    is_flag=True,
    help="Write a valid COG. This will automatically generate verviews. (GTiff only)",
)
@options.opt_overwrite
@options.opt_verbose
@options.opt_no_pbar
@options.opt_debug
@options.opt_workers
@options.opt_concurrency
@options.opt_dask_no_task_graph
@options.opt_logfile
@options.opt_vrt
@options.opt_idx_out_dir
@options.opt_src_fs_opts
@options.opt_dst_fs_opts
def convert(
    tiledir,
    output,
    *args,
    vrt=False,
    idx_out_dir=None,
    debug=False,
    no_pbar=False,
    verbose=False,
    dask_no_task_graph=False,
    logfile=None,
    **kwargs,
):
    with mapchete.Timer() as t:
        with PBar(
            total=100, desc="tasks", disable=debug or no_pbar, print_messages=verbose
        ) as pbar:
            commands.convert(
                tiledir,
                output,
                *args,
                dask_settings=DaskSettings(process_graph=not dask_no_task_graph),
                observers=[pbar],
                **kwargs,
            )
        tqdm.tqdm.write(f"processing {tiledir} finished in {t}")

        if vrt:
            tqdm.tqdm.write("creating VRT(s)")
            commands.index(
                output,
                *args,
                vrt=vrt,
                idx_out_dir=idx_out_dir,
                observers=[pbar],
                **kwargs,
            )
            tqdm.tqdm.write(f"index(es) creation for {tiledir} finished")
