import click
import fiona
import logging
from multiprocessing import cpu_count
import os
import rasterio
from rasterio.enums import Resampling
from rasterio.dtypes import dtype_ranges
from rasterio.rio.options import creation_options
from shapely.geometry import box
import sys
import tilematrix

from mapchete.cli import utils
from mapchete.config import raw_conf, raw_conf_output_pyramid
from mapchete.formats import (
    driver_from_file, available_output_formats, available_input_formats
)
from mapchete.io import read_json, get_best_zoom_level
from mapchete.io.vector import reproject_geometry
from mapchete.tile import BufferedTilePyramid
from mapchete.validate import validate_zooms

logger = logging.getLogger(__name__)
OUTPUT_FORMATS = available_output_formats()


def _validate_bidx(ctx, param, bidx):
    if bidx:
        try:
            return list(map(int, bidx.split(",")))
        except ValueError:
            raise click.BadParameter("band indexes must be positive integer values")


@click.command(help="Copy TileDirectory.")
@utils.arg_input
@utils.arg_output
@utils.opt_zoom
@utils.opt_bounds
@utils.opt_point
@utils.opt_wkt_geometry
@utils.opt_overwrite
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_logfile
@click.option(
    "--username", "-u",
    type=click.STRING,
    help="Username for HTTP Auth."
)
@click.option(
    "--password", "-p",
    type=click.STRING,
    help="Password for HTTP Auth."
)
def cp(
    input_,
    output,
    zoom=None,
    bounds=None,
    point=None,
    wkt_geometry=None,
    overwrite=False,
    verbose=False,
    no_pbar=False,
    debug=False,
    logfile=None,
    username=None,
    password=None
):
    """Copy TileDirectory."""
    protocol, _, host = input.split("/")[:3]
    src_fs = fsspec.filesystem(
        _guess_fstype(input),
        auth=BasicAuth(username, password)
    )
    dst_fs = fsspec.filesystem(_guess_fstype(output))

    # read source TileDirectory metadata
    with src_fs.open(os.path.join(input, "metadata.json")) as src:
        metadata = json.loads(src.read())
        tp = TilePyramid.from_dict(
            {
                k: v for k, v in metadata.get("pyramid").items()
                if k in ["grid", "tile_size", "metatiling"]
            }
        )

    # copy metadata to destination if necessary
    _copy(
        src_fs,
        os.path.join(input, "metadata.json"),
        dst_fs,
        os.path.join(output, "metadata.json")
    )
    for z in zoom:
        click.echo(f"copy zoom {z}...")
        for tile in tqdm.tqdm(
            list(tp.tiles_from_bounds(bounds or tp.bounds, z)),
            unit="tile",
            disable=debug or no_pbar
        ):
            _copy(
                src_fs,
                os.path.join(input, _get_tile_path(tile)),
                dst_fs,
                os.path.join(output, _get_tile_path(tile))
            )


def _copy(src_fs, src_path, dst_fs, dst_path, overwrite=False):
    if dst_fs.exists(dst_path) and not overwrite:
        return
    elif not src_fs.exists(src_path):
        return
    with src_fs.open(src_path, "rb") as src:
        dst_fs.mkdir(os.path.dirname(dst_path), create_parents=True)
        with dst_fs.open(dst_path, "wb") as dst:
            dst.write(src.read())


def _guess_fstype(path):
    if path.startswith(("http://", "https://")):
        return "https"
    elif path.startswith(("s3://")):
        return "s3"
    else:
        return "file"


def _get_tile_path(tile, basepath=None):
    return f"{tile.zoom}/{tile.row}/{tile.col}.tif"
