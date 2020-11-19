import click
import fiona
import json
import logging
import os
from shapely.geometry import box, shape
from shapely.ops import unary_union
from tilematrix import TilePyramid
import tqdm

import mapchete
from mapchete.cli import utils
from mapchete.config import _guess_geometry
from mapchete.formats import file_extension_from_metadata, read_output_metadata
from mapchete.io import fs_from_path, tiles_exist
from mapchete.io.vector import reproject_geometry

logger = logging.getLogger(__name__)


@click.command(help="Copy TileDirectory from one source to another.")
@utils.arg_input
@utils.arg_output
@utils.opt_zoom
@utils.opt_area
@utils.opt_area_crs
@utils.opt_bounds
@utils.opt_bounds_crs
@utils.opt_wkt_geometry
@utils.opt_overwrite
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_logfile
@utils.opt_multi
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
    area=None,
    area_crs=None,
    bounds=None,
    bounds_crs=None,
    wkt_geometry=None,
    overwrite=False,
    verbose=False,
    no_pbar=False,
    debug=False,
    logfile=None,
    multi=None,
    username=None,
    password=None
):
    """Copy TileDirectory."""
    src_fs = fs_from_path(input_, username=username, password=password)
    dst_fs = fs_from_path(output, username=username, password=password)

    # read source TileDirectory metadata
    metadata = read_output_metadata(
        os.path.join(input_, "metadata.json"),
        fs=src_fs
    )
    file_extension = file_extension_from_metadata(metadata)
    tp = metadata["pyramid"]

    # get aoi
    if area:
        aoi, crs = _guess_geometry(area, base_dir=os.getcwd())
        aoi_geom = reproject_geometry(
            aoi,
            src_crs=crs or area_crs or tp.crs,
            dst_crs=tp.crs
        )
    elif bounds:
        aoi_geom = reproject_geometry(
            box(*bounds),
            src_crs=bounds_crs or tp.crs,
            dst_crs=tp.crs
        )
    else:
        aoi_geom = box(*tp.bounds)

    if aoi_geom.is_empty:
        click.echo("AOI is empty, nothing to copy.")
        return

    # copy metadata to destination if necessary
    src_path = os.path.join(input_, "metadata.json")
    dst_path = os.path.join(output, "metadata.json")
    if not dst_fs.exists(dst_path):
        logger.debug(f"copy {src_path} to {dst_path}")
        _copy(src_fs, src_path, dst_fs, dst_path)

    for z in range(min(zoom), max(zoom) + 1):
        click.echo(f"copy zoom {z}...")
        # materialize all tiles
        tiles = [
            t for t in tp.tiles_from_bounds(aoi_geom.bounds, z)
            if aoi_geom.intersection(t.bbox).area
        ]

        # check which source tiles exist
        src_tiles_exist = {
            tile: exists
            for tile, exists in tiles_exist(
                output_tiles=tiles,
                basepath=input_,
                file_extension=file_extension,
                output_pyramid=tp,
                fs=src_fs,
                multi=multi
            )
        }

        # chech which destination tiles exist
        dst_tiles_exist = {
            tile: exists
            for tile, exists in tiles_exist(
                output_tiles=tiles,
                basepath=output,
                file_extension=file_extension,
                output_pyramid=tp,
                fs=dst_fs,
                multi=multi
            )
        }

        # copy
        for tile in tqdm.tqdm(
            tiles,
            unit="tile",
            disable=debug or no_pbar
        ):
            # only copy if source tile exists
            if src_tiles_exist[tile]:
                # skip if destination tile exists and overwrite is deactivated
                if dst_tiles_exist[tile] and not overwrite:
                    logger.debug(f"{tile}: destination tile exists")
                    continue
                # copy from source to target
                else:
                    src_path = os.path.join(input_, _get_tile_path(tile))
                    dst_path = os.path.join(output, _get_tile_path(tile))
                    logger.debug(f"{tile}: copy {src_path} to {dst_path}")
                    _copy(src_fs, src_path, dst_fs, dst_path)
            else:
                logger.debug(f"{tile}: source tile does not exist")


def _copy(src_fs, src_path, dst_fs, dst_path):
    # create parent directories on local filesystems
    if dst_fs.protocol == "file":
        dst_fs.mkdir(os.path.dirname(dst_path), create_parents=True)

    # copy either within a filesystem or between filesystems
    if src_fs == dst_fs:
        src_fs.copy(src_path, dst_path)
    else:
        with src_fs.open(src_path, "rb") as src:
            with dst_fs.open(dst_path, "wb") as dst:
                dst.write(src.read())


def _get_tile_path(tile, basepath=None):
    return f"{tile.zoom}/{tile.row}/{tile.col}.tif"
