from aiohttp import BasicAuth
import click
import fiona
import json
import logging
import os
from shapely.geometry import box, shape
from shapely.ops import unary_union
from tilematrix import TilePyramid
import tqdm

from mapchete.cli import utils
from mapchete.io import fs_from_path, tiles_exist

logger = logging.getLogger(__name__)


@click.command(help="Copy TileDirectory from one source to another.")
@utils.arg_input
@utils.arg_output
@utils.opt_zoom
@utils.opt_area
@utils.opt_area_crs
@utils.opt_bounds
@utils.opt_bounds_crs
@utils.opt_point
@utils.opt_point_crs
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
    point=None,
    point_crs=None,
    wkt_geometry=None,
    aoi=None,
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
    with src_fs.open(os.path.join(input_, "metadata.json")) as src:
        metadata = json.loads(src.read())
        _format = metadata["driver"]["format"]
        if _format == "GTiff":
            file_extension = ".tif"
        elif _format in ["PNG", "PNG_hillshade"]:
            file_extension = ".png"
        elif _format == "GeoJSON":
            file_extension = ".geojson"
        else:
            raise TypeError(f"cannot determine file extension from format {_format}")
        tp = TilePyramid.from_dict(
            {
                k: v for k, v in metadata.get("pyramid").items()
                if k in ["grid", "tile_size", "metatiling"]
            }
        )

    # get aoi
    if aoi:
        with fiona.open(aoi) as src:
            aoi_geom = unary_union([shape(f["geometry"]) for f in src])
    elif bounds:
        aoi_geom = box(*bounds)
    else:
        aoi_geom = box(*tp.bounds)

    # copy metadata to destination if necessary
    src_path = os.path.join(input_, "metadata.json"),
    dst_path = os.path.join(output, "metadata.json")
    if not dst_fs.exists(dst_path):
        logger.debug(f"copy {src_path} to {dst_path}")
        _copy(src_fs, src_path, dst_fs, dst_path)

    for z in range(min(zoom), max(zoom) + 1):
        click.echo(f"copy zoom {z}...")
        # materialize all tiles
        tiles = [
            t for t in tp.tiles_from_bounds(aoi_geom.bounds, z)
            if aoi_geom.intersection(t.bbox()).area
        ]

        # check which source tiles exist
        src_tiles_exist = {
            tile: exists
            for tile, exists in tiles_exist(
                output_tiles=tiles,
                basepath=input_,
                file_extension=file_extension,
                output_pyramid=tp,
                process_pyramid=tp,
                fs=src_fs,
                multi=multi
            )
        }

        # chech which destination tiles exist
        dst_tiles_exist =  {
            tile: exists
            for tile, exists in tiles_exist(
                output_tiles=tiles,
                basepath=output,
                file_extension=file_extension,
                output_pyramid=tp,
                process_pyramid=tp,
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
