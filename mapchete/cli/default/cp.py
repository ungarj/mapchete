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
from mapchete.formats import read_output_metadata
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
@utils.opt_http_username
@utils.opt_http_password
@utils.opt_src_fs_opts
@utils.opt_dst_fs_opts
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
    password=None,
    src_fs_opts=None,
    dst_fs_opts=None,
):
    """Copy TileDirectory."""
    if zoom is None:  # pragma: no cover
        raise click.UsageError("zoom level(s) required")

    src_fs = fs_from_path(input_, username=username, password=password, **src_fs_opts)
    dst_fs = fs_from_path(output, username=username, password=password, **dst_fs_opts)

    # open source tile directory
    with mapchete.open(
        input_,
        zoom=zoom,
        area=area,
        area_crs=area_crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        wkt_geometry=wkt_geometry,
        username=username,
        password=password,
        fs=src_fs,
        fs_kwargs=src_fs_opts,
    ) as src_mp:
        tp = src_mp.config.output_pyramid

        # copy metadata to destination if necessary
        src_metadata = os.path.join(input_, "metadata.json")
        dst_metadata = os.path.join(output, "metadata.json")
        if not dst_fs.exists(dst_metadata):
            logger.debug(f"copy {src_metadata} to {dst_metadata}")
            _copy(src_fs, src_metadata, dst_fs, dst_metadata)

        with mapchete.open(
            output,
            zoom=zoom,
            area=area,
            area_crs=area_crs,
            bounds=bounds,
            bounds_crs=bounds_crs,
            wkt_geometry=wkt_geometry,
            username=username,
            password=password,
            fs=dst_fs,
            fs_kwargs=dst_fs_opts,
        ) as dst_mp:
            for z in range(min(zoom), max(zoom) + 1):
                click.echo(f"copy zoom {z}...")
                # materialize all tiles
                aoi_geom = src_mp.config.area_at_zoom(z)
                tiles = [
                    t
                    for t in tp.tiles_from_geom(aoi_geom, z)
                    # this is required to omit tiles touching the config area
                    if aoi_geom.intersection(t.bbox).area
                ]

                # check which source tiles exist
                logger.debug("looking for existing source tiles...")
                src_tiles_exist = {
                    tile: exists
                    for tile, exists in tiles_exist(
                        config=src_mp.config, output_tiles=tiles, multi=multi
                    )
                }

                logger.debug("looking for existing destination tiles...")
                # chech which destination tiles exist
                dst_tiles_exist = {
                    tile: exists
                    for tile, exists in tiles_exist(
                        config=dst_mp.config, output_tiles=tiles, multi=multi
                    )
                }

                # copy
                for tile in tqdm.tqdm(tiles, unit="tile", disable=debug or no_pbar):
                    src_path = src_mp.config.output_reader.get_path(tile)
                    # only copy if source tile exists
                    if src_tiles_exist[tile]:
                        # skip if destination tile exists and overwrite is deactivated
                        if dst_tiles_exist[tile] and not overwrite:
                            logger.debug(f"{tile}: destination tile exists")
                            continue
                        # copy from source to target
                        else:
                            dst_path = dst_mp.config.output_reader.get_path(tile)
                            logger.debug(f"{tile}: copy {src_path} to {dst_path}")
                            _copy(src_fs, src_path, dst_fs, dst_path)
                    else:
                        logger.debug(f"{tile}: source tile ({src_path}) does not exist")


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
