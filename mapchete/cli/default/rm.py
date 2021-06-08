import click
import logging
import tqdm

import mapchete
from mapchete.cli import utils
from mapchete.io import fs_from_path, rm, tiles_exist

logger = logging.getLogger(__name__)


@click.command("rm", help="Remove tiles from TileDirectory.")
@utils.arg_input
@utils.opt_zoom
@utils.opt_area
@utils.opt_area_crs
@utils.opt_bounds
@utils.opt_bounds_crs
@utils.opt_wkt_geometry
@utils.opt_multi
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_logfile
@utils.opt_force
@utils.opt_fs_opts
def rm_(
    input_,
    zoom=None,
    area=None,
    area_crs=None,
    bounds=None,
    bounds_crs=None,
    wkt_geometry=None,
    multi=None,
    verbose=False,
    no_pbar=False,
    debug=False,
    logfile=None,
    force=None,
    fs_opts=None,
):
    """Copy TileDirectory."""
    if zoom is None:  # pragma: no cover
        raise click.UsageError("zoom level(s) required")

    src_fs = fs_from_path(input_, **fs_opts)

    # open source tile directory
    with mapchete.open(
        input_,
        zoom=zoom,
        area=area,
        area_crs=area_crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        wkt_geometry=wkt_geometry,
        fs=src_fs,
        fs_kwargs=fs_opts,
        mode="readonly",
    ) as src_mp:
        tp = src_mp.config.output_pyramid

        tiles = {}
        for z in range(min(zoom), max(zoom) + 1):
            tiles[z] = []
            # check which source tiles exist
            logger.debug(f"looking for existing source tiles in zoom {z}...")
            for tile, exists in tiles_exist(
                config=src_mp.config,
                output_tiles=[
                    t
                    for t in tp.tiles_from_geom(src_mp.config.area_at_zoom(z), z)
                    # this is required to omit tiles touching the config area
                    if src_mp.config.area_at_zoom(z).intersection(t.bbox).area
                ],
                multi=multi,
            ):
                if exists:
                    tiles[z].append(tile)

        total_tiles = sum([len(v) for v in tiles.values()])

        if total_tiles:
            if force or click.confirm(
                f"Do you want to delete {total_tiles} tiles?", abort=True
            ):
                # remove
                rm(
                    [
                        src_mp.config.output_reader.get_path(tile)
                        for zoom_tiles in tiles.values()
                        for tile in zoom_tiles
                    ],
                    fs=src_fs,
                )
        else:  # pragma: no cover
            click.echo("No tiles found to delete.")
