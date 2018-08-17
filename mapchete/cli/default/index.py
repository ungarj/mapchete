"""Create index for process output."""

import click
import logging
import os
from shapely import wkt
import sys
import tqdm
import yaml

import mapchete
from mapchete.cli import _utils
from mapchete.config import _map_to_new_config
from mapchete.index import zoom_index_gen
from mapchete.tile import BufferedTilePyramid


# workaround for https://github.com/tqdm/tqdm/issues/481
tqdm.monitor_interval = 0

logger = logging.getLogger(__name__)


@click.command(help="Create index of output tiles.")
@_utils.arg_mapchete_files
@_utils.opt_out_dir
@_utils.opt_geojson
@_utils.opt_gpkg
@_utils.opt_shp
@_utils.opt_txt
@_utils.opt_fieldname
@_utils.opt_basepath
@_utils.opt_for_gdal
@_utils.opt_zoom
@_utils.opt_bounds
@_utils.opt_wkt_geometry
@_utils.opt_tile
@_utils.opt_verbose
@_utils.opt_debug
@_utils.opt_logfile
def index(
    mapchete_files,
    out_dir=None,
    geojson=False,
    gpkg=False,
    shp=False,
    txt=False,
    fieldname=None,
    basepath=None,
    for_gdal=False,
    zoom=None,
    bounds=None,
    wkt_geometry=None,
    tile=None,
    verbose=False,
    debug=False,
    logfile=None
):
    if not any([geojson, gpkg, shp, txt]):
        raise click.MissingParameter(
            "At least one of '--geojson', '--gpkg', '--shp', or '--txt' must be provided.",
            param_type="option"
        )

    # send verbose output to /dev/null if not activated
    if debug or not verbose:
        verbose_dst = open(os.devnull, 'w')
    else:
        verbose_dst = sys.stdout

    for mapchete_file in mapchete_files:
        tqdm.tqdm.write("create index for %s" % mapchete_file, file=verbose_dst)
        # process single tile
        if tile:
            conf = _map_to_new_config(
                yaml.load(open(mapchete_file, "r").read()))
            tile = BufferedTilePyramid(
                conf["pyramid"]["grid"],
                metatiling=conf["pyramid"].get("metatiling", 1),
                pixelbuffer=conf["pyramid"].get("pixelbuffer", 0)
            ).tile(*tile)
            with mapchete.open(
                mapchete_file, mode="readonly", bounds=tile.bounds,
                zoom=tile.zoom
            ) as mp:
                out_dir = out_dir if out_dir else mp.config.output.path
                for tile in tqdm.tqdm(
                    zoom_index_gen(
                        mp=mp,
                        zoom=tile.zoom,
                        out_dir=out_dir,
                        geojson=geojson,
                        gpkg=gpkg,
                        shapefile=shp,
                        txt=txt,
                        fieldname=fieldname,
                        basepath=basepath,
                        for_gdal=for_gdal),
                    total=mp.count_tiles(tile.zoom, tile.zoom),
                    unit="tile",
                    disable=debug
                ):
                    logger.debug(tile)

        else:
            if wkt_geometry:
                bounds = wkt.loads(wkt_geometry).bounds
            else:
                bounds = bounds
            with mapchete.open(
                mapchete_file, mode="readonly", zoom=zoom, bounds=bounds
            ) as mp:
                out_dir = out_dir if out_dir else mp.config.output.path
                logger.debug("process bounds: %s", mp.config.init_bounds)
                logger.debug("process zooms: %s", mp.config.init_zoom_levels)
                logger.debug("fieldname: %s", fieldname)
                for z in mp.config.init_zoom_levels:
                    logger.debug("zoom %s", z)
                    for tile in tqdm.tqdm(
                        zoom_index_gen(
                            mp=mp,
                            zoom=z,
                            out_dir=out_dir,
                            geojson=geojson,
                            gpkg=gpkg,
                            shapefile=shp,
                            txt=txt,
                            fieldname=fieldname,
                            basepath=basepath,
                            for_gdal=for_gdal),
                        total=mp.count_tiles(z, z),
                        unit="tile",
                        disable=debug
                    ):
                        logger.debug(tile)

        tqdm.tqdm.write("index creation finished", file=verbose_dst)
