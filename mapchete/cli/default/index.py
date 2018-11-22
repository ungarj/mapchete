"""Create index for process output."""

import click
import click_spinner
import logging
import os
from shapely import wkt
import sys
import tqdm
import yaml

import mapchete
from mapchete.cli import utils
from mapchete.config import _map_to_new_config
from mapchete.index import zoom_index_gen
from mapchete.tile import BufferedTilePyramid


# workaround for https://github.com/tqdm/tqdm/issues/481
tqdm.monitor_interval = 0

logger = logging.getLogger(__name__)


@click.command(help="Create index of output tiles.")
@utils.arg_mapchete_files
@utils.opt_idx_out_dir
@utils.opt_geojson
@utils.opt_gpkg
@utils.opt_shp
@utils.opt_vrt
@utils.opt_txt
@utils.opt_fieldname
@utils.opt_basepath
@utils.opt_for_gdal
@utils.opt_zoom
@utils.opt_bounds
@utils.opt_wkt_geometry
@utils.opt_tile
@utils.opt_verbose
@utils.opt_debug
@utils.opt_logfile
def index(
    mapchete_files,
    idx_out_dir=None,
    geojson=False,
    gpkg=False,
    shp=False,
    vrt=False,
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
    if not any([geojson, gpkg, shp, txt, vrt]):
        raise click.MissingParameter(
            """At least one of '--geojson', '--gpkg', '--shp', '--vrt' or '--txt'"""
            """must be provided.""",
            param_type="option"
        )

    # send verbose output to /dev/null if not activated
    verbose_dst = open(os.devnull, 'w') if debug or not verbose else sys.stdout

    for mapchete_file in mapchete_files:
        tqdm.tqdm.write("create index for %s" % mapchete_file, file=verbose_dst)
        with click_spinner.spinner(disable=debug) as spinner:
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
                    spinner.stop()
                    for tile in tqdm.tqdm(
                        zoom_index_gen(
                            mp=mp,
                            zoom=tile.zoom,
                            out_dir=idx_out_dir if idx_out_dir else mp.config.output.path,
                            geojson=geojson,
                            gpkg=gpkg,
                            shapefile=shp,
                            vrt=vrt,
                            txt=txt,
                            fieldname=fieldname,
                            basepath=basepath,
                            for_gdal=for_gdal
                        ),
                        total=mp.count_tiles(tile.zoom, tile.zoom),
                        unit="tile",
                        disable=debug
                    ):
                        logger.debug(tile)

            else:
                with mapchete.open(
                    mapchete_file,
                    mode="readonly",
                    zoom=zoom,
                    bounds=wkt.loads(wkt_geometry).bounds if wkt_geometry else bounds
                ) as mp:
                    spinner.stop()
                    logger.debug("process bounds: %s", mp.config.init_bounds)
                    logger.debug("process zooms: %s", mp.config.init_zoom_levels)
                    logger.debug("fieldname: %s", fieldname)
                    for z in mp.config.init_zoom_levels:
                        logger.debug("zoom %s", z)
                        for tile in tqdm.tqdm(
                            zoom_index_gen(
                                mp=mp,
                                zoom=z,
                                out_dir=(
                                    idx_out_dir if idx_out_dir else mp.config.output.path
                                ),
                                geojson=geojson,
                                gpkg=gpkg,
                                shapefile=shp,
                                vrt=vrt,
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
