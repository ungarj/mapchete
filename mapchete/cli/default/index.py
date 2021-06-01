"""Create index for process output."""

import click
import click_spinner
import logging
import os
import sys
import tqdm

import mapchete
from mapchete.cli import utils
from mapchete.index import zoom_index_gen


# workaround for https://github.com/tqdm/tqdm/issues/481
tqdm.monitor_interval = 0

logger = logging.getLogger(__name__)


@click.command(help="Create index of output tiles.")
@utils.arg_inputs
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
@utils.opt_bounds_crs
@utils.opt_area
@utils.opt_area_crs
@utils.opt_point
@utils.opt_point_crs
@utils.opt_wkt_geometry
@utils.opt_tile
@utils.opt_http_username
@utils.opt_http_password
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_logfile
def index(
    inputs,
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
    bounds_crs=None,
    area=None,
    area_crs=None,
    point=None,
    point_crs=None,
    wkt_geometry=None,
    tile=None,
    username=None,
    password=None,
    verbose=False,
    no_pbar=False,
    debug=False,
    logfile=None,
):
    if not any([geojson, gpkg, shp, txt, vrt]):
        raise click.MissingParameter(
            """At least one of '--geojson', '--gpkg', '--shp', '--vrt' or '--txt'"""
            """must be provided.""",
            param_type="option",
        )
    # send verbose messages to /dev/null if not activated
    verbose_dst = open(os.devnull, "w") if debug or not verbose else sys.stdout

    for input_ in inputs:

        tqdm.tqdm.write("create index(es) for %s" % input_, file=verbose_dst)

        with click_spinner.spinner(disable=debug) as spinner:

            # process single tile
            if tile:
                with mapchete.open(
                    input_, mode="readonly", username=username, password=password
                ) as mp:
                    spinner.stop()
                    tile = mp.config.process_pyramid.tile(*tile)
                    for tile in tqdm.tqdm(
                        zoom_index_gen(
                            mp=mp,
                            zoom=tile.zoom,
                            out_dir=idx_out_dir
                            if idx_out_dir
                            else mp.config.output.path,
                            geojson=geojson,
                            gpkg=gpkg,
                            shapefile=shp,
                            vrt=vrt,
                            txt=txt,
                            fieldname=fieldname,
                            basepath=basepath,
                            for_gdal=for_gdal,
                        ),
                        total=mp.count_tiles(tile.zoom, tile.zoom),
                        unit="tile",
                        disable=debug or no_pbar,
                    ):
                        logger.debug("%s indexed", tile)

            # process area
            else:
                with mapchete.open(
                    input_,
                    mode="readonly",
                    zoom=zoom,
                    wkt_geometry=wkt_geometry,
                    point=point,
                    point_crs=point_crs,
                    bounds=bounds,
                    bounds_crs=bounds_crs,
                    area=area,
                    area_crs=area_crs,
                    username=username,
                    password=password,
                ) as mp:
                    spinner.stop()
                    logger.debug("process bounds: %s", mp.config.init_bounds)
                    logger.debug("process area: %s", mp.config.init_area)
                    logger.debug("process zooms: %s", mp.config.init_zoom_levels)
                    logger.debug("fieldname: %s", fieldname)
                    for tile in tqdm.tqdm(
                        zoom_index_gen(
                            mp=mp,
                            zoom=mp.config.init_zoom_levels,
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
                            for_gdal=for_gdal,
                        ),
                        total=mp.count_tiles(
                            min(mp.config.init_zoom_levels),
                            max(mp.config.init_zoom_levels),
                        ),
                        unit="tile",
                        disable=debug or no_pbar,
                    ):
                        logger.debug("%s indexed", tile)

        tqdm.tqdm.write("index(es) creation for %s finished" % input_, file=verbose_dst)
