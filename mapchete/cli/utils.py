import click
import click_spinner
import logging
from multiprocessing import cpu_count
import os
from rasterio.enums import Resampling
import tilematrix
import tqdm

import mapchete
from mapchete.config import raw_conf, bounds_from_opts
from mapchete.formats import available_output_formats
from mapchete.index import zoom_index_gen
from mapchete.log import set_log_level, setup_logfile
from mapchete.validate import validate_bounds, validate_zooms


logger = logging.getLogger(__name__)


# verbose stdout writer #
#########################
def write_verbose_msg(process_info, dst):
    tqdm.tqdm.write(
        "Tile %s: %s, %s" % (
            tuple(process_info.tile.id), process_info.process_msg, process_info.write_msg
        ),
        file=dst
    )


# click callbacks #
###################
def _validate_zoom(ctx, param, zoom):
    if zoom:
        try:
            zoom_levels = list(map(int, zoom.split(",")))
        except ValueError:
            raise click.BadParameter("zoom levels must be integer values")
        try:
            if len(zoom_levels) > 2:
                raise ValueError("zooms can be maximum two items")
            return validate_zooms(zoom_levels, expand=False)
        except Exception as e:
            raise click.BadParameter(e)


def _validate_bounds(ctx, param, bounds):
    return validate_bounds(bounds) if bounds else None


def _validate_mapchete_files(ctx, param, mapchete_files):
    if len(mapchete_files) == 0:
        raise click.MissingParameter("at least one mapchete file required")
    return mapchete_files


def _set_debug_log_level(ctx, param, debug):
    if debug:
        set_log_level(logging.DEBUG)
    return debug


def _setup_logfile(ctx, param, logfile):
    if logfile:
        setup_logfile(logfile)
    return logfile


# click arguments #
###################
arg_mapchete_file = click.argument("mapchete_file", type=click.Path(exists=True))
arg_create_mapchete_file = click.argument("mapchete_file", type=click.Path())
arg_mapchete_files = click.argument(
    "mapchete_files", type=click.Path(exists=True), nargs=-1,
    callback=_validate_mapchete_files
)
arg_process_file = click.argument("process_file", type=click.Path())
arg_out_format = click.argument(
    "out_format", type=click.Choice(available_output_formats())
)
arg_input_raster = click.argument("input_raster", type=click.Path(exists=True))
arg_out_dir = click.argument("output_dir", type=click.Path())
arg_input = click.argument("input_", metavar="INPUT", type=click.STRING)
arg_output = click.argument("output", type=click.STRING)


# click options #
#################
opt_out_path = click.option(
    "--out-path", "-op",
    type=click.Path(),
    default=os.path.join(os.getcwd(), "output"),
    help="Process output path."
)
opt_idx_out_dir = click.option(
    "--idx-out-dir", "-od",
    type=click.Path(),
    help="Index output directory."
)
opt_input_file = click.option(
    "--input-file", "-i",
    type=click.Path(),
    help=(
        """Specify an input file via command line (in mapchete file, """
        """set 'input_file' parameter to 'from_command_line')."""
    ),
)
opt_zoom = click.option(
    "--zoom", "-z",
    callback=_validate_zoom,
    help="Single zoom level or min and max separated by ','.",
)
opt_bounds = click.option(
    "--bounds", "-b",
    type=click.FLOAT,
    nargs=4,
    callback=_validate_bounds,
    help="Left, bottom, right, top bounds in tile pyramid CRS.",
)
opt_point = click.option(
    "--point", "-p",
    type=click.FLOAT,
    nargs=2,
    help="Process tiles over single point location."
)
opt_wkt_geometry = click.option(
    "--wkt-geometry", "-g",
    type=click.STRING,
    help="Take boundaries from WKT geometry in tile pyramid CRS.",
)
opt_tile = click.option(
    "--tile", "-t",
    type=click.INT,
    nargs=3,
    help="Zoom, row, column of single tile."
)
opt_overwrite = click.option(
    "--overwrite", "-o",
    is_flag=True,
    help="Overwrite if tile(s) already exist(s)."
)
opt_multi = click.option(
    "--multi", "-m",
    type=click.INT,
    help="Number of concurrent processes.",
)
opt_force = click.option(
    "--force", "-f",
    is_flag=True,
    help="Overwrite if files already exist."
)
opt_logfile = click.option(
    "--logfile", "-l",
    type=click.Path(),
    callback=_setup_logfile,
    help="Write debug log infos into file."
)
opt_verbose = click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Print info for each process tile."
)
opt_no_pbar = click.option(
    "--no-pbar",
    is_flag=True,
    help="Deactivate progress bar."
)
opt_debug = click.option(
    "--debug", "-d",
    is_flag=True,
    callback=_set_debug_log_level,
    help="Deactivate progress bar and print debug log output."
)
opt_max_chunksize = click.option(
    "--max-chunksize", "-c",
    type=click.INT,
    default=1,
    help="Maximum number of process tiles to be queued for each  worker. (default: 1)"
)
opt_input_formats = click.option(
    "--input-formats", "-i",
    is_flag=True,
    help="Show only input formats."
)
opt_output_formats = click.option(
    "--output-formats", "-o",
    is_flag=True,
    help="Show only output formats."
)
opt_geojson = click.option(
    "--geojson",
    is_flag=True,
    help="Write GeoJSON index."
)
opt_gpkg = click.option(
    "--gpkg",
    is_flag=True,
    help="Write GeoPackage index."
)
opt_shp = click.option(
    "--shp",
    is_flag=True,
    help="Write Shapefile index."
)
opt_vrt = click.option(
    "--vrt",
    is_flag=True,
    help="Write VRT file."
)
opt_txt = click.option(
    "--txt",
    is_flag=True,
    help="Write output tile paths to text file."
)
opt_fieldname = click.option(
    "--fieldname",
    type=click.STRING,
    default="location",
    help="Field to store tile paths in."
)
opt_basepath = click.option(
    "--basepath",
    type=click.STRING,
    help="Use other base path than given process output path."
)
opt_for_gdal = click.option(
    "--for-gdal",
    is_flag=True,
    help="Make remote paths readable by GDAL (not applied for txt output)."
)
opt_output_format = click.option(
    "--output-format", "-of",
    type=click.Choice(["GTiff", "PNG"]),
    default="GTiff",
    help="Output data format (GTiff or PNG)."
)
opt_pyramid_type = click.option(
    "--pyramid-type", "-pt",
    type=click.Choice(tilematrix._conf.PYRAMID_PARAMS.keys()),
    default="geodetic",
    help="Output pyramid type. (default: geodetic)"
)
opt_resampling_method = click.option(
    "--resampling-method", "-r",
    type=click.Choice([it.name for it in Resampling if it.value in range(8)]),
    default="nearest",
    help=("Resampling method used. (default: nearest)"),
)
opt_port = click.option(
    "--port", "-p",
    type=click.INT,
    default=5000,
    help="Port process is hosted on. (default: 5000)",
)
opt_internal_cache = click.option(
    "--internal-cache", "-c",
    type=click.INT,
    default=1024,
    help="Number of web tiles to be cached in RAM. (default: 1024)",
)
opt_readonly = click.option(
    "--readonly", "-ro",
    is_flag=True,
    help="Just read process output without writing."
)
opt_memory = click.option(
    "--memory", "-mo",
    is_flag=True,
    help="Always get output from freshly processed output."
)


# convenience processing functions #
####################################
def _process_single_tile(
    debug=None,
    raw_conf_process_pyramid=None,
    mapchete_config=None,
    tile=None,
    mode=None,
    input_file=None,
    verbose_dst=None,
    vrt=None,
    idx_out_dir=None,
    no_pbar=None
):
    with click_spinner.spinner(disable=debug) as spinner:
        with mapchete.Timer() as t:
            tile = raw_conf_process_pyramid(raw_conf(mapchete_config)).tile(*tile)
            with mapchete.open(
                mapchete_config,
                mode=mode,
                bounds=tile.bounds,
                zoom=tile.zoom,
                single_input_file=input_file
            ) as mp:
                spinner.stop()
                tqdm.tqdm.write("processing 1 tile", file=verbose_dst)

                # run process on tile
                for result in mp.batch_processor(tile=tile):
                    write_verbose_msg(result, dst=verbose_dst)

            tqdm.tqdm.write(
                (
                    "processing %s finished in %s" % (mapchete_config, t)
                    if isinstance(mapchete_config, str)
                    else "processing finished in %s" % t
                ),
                file=verbose_dst
            )

            # write VRT index
            if vrt:
                with mapchete.Timer() as t_vrt:
                    tqdm.tqdm.write("creating VRT", file=verbose_dst)
                    for tile in tqdm.tqdm(
                        zoom_index_gen(
                            mp=mp,
                            zoom=tile.zoom,
                            out_dir=idx_out_dir or mp.config.output.path,
                            vrt=vrt,
                        ),
                        total=mp.count_tiles(tile.zoom, tile.zoom),
                        unit="tile",
                        disable=debug or no_pbar
                    ):
                        logger.debug("%s indexed", tile)

                    tqdm.tqdm.write(
                        (
                            "VRT(s) for %s created in %s" % (mapchete_config, t_vrt)
                            if isinstance(mapchete_config, str)
                            else "VRT(s) created in %s" % t_vrt
                        ),
                        file=verbose_dst
                    )


def _process_area(
    debug=None,
    mapchete_config=None,
    mode=None,
    zoom=None,
    wkt_geometry=None,
    point=None,
    bounds=None,
    input_file=None,
    multi=None,
    verbose_dst=None,
    max_chunksize=None,
    no_pbar=None,
    vrt=None,
    idx_out_dir=None,
):
    multi = multi or cpu_count()
    with click_spinner.spinner(disable=debug) as spinner:
        with mapchete.Timer() as t:
            with mapchete.open(
                mapchete_config,
                mode=mode,
                zoom=zoom,
                bounds=bounds_from_opts(
                    wkt_geometry=wkt_geometry,
                    point=point,
                    bounds=bounds,
                    raw_conf=raw_conf(mapchete_config)
                ),
                single_input_file=input_file
            ) as mp:
                spinner.stop()
                tiles_count = mp.count_tiles(
                    min(mp.config.init_zoom_levels),
                    max(mp.config.init_zoom_levels)
                )

                tqdm.tqdm.write(
                    "processing %s tile(s) on %s worker(s)" % (tiles_count, multi),
                    file=verbose_dst
                )

                # run process on tiles
                for process_info in tqdm.tqdm(
                    mp.batch_processor(
                        multi=multi,
                        zoom=zoom,
                        max_chunksize=max_chunksize
                    ),
                    total=tiles_count,
                    unit="tile",
                    disable=debug or no_pbar
                ):
                    write_verbose_msg(process_info, dst=verbose_dst)

            tqdm.tqdm.write(
                (
                    "processing %s finished in %s" % (mapchete_config, t)
                    if isinstance(mapchete_config, str)
                    else "processing finished in %s" % t
                ),
                file=verbose_dst
            )

            # write VRT index
            if vrt:
                with mapchete.Timer() as t_vrt:
                    tqdm.tqdm.write("creating VRT(s)", file=verbose_dst)
                    for tile in tqdm.tqdm(
                        zoom_index_gen(
                            mp=mp,
                            zoom=mp.config.init_zoom_levels,
                            out_dir=idx_out_dir or mp.config.output.path,
                            vrt=vrt
                        ),
                        total=mp.count_tiles(
                            min(mp.config.init_zoom_levels),
                            max(mp.config.init_zoom_levels)
                        ),
                        unit="tile",
                        disable=debug or no_pbar
                    ):
                        logger.debug("%s indexed", tile)

                    tqdm.tqdm.write(
                        (
                            "VRT(s) for %s created in %s" % (mapchete_config, t_vrt)
                            if isinstance(mapchete_config, str)
                            else "VRT(s) created in %s" % t_vrt
                        ),
                        file=verbose_dst
                    )
