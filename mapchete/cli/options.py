import logging
import os
from multiprocessing import get_all_start_methods

import click
import tilematrix
from rasterio.enums import Resampling

from mapchete.bounds import Bounds
from mapchete.config import MULTIPROCESSING_DEFAULT_START_METHOD
from mapchete.formats import available_output_formats
from mapchete.io import MPath
from mapchete.log import set_log_level, setup_logfile
from mapchete.zoom_levels import ZoomLevels
from mapchete.validate import validate_crs

logger = logging.getLogger(__name__)


MULTIPROCESSING_START_METHODS = get_all_start_methods()


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
            return ZoomLevels.from_inp(zoom_levels)
        except Exception as exc:
            raise click.BadParameter(str(exc))


def _validate_bounds(ctx, param, bounds):
    if bounds:
        return Bounds.from_inp(bounds)


def _validate_crs(ctx, param, crs):
    return validate_crs(crs) if crs else None


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


def _cb_key_val(ctx, param, value):
    """
    from: https://github.com/mapbox/rasterio/blob/69305c72b58b15a96330d371ad90ef31c209e981/rasterio/rio/options.py

    click callback to validate `--opt KEY1=VAL1 --opt KEY2=VAL2` and collect
    in a dictionary like the one below, which is what the CLI function receives.
    If no value or `None` is received then an empty dictionary is returned.
        {
            'KEY1': 'VAL1',
            'KEY2': 'VAL2'
        }
    Note: `==VAL` breaks this as `str.split('=', 1)` is used.
    """

    if not value:
        return {}
    else:
        out = {}
        for pair in value:
            if "=" not in pair:  # pragma: no cover
                raise click.BadParameter(
                    "Invalid syntax for KEY=VAL arg: {}".format(pair)
                )
            else:
                k, v = pair.split("=", 1)
                # cast numbers
                for func in (int, float):
                    try:
                        v = func(v)
                    except Exception:
                        pass
                # cast bools and None
                if isinstance(v, str):
                    if v.lower() in ["true", "yes"]:
                        v = True
                    elif v.lower() in ["false", "no"]:
                        v = False
                    elif v.lower() in ["none", "null", "nil", "nada"]:
                        v = None
                out[k.lower()] = v
        return out


def _cb_none_concurrency(ctx, param, value):
    return None if value == "none" else value


# click arguments #
###################
arg_mapchete_file = click.argument("mapchete_file", type=click.Path())
arg_create_mapchete_file = click.argument("mapchete_file", type=click.Path())
arg_mapchete_files = click.argument(
    "mapchete_files",
    type=click.Path(),
    nargs=-1,
    callback=_validate_mapchete_files,
)
arg_process_file = click.argument("process_file", type=click.Path())
arg_out_format = click.argument(
    "out_format", type=click.Choice(available_output_formats())
)
arg_input_raster = click.argument("input_raster", type=click.Path())
arg_out_dir = click.argument("output_dir", type=click.Path())
arg_input = click.argument("input_", metavar="INPUT", type=click.STRING)
arg_output = click.argument("output", type=click.STRING)
arg_src_tiledir = click.argument("src_tiledir", type=click.STRING)
arg_dst_tiledir = click.argument("dst_tiledir", type=click.STRING)
arg_tiledir = click.argument("tiledir", type=click.STRING)
arg_path = click.argument("path", type=click.Path(path_type=MPath))
arg_out_path = click.argument("out_path", type=click.Path(path_type=MPath))


# click options #
#################
opt_out_path = click.option(
    "--out-path",
    type=click.Path(),
    default=MPath.from_inp(os.getcwd()) / "output",
    help="Output path.",
)
opt_idx_out_dir = click.option(
    "--idx-out-dir", type=click.Path(), help="Index output directory."
)
opt_input_file = click.option(
    "--input-file",
    "-i",
    type=click.Path(),
    help=(
        """Specify an input file via command line (in mapchete file, """
        """set 'input_file' parameter to 'from_command_line')."""
    ),
)
opt_zoom = click.option(
    "--zoom",
    "-z",
    callback=_validate_zoom,
    help="Single zoom level or min and max separated by ','.",
)
opt_bounds = click.option(
    "--bounds",
    "-b",
    type=click.FLOAT,
    nargs=4,
    callback=_validate_bounds,
    help="Left, bottom, right, top bounds in tile pyramid CRS.",
)
opt_bounds_crs = click.option(
    "--bounds-crs",
    callback=_validate_crs,
    help="CRS of --bounds. (default: process CRS)",
)
opt_area = click.option(
    "--area",
    "-a",
    type=click.STRING,
    help="Process area as either WKT string or path to vector file.",
)
opt_area_crs = click.option(
    "--area-crs",
    callback=_validate_crs,
    help="CRS of --area (does not override CRS of vector file). (default: process CRS)",
)
opt_point = click.option(
    "--point",
    "-p",
    type=click.FLOAT,
    nargs=2,
    help="Process tiles over single point location.",
)
opt_point_crs = click.option(
    "--point-crs", callback=_validate_crs, help="CRS of --point. (default: process CRS)"
)
opt_wkt_geometry = click.option(
    "--wkt-geometry",
    "-g",
    type=click.STRING,
    help="Take boundaries from WKT geometry in tile pyramid CRS.",
)
opt_tile = click.option(
    "--tile", "-t", type=click.INT, nargs=3, help="Zoom, row, column of single tile."
)
opt_overwrite = click.option(
    "--overwrite", "-o", is_flag=True, help="Overwrite if output already exist(s)."
)
opt_workers = click.option(
    "--workers",
    "-w",
    type=click.INT,
    help="Number of workers when processing concurrently.",
)
opt_force = click.option(
    "--force", "-f", is_flag=True, help="Overwrite if files already exist."
)
opt_logfile = click.option(
    "--logfile",
    "-l",
    type=click.Path(),
    callback=_setup_logfile,
    help="Write debug log infos into file.",
)
opt_verbose = click.option(
    "--verbose", "-v", is_flag=True, help="Print info for each process tile."
)
opt_no_pbar = click.option("--no-pbar", is_flag=True, help="Deactivate progress bar.")
opt_debug = click.option(
    "--debug",
    "-d",
    is_flag=True,
    callback=_set_debug_log_level,
    help="Deactivate progress bar and print debug log output.",
)
opt_multiprocessing_start_method = click.option(
    "--multiprocessing-start-method",
    type=click.Choice(MULTIPROCESSING_START_METHODS),
    default=MULTIPROCESSING_DEFAULT_START_METHOD,
    help=(
        "Method used by multiprocessing module to start child workers. Availability of "
        f"methods depends on OS (default: {MULTIPROCESSING_DEFAULT_START_METHOD})"
    ),
)
opt_input_formats = click.option(
    "--input-formats", "-i", is_flag=True, help="Show only input formats."
)
opt_output_formats = click.option(
    "--output-formats", "-o", is_flag=True, help="Show only output formats."
)
opt_geojson = click.option("--geojson", is_flag=True, help="Write GeoJSON index.")
opt_gpkg = click.option("--gpkg", is_flag=True, help="Write GeoPackage index.")
opt_shp = click.option("--shp", is_flag=True, help="Write Shapefile index.")
opt_fgb = click.option("--fgb", is_flag=True, help="Write FlatGeobuf index.")
opt_vrt = click.option("--vrt", is_flag=True, help="Write VRT file.")
opt_txt = click.option(
    "--txt", is_flag=True, help="Write output tile paths to text file."
)
opt_fieldname = click.option(
    "--fieldname",
    type=click.STRING,
    default="location",
    help="Field to store tile paths in.",
)
opt_basepath = click.option(
    "--basepath",
    type=click.STRING,
    help="Use other base path than given process output path.",
)
opt_for_gdal = click.option(
    "--for-gdal",
    is_flag=True,
    help="Make remote paths readable by GDAL (not applied for txt output).",
)
opt_output_format = click.option(
    "--output-format",
    "-of",
    type=click.Choice(["GTiff", "PNG"]),
    default="GTiff",
    help="Output data format (GTiff or PNG).",
)
opt_pyramid_type = click.option(
    "--pyramid-type",
    "-pt",
    type=click.Choice(tilematrix._conf.PYRAMID_PARAMS.keys()),
    default="geodetic",
    help="Output pyramid type. (default: geodetic)",
)
opt_resampling_method = click.option(
    "--resampling-method",
    "-r",
    type=click.Choice([it.name for it in Resampling if it.value in range(8)]),
    default="nearest",
    help=("Resampling method used. (default: nearest)"),
)
opt_port = click.option(
    "--port",
    "-p",
    type=click.INT,
    default=5000,
    help="Port process is hosted on. (default: 5000)",
)
opt_internal_cache = click.option(
    "--internal-cache",
    "-c",
    type=click.INT,
    default=1024,
    help="Number of web tiles to be cached in RAM. (default: 1024)",
)
opt_readonly = click.option(
    "--readonly", "-ro", is_flag=True, help="Just read process output without writing."
)
opt_memory = click.option(
    "--memory",
    "-mo",
    is_flag=True,
    help="Always get output from freshly processed output.",
)
opt_http_username = click.option(
    "--username", "-u", type=click.STRING, help="Username for HTTP Auth."
)
opt_http_password = click.option(
    "--password", type=click.STRING, help="Password for HTTP Auth."
)
opt_force = click.option("-f", "--force", is_flag=True, help="Don't ask, just do.")
opt_src_fs_opts = click.option(
    "--src-fs-opts",
    metavar="NAME=VALUE",
    multiple=True,
    callback=_cb_key_val,
    help="Configuration options for source fsspec filesystem.",
)
opt_dst_fs_opts = click.option(
    "--dst-fs-opts",
    metavar="NAME=VALUE",
    multiple=True,
    callback=_cb_key_val,
    help="Configuration options for destination fsspec filesystem.",
)
opt_fs_opts = click.option(
    "--fs-opts",
    metavar="NAME=VALUE",
    multiple=True,
    callback=_cb_key_val,
    help="Configuration options for destination fsspec filesystem.",
)
opt_dask_scheduler = click.option(
    "--dask-scheduler", type=click.STRING, help="Address for dask scheduler."
)
opt_concurrency = click.option(
    "--concurrency",
    type=click.Choice(["processes", "threads", "dask", "none"]),
    default="processes",
    callback=_cb_none_concurrency,
    help="Decide which Executor to use for concurrent processing.",
)
opt_dask_no_task_graph = click.option(
    "--dask-no-task-graph",
    is_flag=True,
    help="Don't compute task graph when using dask.",
)
opt_profiling = click.option(
    "--profiling",
    is_flag=True,
    help="Add profiling information to executed tasks.",
)
