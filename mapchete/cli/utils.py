import click
import logging
import os
import tilematrix
import tqdm

from mapchete.formats import available_output_formats
from mapchete.log import set_log_level, setup_logfile


def write_verbose_msg(process_info, dst):
    tqdm.tqdm.write(
        "Tile %s: %s, %s" % (
            tuple(process_info.tile.id), process_info.process_msg, process_info.write_msg
        ),
        file=dst
    )


def _validate_zoom(ctx, param, zoom):
    if zoom:
        try:
            zoom_levels = list(map(int, zoom.split(",")))
        except ValueError:
            raise click.BadParameter("zoom levels must be integer values")
        if len(zoom_levels) > 2:
            raise click.BadParameter(
                "either provide one zoom level or min and max (format: min,max)"
            )
        return zoom_levels


def _validate_bounds(ctx, param, bounds):
    if bounds:
        if (
            not isinstance(bounds, (list, tuple)) or
            len(bounds) != 4 or
            any([not isinstance(i, (int, float)) for i in bounds])
        ):
            raise click.BadParameter("bounds not valid")
        return bounds


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


# arguments
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

# options
opt_out_path = click.option(
    "--out_path", "-op", type=click.Path(), default=os.path.join(os.getcwd(), "output"),
    help="Process output path."
)
opt_idx_out_dir = click.option(
    "--idx_out_dir", "-od", type=click.Path(),
    help="Index output directory."
)
opt_input_file = click.option(
    "--input_file", "-i", type=click.Path(),
    help=(
        """Specify an input file via command line (in mapchete file, """
        """set 'input_file' parameter to 'from_command_line')."""
    ),
)
opt_zoom = click.option(
    "--zoom", "-z", callback=_validate_zoom,
    help="Single zoom level or min and max separated by ','.",
)
opt_bounds = click.option(
    "--bounds", "-b", type=click.FLOAT, nargs=4, callback=_validate_bounds,
    help="Left, bottom, right, top bounds in tile pyramid CRS.",
)
opt_point = click.option(
    "--point", "-p", type=click.FLOAT, nargs=2,
    help="Process tiles over single point location."
)
opt_wkt_geometry = click.option(
    "--wkt_geometry", "-g", type=click.STRING,
    help="Take boundaries from WKT geometry in tile pyramid CRS.",
)
opt_tile = click.option(
    "--tile", "-t", type=click.INT, nargs=3,
    help="Zoom, row, column of single tile."
)
opt_overwrite = click.option(
    "--overwrite", "-o", is_flag=True,
    help="Overwrite if tile(s) already exist(s)."
)
opt_multi = click.option(
    "--multi", "-m", type=click.INT,
    help="Number of concurrent processes.",
)
opt_force = click.option(
    "--force", "-f", is_flag=True,
    help="Overwrite if files already exist."
)
opt_logfile = click.option(
    "--logfile", "-l", type=click.Path(), callback=_setup_logfile,
    help="Write debug log infos into file."
)
opt_verbose = click.option(
    "--verbose", "-v", is_flag=True,
    help="Print info for each process tile."
)
opt_no_pbar = click.option(
    "--no_pbar", is_flag=True,
    help="Deactivate progress bar."
)
opt_debug = click.option(
    "--debug", "-d", is_flag=True, callback=_set_debug_log_level,
    help="Deactivate progress bar and print debug log output."
)
opt_max_chunksize = click.option(
    "--max_chunksize", "-c", type=click.INT, default=1,
    help="Maximum number of process tiles to be queued for each  worker. (default: 1)"
)
opt_input_formats = click.option(
    "--input_formats", "-i", is_flag=True,
    help="Show only input formats."
)
opt_output_formats = click.option(
    "--output_formats", "-o", is_flag=True,
    help="Show only output formats."
)
opt_geojson = click.option(
    "--geojson", is_flag=True,
    help="Write GeoJSON index."
)
opt_gpkg = click.option(
    "--gpkg", is_flag=True,
    help="Write GeoPackage index."
)
opt_shp = click.option(
    "--shp", is_flag=True,
    help="Write Shapefile index."
)
opt_vrt = click.option(
    "--vrt", is_flag=True,
    help="Write VRT file."
)
opt_txt = click.option(
    "--txt", is_flag=True,
    help="Write output tile paths to text file."
)
opt_fieldname = click.option(
    "--fieldname", type=str, default="location",
    help="Field to store tile paths in."
)
opt_basepath = click.option(
    "--basepath", type=str,
    help="Use other base path than given process output path."
)
opt_for_gdal = click.option(
    "--for_gdal", is_flag=True,
    help="Make remote paths readable by GDAL (not applied for txt output)."
)
opt_output_format = click.option(
    "--output_format", "-of", type=click.Choice(["GTiff", "PNG"]), default="GTiff",
    help="Output data format (GTiff or PNG)."
)
opt_pyramid_type = click.option(
    "--pyramid_type", "-pt", type=click.Choice(tilematrix._conf.PYRAMID_PARAMS.keys()),
    default="geodetic",
    help="Output pyramid type. (default: geodetic)"
)
opt_pyramid_type_mercator = click.option(
    "--pyramid_type", "-pt", type=click.Choice(tilematrix._conf.PYRAMID_PARAMS.keys()),
    default="mercator",
    help="Output pyramid type. (default: mercator)"
)
opt_resampling_method = click.option(
    "--resampling_method", "-r", type=click.Choice([
        "nearest", "bilinear", "cubic", "cubic_spline", "lanczos", "average", "mode"
    ]), default="nearest",
    help=(
        """Resampling method to be used (nearest, bilinear, cubic, cubic_spline, """
        """lanczos, average or mode)."""
    ),
)
opt_scale_method = click.option(
    "--scale_method", "-s", type=click.Choice(
        ["dtype_scale", "minmax_scale", "crop", ""]
    ), default="",
    help=(
        """Scale method if input bands have more than 8 bit (dtype_scale, """
        """minmax_scale or crop)."""
    ),
)
opt_port = click.option(
    "--port", "-p", type=click.INT, default=5000,
    help="Port process is hosted on. (default: 5000)",
)
opt_internal_cache = click.option(
    "--internal_cache", "-c", type=click.INT, default=1024,
    help="Number of web tiles to be cached in RAM. (default: 1024)",
)
opt_readonly = click.option(
    "--readonly", "-ro", is_flag=True,
    help="Just read process output without writing."
)
opt_memory = click.option(
    "--memory", "-mo", is_flag=True,
    help="Always get output from freshly processed output."
)
