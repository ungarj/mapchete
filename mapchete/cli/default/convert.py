import click
import logging
import os
import rasterio
from rasterio.dtypes import dtype_ranges
import sys
import tilematrix

from mapchete.cli import utils
from mapchete.config import raw_conf, raw_conf_output_pyramid, get_zoom_levels
from mapchete.formats import (
    driver_from_file, available_output_formats, available_input_formats
)
from mapchete.io import read_json, get_best_zoom_level
from mapchete.tile import BufferedTilePyramid

logger = logging.getLogger(__name__)
OUTPUT_FORMATS = available_output_formats()


@click.command(help="Convert outputs or other geodata.")
@utils.arg_input
@utils.arg_output
@utils.opt_zoom
@utils.opt_bounds
@utils.opt_point
@utils.opt_wkt_geometry
@click.option(
    "--clip_geometry", "-c", type=click.Path(exists=True),
    help="Clip output by geometry"
)
@click.option(
    "--output_pyramid", "-op", type=click.Choice(tilematrix._conf.PYRAMID_PARAMS.keys()),
    help="Output pyramid to write to."
)
@click.option(
    "--output_metatiling", "-m", type=click.INT, default=1,
    help="Output metatiling.",
)
@click.option(
    "--output_format", type=click.Choice(available_output_formats()),
    help="Output format."
)
@click.option(
    "--output_dtype", type=click.Choice(dtype_ranges.keys()),
    help="Output data type (for raster output only)."
)
@click.option(
    "--scale-ratio", type=click.FLOAT, default=1.,
    help="Scaling factor (for raster output only)."
)
@click.option(
    "--scale-offset", type=click.FLOAT, default=0.,
    help="Scaling offset (for raster output only)."
)
@utils.opt_overwrite
@utils.opt_verbose
@utils.opt_no_pbar
@utils.opt_debug
@utils.opt_logfile
def convert(
    input_,
    output,
    zoom=None,
    bounds=None,
    point=None,
    wkt_geometry=None,
    clip_geometry=None,
    output_pyramid=None,
    output_metatiling=None,
    output_format=None,
    output_dtype=None,
    scale_ratio=None,
    scale_offset=None,
    overwrite=False,
    logfile=None,
    verbose=False,
    no_pbar=False,
    debug=False,
):
    input_info = _get_input_info(input_)
    output_info = _get_output_info(output)

    # collect mapchete configuration
    mapchete_config = dict(
        process="mapchete.processes.convert",
        input=dict(raster=input_, clip=clip_geometry),
        pyramid=(
            dict(grid=output_pyramid, metatiling=output_metatiling)
            if output_pyramid
            else input_info["pyramid"]
        ),
        output=dict(
            input_info["output_params"],
            path=output,
            format=(
                output_format or
                output_info["driver"] or
                input_info["output_params"]["format"]
            )
        ),
        config_dir=os.getcwd(),
        zoom_levels=zoom or input_info["zoom_levels"]
    )

    if mapchete_config["output"]["format"] is None:
        raise click.BadOptionUsage("output_format", "Output format required.")
    output_type = OUTPUT_FORMATS[mapchete_config["output"]["format"]]["data_type"]

    if mapchete_config["pyramid"] is None:
        raise click.BadOptionUsage("output_pyramid", "Output pyramid required.")
    elif mapchete_config["zoom_levels"] is None:
        try:
            mapchete_config.update(
                zoom=list(range(
                    0,
                    get_best_zoom_level(input_, mapchete_config["pyramid"]["grid"]) + 1
                ))
            )
        except:
            raise click.BadOptionUsage("zoom", "Zoom levels required.")
    elif input_info["input_type"] != output_type:
        raise click.BadArgumentUsage(
            "output",
            "Output format type (%s) is incompatible with input format (%s)." % (
                output_type, input_info["input_type"]
            )
        )

    utils._process_area(
        debug=debug,
        mapchete_config=mapchete_config,
        mode="overwrite" if overwrite else "continue",
        zoom=zoom,
        wkt_geometry=wkt_geometry,
        point=point,
        bounds=bounds,
        verbose_dst=open(os.devnull, 'w') if debug or not verbose else sys.stdout,
        no_pbar=no_pbar,
    )


def _get_input_info(input_):
    if os.path.isfile(input_):
        # single file input can be a mapchete file or a rasterio/fiona file
        driver = driver_from_file(input_)

        if driver == "Mapchete":
            logger.debug("input is mapchete file")
            input_info = _input_mapchete_info(input_)

        elif driver == "raster_file":
            # this should be readable by rasterio
            logger.debug("input is raster_file")
            input_info = _input_rasterio_info(input_)

        elif driver == "vector_file":
            # this should be readable by fiona
            logger.debug("input is vector_file")
            raise NotImplementedError()

    else:
        # assuming tile directory
        logger.debug("input is tile directory")
        input_info = _input_tile_directory_info(input_)

    return input_info


def _input_mapchete_info(input_):
    conf = raw_conf(input_)
    output_params = conf["output"]
    pyramid = raw_conf_output_pyramid(conf)
    return dict(
        output_params=output_params,
        pyramid=pyramid.to_dict(),
        crs=pyramid.crs,
        zoom_levels=get_zoom_levels(process_zoom_levels=conf["zoom_levels"]),
        pixel_size=None,
        input_type=OUTPUT_FORMATS[output_params["format"]]["data_type"]
    )


def _input_rasterio_info(input_):
    with rasterio.open(input_) as src:
        return dict(
            output_params=dict(
                bands=src.meta["count"],
                dtype=src.meta["dtype"],
                format=src.driver if src.driver in available_input_formats() else None
            ),
            pyramid=None,
            crs=src.crs,
            zoom_levels=None,
            pixel_size=src.transform[0],
            input_type="raster"
        )


def _input_tile_directory_info(input_):
    conf = read_json(os.path.join(input_, "metadata.json"))
    pyramid = BufferedTilePyramid.from_dict(conf["pyramid"])
    return dict(
        output_params=conf["driver"],
        pyramid=pyramid.to_dict(),
        crs=pyramid.crs,
        zoom_levels=None,
        pixel_size=None,
        input_type=OUTPUT_FORMATS[conf["driver"]["format"]]["data_type"]
    )


def _get_output_info(output):
    _, file_ext = os.path.splitext(output)
    if not file_ext:
        return dict(
            type="TileDirectory",
            driver=None
        )
    elif file_ext == ".tif":
        return dict(
            type="SingleFile",
            driver="GTiff"
        )
    elif file_ext == ".png":
        return dict(
            type="SingleFile",
            driver="PNG"
        )
    else:
        raise TypeError("Output file extension not recognized: %s", file_ext)
