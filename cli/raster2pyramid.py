#!/usr/bin/env python
"""
Utility to create tile pyramids out of input rasters. Also provides various
options to rescale data if necessary
"""

import os
import sys
import argparse
from functools import partial
from multiprocessing.pool import Pool
import logging
import logging.config
import traceback
from py_compile import PyCompileError
import rasterio

from mapchete import Mapchete, MapcheteConfig, get_log_config
from mapchete.io_utils import get_best_zoom_level

LOGGER = logging.getLogger("mapchete")
# ranges from rasterio
# https://github.com/mapbox/rasterio/blob/master/rasterio/dtypes.py#L61
DTYPE_RANGES = {
    'uint8': (0, 255),
    'uint16': (0, 65535),
    'int16': (-32768, 32767),
    'uint32': (0, 4294967295),
    'int32': (-2147483648, 2147483647),
    'float32': (-3.4028235e+38, 3.4028235e+38),
    'float64': (-1.7976931348623157e+308, 1.7976931348623157e+308)
}

def main(args=None):
    """
    Main entry point to tool.
    """

    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_raster",
        type=str,
        help="input raster file"
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="output directory where tiles are stored"
    )
    parser.add_argument(
        "--pyramid_type",
        type=str,
        default="mercator",
        choices=["geodetic", "mercator"],
        help="pyramid schema to be used"
    )
    parser.add_argument(
        "--output_format",
        type=str,
        default="GTiff",
        choices=["GTiff", "PNG"],
        help="output data type"
    )
    parser.add_argument(
        "--resampling_method",
        type=str,
        default="nearest",
        choices=[
            "nearest",
            "bilinear",
            "cubic",
            "cubic_spline",
            "lanczos",
            "average",
            "mode"
        ]
    )
    parser.add_argument(
        "--scale_method",
        type=str,
        default="minmax_scale",
        choices=["dtype_scale", "minmax_scale", "crop"],
        help="scale method if input bands have more than 8 bit"
    )
    parser.add_argument(
        "--zoom",
        "-z",
        type=int,
        nargs='*'
    )
    parser.add_argument(
        "--bounds",
        "-b",
        type=float,
        nargs='*',
        help='left bottom right top in pyramid CRS (i.e. either EPSG:4326 for \
        geodetic or EPSG:3857 for mercator)'
    )
    parser.add_argument("--log", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parsed = parser.parse_args(args)

    options = {}
    options.update(
        pyramid_type=parsed.pyramid_type,
        scale_method=parsed.scale_method,
        output_format=parsed.output_format,
        resampling=parsed.resampling_method,
        zoom=parsed.zoom,
        bounds=parsed.bounds,
        overwrite=parsed.overwrite
    )
    raster2pyramid(
        parsed.input_raster,
        parsed.output_dir,
        options
    )


def raster2pyramid(
    input_file,
    output_dir,
    options
    ):
    """
    Creates a tile pyramid out of an input raster dataset.
    """
    pyramid_type = options["pyramid_type"]
    scale_method = options["scale_method"]
    output_format = options["output_format"]
    resampling = options["resampling"]
    zoom = options["zoom"]
    bounds = options["bounds"]
    overwrite = options["overwrite"]

    # Prepare process parameters
    minzoom, maxzoom = _get_zoom(zoom, input_file, pyramid_type)
    process_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "tilify.py"
    )

    with rasterio.open(input_file, "r") as input_raster:
        output_bands = input_raster.count
        input_dtype = input_raster.dtypes[0]
        output_dtype = input_raster.dtypes[0]
        nodataval = input_raster.nodatavals[0]
        if not nodataval:
            nodataval = 0
        if output_format == "PNG":
            if output_bands > 3:
                output_bands = 3
                output_dtype = 'uint8'
        scales_minmax = ()
        if scale_method == "dtype_scale":
            for index in range(1, output_bands+1):
                scales_minmax += (DTYPE_RANGES[input_dtype], )
        elif scale_method == "minmax_scale":
            for index in range(1, output_bands+1):
                band = input_raster.read(index)
                scales_minmax += ((band.min(), band.max()), )
        elif scale_method == "crop":
            for index in range(1, output_bands+1):
                scales_minmax += ((0, 255), )
        if input_dtype == "uint8":
            scale_method = None
            scales_minmax = ()
            for index in range(1, output_bands+1):
                scales_minmax += ((None, None), )

    # Create configuration
    config = {}
    config.update(
        process_file=process_file,
        output={
            "path": output_dir,
            "format": output_format,
            "type": pyramid_type,
            "bands": output_bands,
            "dtype": output_dtype
            },
        scale_method=scale_method,
        scales_minmax=scales_minmax,
        input_files={"raster": input_file},
        config_dir=os.getcwd(),
        process_minzoom=minzoom,
        process_maxzoom=maxzoom,
        nodataval=nodataval,
        resampling=resampling,
        bounds=bounds,
        pixelbuffer=5,
        baselevel={"zoom": maxzoom, "resampling": resampling}
    )

    LOGGER.info("preparing process ...")

    try:
        mapchete = Mapchete(
            MapcheteConfig(
                config,
                zoom=zoom,
                bounds=bounds
            )
        )
    except PyCompileError as error:
        print error
        return
    except:
        raise

    # Prepare output directory and logging
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    logging.config.dictConfig(get_log_config(mapchete))

    for zoom in reversed(range(minzoom, maxzoom+1)):
        # Determine work tiles and run
        work_tiles = mapchete.get_work_tiles(zoom)
        func = partial(_worker,
            mapchete=mapchete,
            overwrite=overwrite
        )
        pool = Pool()
        try:
            pool.map_async(func, work_tiles)
            pool.close()
        except KeyboardInterrupt:
            LOGGER.info(
                "Caught KeyboardInterrupt, terminating workers"
                )
            pool.terminate()
            break
        except:
            raise
        finally:
            pool.close()
            pool.join()


def _worker(tile, mapchete, overwrite):
    """
    Worker function running the process depending on the overwrite flag and
    whether the tile exists.
    """
    try:
        log_message = mapchete.execute(tile, overwrite=overwrite)
    except Exception:
        log_message = (tile.id, "failed", traceback.print_exc())
    LOGGER.info(log_message)
    return log_message


def _get_zoom(zoom, input_raster, pyramid_type):
    """
    Determines minimum and maximum zoomlevel.
    """
    if not zoom:
        minzoom = 1
        maxzoom = get_best_zoom_level(input_raster, pyramid_type)
    elif len(zoom) == 1:
        minzoom = zoom[0]
        maxzoom = zoom[0]
    elif len(zoom) == 2:
        if zoom[0] < zoom[1]:
            minzoom = zoom[0]
            maxzoom = zoom[1]
        else:
            minzoom = zoom[1]
            maxzoom = zoom[0]
    else:
        raise ValueError("invalid number of zoom levels provided")
    return minzoom, maxzoom

if __name__ == "__main__":
    main()
