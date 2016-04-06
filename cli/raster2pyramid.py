#!/usr/bin/env python

import os
import sys
import argparse
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
import logging
import logging.config
import traceback
from py_compile import PyCompileError
import rasterio
import numpy as np

from mapchete import *
from tilematrix import TilePyramid, MetaTilePyramid, get_best_zoom_level

logger = logging.getLogger("mapchete")

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_raster",
        type=str,
        help="input raster file"
    )
    parser.add_argument(
        "pyramid_type",
        type=str,
        choices=["geodetic", "mercator"],
        help="pyramid schema to be used"
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="output directory where tiles are stored"
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

    raster2pyramid(
        parsed.input_raster,
        parsed.output_dir,
        parsed.pyramid_type,
        output_format=parsed.output_format,
        resampling=parsed.resampling_method,
        zoom=parsed.zoom,
        bounds=parsed.bounds,
        overwrite=parsed.overwrite
    )


def raster2pyramid(
    input_file,
    output_dir,
    output_type,
    output_format="GTiff",
    resampling="nearest",
    zoom=None,
    bounds=None,
    overwrite=False
    ):
    """
    Creates a tile pyramid out of an input raster dataset.
    """

    # Prepare output directory and logging
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    logging.config.dictConfig(get_log_config(output_dir))

    # Prepare process parameters
    minzoom, maxzoom = _get_zoom(zoom, input_file, output_type)
    process_file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "tilify.py"
    )
    with rasterio.open(input_file, "r") as input_raster:
        output_bands = input_raster.count
        output_dtype = input_raster.dtypes[0]
        if output_format == "PNG":
            if output_bands > 3:
                output_bands = 3
            output_dtype = 'uint8'
        nodataval = input_raster.nodatavals[0]
        if not nodataval:
            nodataval = 0

    # Create configuration
    config = {}
    config.update(
        process_file=process_file,
        output_name=output_dir,
        output_type=output_type,
        output_format=output_format,
        input_files={"raster": input_file},
        config_dir=os.getcwd(),
        output_bands=output_bands,
        output_dtype=output_dtype,
        process_minzoom=minzoom,
        process_maxzoom=maxzoom,
        nodataval=nodataval,
        resampling=resampling,
        bounds=bounds,
        pixelbuffer=5,
        baselevel={"zoom": maxzoom, "resampling": resampling}
    )

    for zoom in reversed(range(minzoom, maxzoom+1)):
        try:
            mapchete = Mapchete(
                MapcheteConfig(
                    config,
                    zoom=zoom,
                    bounds=bounds
                )
            )
        except PyCompileError as e:
            print e
            return
        except:
            raise

        # Determine work tiles and run
        work_tiles = mapchete.get_work_tiles()

        f = partial(_worker,
            mapchete=mapchete,
            overwrite=overwrite
        )
        pool = Pool()
        try:
            output = pool.map_async(f, work_tiles)
            pool.close()
        except KeyboardInterrupt:
            pool.close()
            pool.terminate()
            sys.exit()
        except:
            pool.close()
            raise
        finally:
            pool.join()
            pool.close()


def _worker(tile, mapchete, overwrite):
    """
    Worker function running the process depending on the overwrite flag and
    whether the tile exists.
    """
    try:
        log_message = mapchete.execute(tile, overwrite=overwrite)
    except Exception as e:
        log_message = (tile.id, "failed", traceback.print_exc())

    logger.info(log_message)
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
