#!/usr/bin/env python

import os
import sys
import argparse
from functools import partial
from multiprocessing import Pool, cpu_count
import time
import logging
import logging.config
import traceback

from mapchete import *
from tilematrix import TilePyramid, MetaTilePyramid

logger = logging.getLogger("mapchete")

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("mapchete_file", type=str)
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument("--bounds", "-b", type=float, nargs='*')
    parser.add_argument("--log", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parsed = parser.parse_args(args)

    log_dir = os.path.dirname(parsed.mapchete_file)
    log_file = os.path.join(log_dir, "mapchete.log")

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'simple': {
                'format': '%(levelname)s: %(message)s'
            },
            'verbose': {
                'format': '[%(asctime)s][%(module)s] %(levelname)s: %(message)s'
            }
        },
        'handlers': {
            'file': {
                'level': 'DEBUG',
                'class': 'logging.handlers.WatchedFileHandler',
                'filename': log_file,
                'formatter': 'verbose',
                'filters': [],
            },
            'stream': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'simple',
                'filters': [],
            }
        },
        'loggers': {
            'mapchete': {
                'handlers': ['file', 'stream'],
                'level': 'DEBUG',
                'propagate': True
            }
        }
    })

    try:
        logger.info("preparing process ...")
        process_host = MapcheteHost(
            parsed.mapchete_file,
            zoom=parsed.zoom,
            bounds=parsed.bounds
            )
    except Exception as e:
        raise

    work_tiles = process_host.get_work_tiles()

    logger.info("%d tiles to be processed" % len(work_tiles))

    overwrite = parsed.overwrite

    f = partial(worker,
        process_host=process_host,
        overwrite=overwrite
    )
    pool = Pool()
    logs = []
    try:
        output = pool.map_async(f, work_tiles, callback=logs.extend)
        pool.close()
    except KeyboardInterrupt:
        pool.terminate()
        sys.exit()
    except:
        raise
    finally:
        pool.close()
        pool.join()

    if process_host.config["output_format"] in [
        "GTiff",
        "PNG",
        "PNG_hillshade"
        ]:
        for zoom in process_host.config["zoom_levels"]:
            out_dir = os.path.join(
                process_host.config["output_name"],
                str(zoom)
            )
            out_vrt = os.path.join(
                process_host.config["output_name"],
                (str(zoom)+".vrt")
            )
            command = "gdalbuildvrt -overwrite %s %s" %(
                out_vrt,
                str(out_dir + "/*/*" + process_host.format.extension)
            )
            os.system(command)

    if parsed.log:
        for row in logs:
            if row[1] not in ["ok", "exists"]:
                print row


def worker(tile, process_host, overwrite):
    """
    Worker function running the process depending on the overwrite flag and
    whether the tile exists.
    """
    if not overwrite:
        image_path = process_host.tile_pyramid.format.get_tile_name(
            process_host.config["output_name"],
            tile
        )
        if os.path.isfile(image_path):
            log_message = (tile, "exists", None)
            logger.info(log_message)
            return tile, "exists", None
    try:
        log_message = process_host.save_tile(tile, overwrite)
    except Exception as e:
        log_message = (tile, "failed", traceback.print_exc())

    logger.info(log_message)
    return log_message


if __name__ == "__main__":
    main(sys.argv[1:])
