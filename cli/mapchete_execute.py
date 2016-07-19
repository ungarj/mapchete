#!/usr/bin/env python

import os
import sys
import argparse
from functools import partial
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
import time
import logging
import logging.config
import traceback
from py_compile import PyCompileError
import re
from datetime import datetime

from mapchete import Mapchete, MapcheteConfig
from tilematrix import Tile

logger = logging.getLogger("mapchete")

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("mapchete_file", type=str)
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument("--bounds", "-b", type=float, nargs='*')
    parser.add_argument("--tile", "-t", type=int, nargs=3, )
    parser.add_argument("--failed_from_log", type=str)
    parser.add_argument("--failed_since", type=str)
    parser.add_argument("--log", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--multi", "-m", type=int)
    parser.add_argument("--create_vrt", action="store_true")
    parser.add_argument("--input_file", type=str)

    parsed = parser.parse_args(args)

    input_file = parsed.input_file

    if input_file and not os.path.isfile(input_file):
        raise IOError("input_file not found")

    overwrite = parsed.overwrite
    multi = parsed.multi

    if not multi:
        multi = cpu_count()
    try:
        logger.info("preparing process ...")
        mapchete = Mapchete(
            MapcheteConfig(
                parsed.mapchete_file,
                zoom=parsed.zoom,
                bounds=parsed.bounds,
                overwrite=overwrite,
                input_file=parsed.input_file
            ),
        )
    except PyCompileError as e:
        print e
        return
    except:
        raise

    f = partial(worker,
        mapchete=mapchete,
        overwrite=overwrite
    )

    logging.config.dictConfig(get_log_config(mapchete))
    logger.info("starting process using %s worker(s)" %(multi))

    work_tiles = []
    if parsed.tile:
        work_tiles = [mapchete.tile(
            Tile(
                mapchete.tile_pyramid,
                *tuple(parsed.tile)
            )
        )]
        mapchete.config.zoom_levels = [parsed.tile[0]]
    elif parsed.failed_from_log:
        work_tiles = read_failed_from_log(
            parsed.failed_from_log,
            mapchete,
            failed_since_str=parsed.failed_since
        )

    for zoom in reversed(mapchete.config.zoom_levels):
        if not work_tiles:
            work_tiles = mapchete.get_work_tiles(zoom)
        pool = Pool(multi)
        try:
            for output in pool.imap_unordered(
                f,
                work_tiles,
                chunksize=1
                ):
                pass
        except KeyboardInterrupt:
            logger.info("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
            break
        except:
            raise
        finally:
            pool.close()
            pool.join()
        work_tiles = []

    if mapchete.output.format in [
        "GTiff",
        "PNG",
        "PNG_hillshade"
        ] and not parsed.tile and parsed.create_vrt:
        for zoom in mapchete.config.zoom_levels:
            out_dir = os.path.join(
                mapchete.config.output_name,
                str(zoom)
            )
            out_vrt = os.path.join(
                mapchete.config.output_name,
                (str(zoom)+".vrt")
            )
            command = "gdalbuildvrt -overwrite %s %s" %(
                out_vrt,
                str(out_dir + "/*/*" + mapchete.format.extension)
            )
            os.system(command)


def read_failed_from_log(logfile, mapchete, failed_since_str='1980-01-01'):
    """
    Reads logfile line by line and returns tile indexes filtered by timestamp
    and failed tiles.
    """
    if not os.path.isfile(logfile):
        raise IOError("input log file not found")
    try:
        failed_since = datetime.strptime(failed_since_str, '%Y-%m-%d')
    except:
        raise ValueError("bad timestamp given")

    with open(logfile) as logs:
        for line in logs.readlines():
            if "failed" in line:
                t = re.search(
                    '\[.*[0-9]\]',
                    line
                ).group(0).replace('[', '').replace(']', '')
                timestamp = datetime.strptime(t, '%Y-%m-%d %H:%M:%S,%f')
                if timestamp > failed_since:
                    tile = map(
                        int,
                        re.search(
                            '\([0-9].*[0-9]\),',
                            line
                        ).group(0).replace('(', '').replace('),', '').split(', ')
                    )
                    yield mapchete.tile(
                        Tile(
                            mapchete.tile_pyramid,
                            *tuple(tile)
                        )
                    )


def worker(tile, mapchete, overwrite):
    """
    Worker function running the process depending on the overwrite flag and
    whether the tile exists.
    """
    starttime = time.time()
    try:
        log_message = mapchete.execute(tile, overwrite=overwrite)
    except Exception as e:
        log_message = mapchete.process_name, (
            tile.id,
            "failed",
            traceback.print_exc()
        )
    endtime = time.time()
    elapsed = "%ss" %(round((endtime - starttime), 3))

    logger.info((mapchete.process_name, log_message, elapsed))
    return log_message


if __name__ == "__main__":
    main()
