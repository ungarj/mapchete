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

from mapchete import *
from tilematrix import Tile, TilePyramid, MetaTilePyramid

logger = logging.getLogger("mapchete")

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("mapchete_file", type=str)
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument("--bounds", "-b", type=float, nargs='*')
    parser.add_argument("--tile", "-t", type=int, nargs=3, )
    parser.add_argument("--log", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parsed = parser.parse_args(args)

    try:
        logger.info("preparing process ...")
        mapchete = Mapchete(
            MapcheteConfig(
                parsed.mapchete_file,
                zoom=parsed.zoom,
                bounds=parsed.bounds
            )
        )
    except PyCompileError as e:
        print e
        return
    except:
        raise

    overwrite = parsed.overwrite
    f = partial(worker,
        mapchete=mapchete,
        overwrite=overwrite
    )

    logging.config.dictConfig(get_log_config(mapchete))
    logger.info("starting process ...")

    if parsed.tile:
        try:
            pool = Pool()
            output = pool.imap_unordered(
                f,
                [mapchete.tile(
                    Tile(
                        mapchete.tile_pyramid,
                        *tuple(parsed.tile)
                    )
                )]
            )
        except KeyboardInterrupt:
            logger.info("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
        except:
            raise
        finally:
            pool.close()
            pool.join()
    else:
        for zoom in reversed(mapchete.config.zoom_levels):
            pool = Pool()
            try:
                for output in pool.imap_unordered(
                    f,
                    mapchete.get_work_tiles(zoom),
                    chunksize=8
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

    if mapchete.config.output_format in [
        "GTiff",
        "PNG",
        "PNG_hillshade"
        ] and not parsed.tile:
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


def worker(tile, mapchete, overwrite):
    """
    Worker function running the process depending on the overwrite flag and
    whether the tile exists.
    """
    try:
        log_message = mapchete.execute(tile, overwrite=overwrite)
    except Exception as e:
        log_message = mapchete.process_name, (
            tile.id,
            "failed",
            traceback.print_exc()
        )

    logger.info((mapchete.process_name, log_message))
    return log_message


if __name__ == "__main__":
    main()
