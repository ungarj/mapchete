#!/usr/bin/env python

import os
import sys
import argparse
import imp
import yaml
from functools import partial
from multiprocessing import Pool, cpu_count
from progressbar import ProgressBar

from mapchete import *
from tilematrix import TilePyramid, MetaTilePyramid

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("mapchete_file", type=str)
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument("--bounds", "-b", type=float, nargs='*')
    parsed = parser.parse_args(args)

    try:
        config = get_clean_configuration(
            parsed.mapchete_file,
            zoom=parsed.zoom,
            bounds=parsed.bounds
            )
        base_tile_pyramid = TilePyramid(str(config["output_srs"]))
        tile_pyramid = MetaTilePyramid(base_tile_pyramid, config["metatiling"])
    except Exception as e:
        #sys.exit(e)
        raise

    # Determine tiles to be processed, depending on:
    # - zoom level and
    # - input files bounds OR user defined bounds
    work_tiles = []
    for zoom in config["zoom_levels"]:
        bbox = config["process_area"][zoom]
        if not bbox.is_empty:
            work_tiles.extend(tile_pyramid.tiles_from_geom(bbox, zoom))

    print len(work_tiles), "tiles to be processed"

    process_name = os.path.splitext(os.path.basename(config["process_file"]))[0]

    # Load source process from python file and initialize.
    new_process = imp.load_source(
        process_name + "Process",
        config["process_file"]
        )
    user_defined_process = new_process.Process(parsed.mapchete_file)

    print "processing", user_defined_process.identifier

    f = partial(worker,
        mapchete_process=user_defined_process,
        tile_pyramid=tile_pyramid,
        params=config
    )

    pool = Pool(cpu_count())
    try:
        counter = 0
        pbar = ProgressBar(maxval=len(work_tiles)).start()
        for output in pool.imap_unordered(f, work_tiles):
            counter += 1
            pbar.update(counter)
        pbar.finish()
    except:
        raise
    finally:
        pool.close()
        pool.join()

def worker(tile, mapchete_process, tile_pyramid, params):

    try:
        return mapchete_process.execute(tile, tile_pyramid, params)
    except:
        raise



if __name__ == "__main__":
    main(sys.argv[1:])
