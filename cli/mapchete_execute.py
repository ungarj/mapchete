#!/usr/bin/env python

import os
import sys
import argparse
import imp
import yaml
from functools import partial
from multiprocessing import Pool, cpu_count
from progressbar import ProgressBar
import traceback

from mapchete import *
from tilematrix import TilePyramid, MetaTilePyramid
from tilematrix import *

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("mapchete_file", type=str)
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument("--bounds", "-b", type=float, nargs='*')
    parser.add_argument("--log", action="store_true")
    parsed = parser.parse_args(args)

    try:
        print "preparing process ..."
        config = get_clean_configuration(
            parsed.mapchete_file,
            zoom=parsed.zoom,
            bounds=parsed.bounds
            )
        base_tile_pyramid = TilePyramid(str(config["output_srs"]))
        base_tile_pyramid.set_format(config["output_format"])
        tile_pyramid = MetaTilePyramid(base_tile_pyramid, config["metatiling"])
    except Exception as e:
        #sys.exit(e)
        raise

    # Determine tiles to be processed, depending on:
    # - zoom level and
    # - input files bounds OR user defined bounds
    work_tiles = []
    for zoom in config["zoom_levels"]:
        bbox = config["zoom_levels"][zoom]["process_area"]
        work_tiles.extend(tile_pyramid.tiles_from_geom(bbox, zoom))


    print len(work_tiles), "tiles to be processed"

    f = partial(worker,
        mapchete_file=parsed.mapchete_file,
        tile_pyramid=tile_pyramid,
        config=config
    )
    pool = Pool(cpu_count())
    log = ""

    try:
        counter = 0
        pbar = ProgressBar(maxval=len(work_tiles)).start()
        for output in pool.imap_unordered(f, work_tiles):
            counter += 1
            pbar.update(counter)
            if output:
                log += str(output) + "\n"
        pbar.finish()
    except:
        raise
    finally:
        pool.close()
        pool.join()


    if config["output_format"] == "GTiff":
        for zoom in config["zoom_levels"]:
            out_dir = os.path.join(config["output_name"], str(zoom))
            out_vrt = os.path.join(config["output_name"], (str(zoom)+".vrt"))
            command = "gdalbuildvrt -overwrite %s %s" %(
                out_vrt,
                str(out_dir + "/*/*.tif")
            )
            os.system(command)

    if parsed.log:
        print log


def worker(tile, mapchete_file, tile_pyramid, config):
    # Prepare input process
    process_name = os.path.splitext(os.path.basename(config["process_file"]))[0]
    new_process = imp.load_source(
        process_name + "Process",
        config["process_file"]
        )
    config["tile"] = tile
    config["tile_pyramid"] = tile_pyramid
    mapchete_process = new_process.Process(config)
    # print "processing", user_defined_process.identifier

    try:
        mapchete_process.execute()
    except Exception as e:
        return tile, traceback.print_exc(), e
    finally:
        mapchete_process = None
    return tile, "ok", None


if __name__ == "__main__":
    main(sys.argv[1:])
