#!/usr/bin/env python

import os
import sys
import argparse
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
        process_host = MapcheteHost(
            parsed.mapchete_file,
            zoom=parsed.zoom,
            bounds=parsed.bounds
            )
    except Exception as e:
        raise

    work_tiles = process_host.get_work_tiles()

    print len(work_tiles), "tiles to be processed"

    overwrite=True

    # f = partial(process_host.save_tile, overwrite=overwrite)

    f = partial(worker,
        process_host=process_host,
        overwrite=overwrite
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


    if process_host.config["output_format"] in ["GTiff", "PNG"]:
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
        print log


def worker(tile, process_host, overwrite):

    process_host.save_tile(tile, overwrite)


if __name__ == "__main__":
    main(sys.argv[1:])
