#!/usr/bin/env python

import os
import sys
import argparse
from functools import partial
from multiprocessing import Pool, cpu_count
from progressbar import ProgressBar
import time

from mapchete import *
from tilematrix import TilePyramid, MetaTilePyramid

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

    f = partial(worker,
        process_host=process_host,
        overwrite=overwrite
    )
    pool = Pool()
    logs = []

    try:
        output = pool.map_async(f, work_tiles, callback=logs.extend)
        total = output._number_left
        pbar = ProgressBar(maxval=total).start()
        pool.close()
        while (True):
            if (output.ready()): break
            counter = total - output._number_left
            pbar.update(counter)
            # update cycle set to one second
            time.sleep(1)
        pbar.finish()
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
            print row


def worker(tile, process_host, overwrite):

    log = process_host.save_tile(tile, overwrite)
    if log[1]:
        return log


if __name__ == "__main__":
    main(sys.argv[1:])
