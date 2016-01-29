#!/usr/bin/env python

import os
import sys
import argparse
import imp
import yaml

from mapchete import *
from tilematrix import TilePyramid

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
        tile_pyramid = TilePyramid("4326")
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

    print config

    process_name = os.path.splitext(os.path.basename(config["process_file"]))[0]

    # Load source process from python file and initialize.
    new_process = imp.load_source(
        process_name + "Process",
        config["process_file"]
        )
    user_defined_process = new_process.Process(parsed.mapchete_file)

    print "processing", user_defined_process.identifier

#     from functools import partial
#     f = partial(worker,
#         params=params,
#         metatilematrix=metatilematrix
#     )
#
#     try:
#         pool = Pool(parallel)
#         total_metatiles = len(out_metatiles)
#         counter = 0
#         pbar = ProgressBar(maxval=total_metatiles).start()
#         for output in pool.imap_unordered(f, out_metatiles):
#             counter += 1
#             pbar.update(counter)
#         pbar.finish()
#     except Exception as e:
#         print e
#         traceback.print_exc()
#         sys.exit(0)
#     finally:
#         pool.close()
#         pool.join()
#
#     if create_vrt and metatilematrix.format.type == "raster":
#         print "creating VRT ..."
#         target_vrt = os.path.join(output_folder, (str(zoom) + ".vrt"))
#         target_files = ((os.path.join(output_folder, str(zoom))) + "/*/*" + \
#             metatilematrix.format.extension)
#         command = "gdalbuildvrt -overwrite %s %s" %(target_vrt, target_files)
#         os.system(command)
#
#
# def worker(metatile, params, metatilematrix):
#
#     output_folder = params.output_folder[0]
#
#     zoom, row, col = metatile
#
#     try:
#
#         return loaded_plugins[params.method].process(metatile, params, metatilematrix)
#
#     except Exception as e:
#         traceback.print_exc()


if __name__ == "__main__":
    main(sys.argv[1:])
