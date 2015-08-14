#!/usr/bin/env python

import sys
import os
import argparse
import fiona
from shapely.geometry import *
from shapely.ops import cascaded_union
import rasterio
from rasterio.warp import *
from rasterio import profiles
import numpy
import plugins
import pkgutil
from multiprocessing import Pool
from progressbar import ProgressBar
import traceback

# import local modules independent from script location
scriptdir = os.path.dirname(os.path.realpath(__file__))
rootdir = os.path.split(scriptdir)[0]
sys.path.append(os.path.join(rootdir, 'modules'))
from tilematrix import *
from tilematrix_io import *

ROUND = 20

loaded_plugins = {}

def main(args):

    parser = argparse.ArgumentParser()

    parser.add_argument("EPSG", nargs=1, type=int)
    parser.add_argument("zoom", nargs=1, type=int)
    parser.add_argument("output_folder", nargs=1, type=str)
    parser.add_argument("--bounds", "-b", nargs=4, type=float,
        help="Only process area within bounds (left, bottom, right, top).")
    parser.add_argument("--metatiling", "-m", nargs=1, type=int, default=1, 
        help="Metatile size. (default 1)")
    parser.add_argument("--parallel", "-p", nargs=1, type=int, default=[1], # wtf idk
        help="Number of parallel processes. (default 1)")
    parser.add_argument("--format", "-f", nargs=1, type=str, default="GTiff")
    parser.add_argument("--create_vrt", "-vrt", action="store_true")
    parser.add_argument("--debug", "-d", action="store_true")

    subparsers = parser.add_subparsers(help='sub-command help')

    for loader, module_name, ispkg in pkgutil.iter_modules(plugins.__path__):
        plugin = loader.find_module(module_name).load_module(module_name)

        subparser = subparsers.add_parser(module_name)
        plugin.config_subparser(subparser)
        subparser.set_defaults(method=module_name)
        loaded_plugins[module_name] = plugin

    parsed = parser.parse_args(args)

    epsg = str(parsed.EPSG[0])
    zoom = parsed.zoom[0]
    output_folder = parsed.output_folder[0]
    metatiling = parsed.metatiling[0]
    parallel = parsed.parallel[0]
    bounds = parsed.bounds
    output_format = parsed.format
    create_vrt = parsed.create_vrt
    global debug
    debug = parsed.debug

    # Initialize TileMatrix and MetaTileMatrix.
    tilematrix = TileMatrix(epsg)
    tilematrix.set_format(output_format)
    metatilematrix = MetaTileMatrix(tilematrix, metatiling)

    # Read input files and get union of envelopes.
    input_files = parsed.input_files
    envelopes = []
    for input_file in input_files:
        envelope = raster_bbox(input_file, tilematrix)
        envelopes.append(envelope)
    input_envelopes = cascaded_union(envelopes)
    process_area = input_envelopes

    if bounds:
        tl = [bounds[0], bounds[3]]
        tr = [bounds[2], bounds[3]]
        br = [bounds[2], bounds[1]]
        bl = [bounds[0], bounds[1]]
        bbox = Polygon([tl, tr, br, bl])
        if bbox.intersects(input_envelopes):
            process_area = bbox.intersection(input_envelopes)
        else:
            print "ERROR: bounds don't intersect with input files."
            sys.exit(0)

    # Get metatiles from metatilematrix and process area.
    metatiles = metatilematrix.tiles_from_geom(process_area, zoom)

    from functools import partial
    f = partial(worker,
        parsed=parsed,
        metatilematrix=metatilematrix
    )

    try:
        pool = Pool(parallel)
        total_metatiles = len(metatiles)
        counter = 0
        pbar = ProgressBar(maxval=total_metatiles).start()
        for output in pool.imap_unordered(f, metatiles):
            counter += 1
            pbar.update(counter)
        pbar.finish()
    except Exception as e:
        print e
        traceback.print_exc()
        sys.exit(0)

    if create_vrt and metatilematrix.format.type == "raster":
        print "creating VRT ..."
        target_vrt = os.path.join(output_folder, (str(zoom) + ".vrt"))
        target_files = ((os.path.join(output_folder, str(zoom))) + "/*/*" + \
            metatilematrix.format.extension)
        command = "gdalbuildvrt -overwrite %s %s" %(target_vrt, target_files)
        os.system(command)


def worker(metatile, parsed, metatilematrix):

    output_folder = parsed.output_folder[0]

    zoom, col, row = metatile

    try:

        return loaded_plugins[parsed.method].process(metatile, parsed, metatilematrix)

    except Exception as e:
        traceback.print_exc()


    
if __name__ == "__main__":
    main(sys.argv[1:])