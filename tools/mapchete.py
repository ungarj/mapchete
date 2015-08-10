#!/usr/bin/env python

import sys
import os
import argparse
import fiona
from shapely.geometry import *
from shapely.ops import cascaded_union
import rasterio
from rasterio.warp import *
import numpy
import plugins
import pkgutil
from multiprocessing import Pool
from progressbar import ProgressBar

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
    global debug
    debug = parsed.debug


    # Initialize TileMatrix and MetaTileMatrix.
    tilematrix = TileMatrix(epsg)
    metatilematrix = MetaTileMatrix(tilematrix, metatiling)

    # Read input files and get union of envelopes.
    input_files = parsed.input_files
    envelopes = []
    for input_file in input_files:
        with rasterio.open(input_file) as raster:
            tl = [raster.bounds.left, raster.bounds.top]
            tr = [raster.bounds.right, raster.bounds.top]
            br = [raster.bounds.right, raster.bounds.bottom]
            bl = [raster.bounds.left, raster.bounds.bottom]
            envelope = Polygon([tl, tr, br, bl])
        envelopes.append(envelope)
    input_envelopes = cascaded_union(envelopes)
    process_area = input_envelopes

    if bounds:
        tl = [bounds[0], bounds[3]]
        tr = [bounds[2], bounds[3]]
        br = [bounds[0], bounds[1]]
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

    pool = Pool(parallel)

    total_metatiles = len(metatiles)
    counter = 0
    pbar = ProgressBar(maxval=total_metatiles).start()
    for output in pool.imap_unordered(f, metatiles):
        counter += 1
        pbar.update(counter)
        

    


def worker(metatile, parsed, metatilematrix):

    output_folder = parsed.output_folder[0]

    zoom, col, row = metatile

    # Create output folders if not existing.
    # OGC Standard:
    # {TileMatrixSet}/{TileMatrix}/{TileRow}/{TileCol}.png

    return loaded_plugins[parsed.method].process(metatile, parsed, metatilematrix)




    
if __name__ == "__main__":
    main(sys.argv[1:])