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
submodules_folder = os.path.join(rootdir, 'submodules')
tilematrix_module_directory = os.path.join(submodules_folder, 'tilematrix')
tilematrix_class_directory = os.path.join(tilematrix_module_directory, 'src')
sys.path.append(tilematrix_class_directory)

from tilematrix import *
from tilematrix_io import *
from mapchete_commons import *

ROUND = 20

loaded_plugins = {}

def main(args):

    parser = argparse.ArgumentParser()

    parser.add_argument("EPSG", nargs=1, type=int)
    parser.add_argument("zoom", nargs=1, type=int)
    parser.add_argument("output_folder", nargs=1, type=str)
    parser.add_argument("format", nargs=1, type=str)
    parser.add_argument("--dtype", nargs=1, type=str, default=None)
    parser.add_argument("--bounds", "-b", nargs=4, type=float,
        help="Only process area within bounds (left, bottom, right, top).")
    parser.add_argument("--metatiling", "-m", nargs=1, type=int, default=1,
        help="Metatile size. (default 1)")
    parser.add_argument("--parallel", "-p", nargs=1, type=int, default=[1], # wtf idk
        help="Number of parallel processes. (default 1)")
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

    params = MapcheteConfig()
    params.load_from_argparse(parsed)

    mapchete(params)


def mapchete(params, tiles=None, metatiles=None):

    for loader, module_name, ispkg in pkgutil.iter_modules(plugins.__path__):
        plugin = loader.find_module(module_name).load_module(module_name)
        loaded_plugins[module_name] = plugin

    epsg = params.epsg
    zoom = params.zoom
    output_folder = params.output_folder
    output_format = params.profile
    dtype = params.dtype
    metatiling = params.metatiling
    parallel = params.parallel
    bounds = params.bounds
    create_vrt = params.create_vrt
    global debug
    debug = params.debug

    # Initialize TileMatrix and MetaTileMatrix.
    tilematrix = TileMatrix(epsg)
    tilematrix.set_format(output_format, dtype)
    metatilematrix = MetaTileMatrix(tilematrix, metatiling)

    # Read input files and get union of envelopes.
    input_files = params.input_files
    envelopes = []
    for input_file in input_files:
        envelope = raster_bbox(input_file, tilematrix)
        envelopes.append(envelope)
    input_envelopes = cascaded_union(envelopes)
    process_area = input_envelopes

    if tiles:
        if metatiles:
            print "Tiles and metatiles provided. In this case, just the tiles \
               are being considered."
            metatiles = None
        out_metatiles = []
        for tile in tiles:
            zoom, row, col = tile
            bbox = tilematrix.tile_bbox(zoom, row, col)
            if bbox.intersects(process_area):
                out_metatiles.extend(metatilematrix.tiles_from_geom(bbox, zoom))

    elif metatiles:
        out_metatiles = []
        for metatile in metatiles:
            bbox = metatilematrix.tile_bbox(*metatile)
            if bbox.intersects(process_area):
                out_metatiles.append(metatile)
    else:
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
        out_metatiles = metatilematrix.tiles_from_geom(process_area, zoom)

    if len(out_metatiles) == 0:
        return None

    from functools import partial
    f = partial(worker,
        params=params,
        metatilematrix=metatilematrix
    )

    try:
        pool = Pool(parallel)
        total_metatiles = len(out_metatiles)
        counter = 0
        pbar = ProgressBar(maxval=total_metatiles).start()
        for output in pool.imap_unordered(f, out_metatiles):
            counter += 1
            pbar.update(counter)
        pbar.finish()
    except Exception as e:
        print e
        traceback.print_exc()
        sys.exit(0)
    finally:
        pool.close()
        pool.join()

    if create_vrt and metatilematrix.format.type == "raster":
        print "creating VRT ..."
        target_vrt = os.path.join(output_folder, (str(zoom) + ".vrt"))
        target_files = ((os.path.join(output_folder, str(zoom))) + "/*/*" + \
            metatilematrix.format.extension)
        command = "gdalbuildvrt -overwrite %s %s" %(target_vrt, target_files)
        os.system(command)


def worker(metatile, params, metatilematrix):

    output_folder = params.output_folder[0]

    zoom, row, col = metatile

    try:

        return loaded_plugins[params.method].process(metatile, params, metatilematrix)

    except Exception as e:
        traceback.print_exc()



if __name__ == "__main__":
    main(sys.argv[1:])
