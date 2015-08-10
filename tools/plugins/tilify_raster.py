#!/usr/bin/env python

import os
import sys

# import local modules independent from script location
scriptdir = os.path.dirname(os.path.realpath(__file__))
rootdir = os.path.split(scriptdir)[0]
sys.path.append(os.path.join(rootdir, 'modules'))
from tilematrix import *
from tilematrix_io import *


def config_subparser(tilify_raster_parser):

    tilify_raster_parser.add_argument("--raster", required=True, nargs=1, type=str, dest="input_files")


def process(metatile, parsed, metatilematrix):

    raster_file = parsed.input_files[0]
    output_folder = parsed.output_folder[0]
    zoom, col, row = metatile
    tilematrix = metatilematrix.tilematrix
    tiles = metatilematrix.tiles_from_tilematrix(zoom, col, row)

    metadata, rasterdata = read_raster_window(raster_file, metatilematrix, metatile,
        pixelbuffer=1)

    for tile in tiles:
        zoom, col, row = tile
    
        if isinstance(rasterdata, np.ndarray):
            basedir = output_folder
            zoomdir = os.path.join(basedir, str(zoom))
            rowdir = os.path.join(zoomdir, str(row))
            out_tile = os.path.join(rowdir, (str(col) + ".tif"))
            # Creat output folders
            if not os.path.exists(basedir):
                os.makedirs(basedir)
            if not os.path.exists(zoomdir):
                os.makedirs(zoomdir)
            if not os.path.exists(rowdir):
                os.makedirs(rowdir)
            
            if os.path.exists(out_tile):
                os.remove(out_tile)

            try:
                write_raster_window(out_tile, tilematrix, tile, metadata,
                    rasterdata, pixelbuffer=0)
            except:
                raise
        else:
            pass

    return "herbert"
