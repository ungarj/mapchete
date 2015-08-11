#!/usr/bin/env python

import os
import sys

# import local modules independent from script location
scriptdir = os.path.dirname(os.path.realpath(__file__))
rootdir = os.path.split(scriptdir)[0]
sys.path.append(os.path.join(rootdir, 'modules'))
from tilematrix import *
from tilematrix_io import *
from mapchete_commons import *


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
            # Create directories.
            create_and_clean_dirs(tile, parsed)

            out_tile = tile_path(tile, parsed)

            try:
                write_raster_window(out_tile, tilematrix, tile, metadata,
                    rasterdata, pixelbuffer=0)
            except:
                raise
        else:
            pass