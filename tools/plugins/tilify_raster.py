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

    tilify_raster_parser.add_argument("--raster", required=True, nargs=1,
        type=str, dest="input_files")


def process(metatile, params, metatilematrix):

    raster_file = params.input_files[0]
    output_folder = params.output_folder
    zoom, row, col = metatile
    tilematrix = metatilematrix.tilematrix
    tiles = metatilematrix.tiles_from_tilematrix(zoom, row, col)

    metadata, rasterdata = read_raster_window(raster_file, metatilematrix, metatile,
        pixelbuffer=3)

    for tile in tiles:
        zoom, row, col = tile
    
        if isinstance(rasterdata, np.ndarray):
            # Create directories.
            extension = metatilematrix.format.extension

            create_and_clean_dirs(tile, params, extension)

            out_tile = tile_path(tile, params, extension)

            try:
                write_raster_window(out_tile, tilematrix, tile, metadata,
                    [rasterdata], pixelbuffer=0)
            except:
                raise
        else:
            pass