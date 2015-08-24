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


def config_subparser(hillshade_parser):

    hillshade_parser.add_argument("--raster", required=True, nargs=1,
        type=str, dest="input_files")
    hillshade_parser.add_argument("--azimuth", nargs=1, type=int, default=315)
    hillshade_parser.add_argument("--altitude", nargs=1, type=int, default=65)


def process(metatile, parsed, metatilematrix):

    raster_file = parsed.input_files[0]
    output_folder = parsed.output_folder[0]
    zoom, row, col = metatile
    tilematrix = metatilematrix.tilematrix
    tiles = metatilematrix.tiles_from_tilematrix(zoom, row, col)

    metadata, rasterdata = read_raster_window(raster_file, metatilematrix, metatile,
        pixelbuffer=3)

    hs = hillshade(rasterdata, 315, 45)

    hs = -(hs - 255)

    hs[rasterdata.mask] = 0 

    for tile in tiles:
        zoom, row, col = tile
    
        if isinstance(hs, np.ndarray):
            # Create directories.
            extension = metatilematrix.format.extension

            create_and_clean_dirs(tile, parsed, extension)

            out_tile = tile_path(tile, parsed, extension)

            try:
                write_raster_window(out_tile, tilematrix, tile, metadata,
                    [hs], pixelbuffer=0)
            except:
                raise
        else:
            pass