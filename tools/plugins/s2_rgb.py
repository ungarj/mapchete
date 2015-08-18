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


def config_subparser(s2_rgb_parser):

    s2_rgb_parser.add_argument("--input_files", required=True, nargs='*',
        type=str)

def process(metatile, parsed, metatilematrix):

    red_file = parsed.input_files[0]
    green_file = parsed.input_files[1]
    blue_file = parsed.input_files[2]

    zoom, row, col = metatile
    tilematrix = metatilematrix.tilematrix
    tiles = metatilematrix.tiles_from_tilematrix(zoom, row, col)

    # scale red band
    red_meta, red_data = read_raster_window(red_file, metatilematrix, metatile,
        pixelbuffer=1)
    red_data[red_data > 255] = 255
    red_data = red_data.astype(np.uint8)

    # scale green band
    green_meta, green_data = read_raster_window(green_file, metatilematrix, metatile,
        pixelbuffer=1)
    green_data[green_data > 255] = 255
    green_data = green_data.astype(np.uint8)

    # scale blue band
    blue_meta, blue_data = read_raster_window(blue_file, metatilematrix, metatile,
        pixelbuffer=1)
    blue_data[blue_data > 255] = 255
    blue_data = blue_data.astype(np.uint8)

    out_meta = red_meta
    out_meta.update(dtype=rasterio.uint8)
    out_meta.update(driver='PNG')
    out_meta.update(count=3)
    out_meta.update(nodata=0)
    del out_meta['compress']

    out_bands = [red_data, green_data, blue_data]

    for tile in tiles:
        zoom, row, col = tile
    
        if isinstance(red_data, np.ndarray):
            # Create directories.
            create_and_clean_dirs(tile, parsed, ".png")

            out_tile = tile_path(tile, parsed, ".png")

            try:
                write_raster_window(out_tile, tilematrix, tile, out_meta,
                    out_bands, pixelbuffer=0)
            except:
                raise
        else:
            pass

