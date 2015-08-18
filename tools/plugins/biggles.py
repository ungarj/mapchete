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


def process(metatile, params, metatilematrix):

    from scipy import ndimage
    from skimage import exposure

    raster_file = params.input_files[0]
    output_folder = params.output_folder
    zoom, row, col = metatile
    tilematrix = metatilematrix.tilematrix
    tiles = metatilematrix.tiles_from_tilematrix(zoom, row, col)

    metadata, rasterdata = read_raster_window(raster_file, metatilematrix, metatile,
        pixelbuffer=20)

    median = ndimage.median_filter(rasterdata, size=5)
    gauss = ndimage.gaussian_filter(median, 5)

    # hillshade 1
    #hs1 = hillshade(gauss, 315, 45)
    #gauss = ndimage.gaussian_filter(hs1, 0.5)
    #hs1 = ndimage.median_filter(gauss, size=5)*1.5

    # hillshade 2
    hs2 = hillshade(gauss, 285, 45)
    #gauss = ndimage.gaussian_filter(hs2, 0.5)
    hs2 = ndimage.median_filter(hs2, size=5)

    # hillshade 3
    hs3 = hillshade(gauss, 345, 45)
    #gauss = ndimage.gaussian_filter(hs3, 0.5)
    hs3 = ndimage.median_filter(hs3, size=5)

    hs = np.minimum(hs2, hs3)

    gauss = ndimage.gaussian_filter(hs, 0.5)

    hs = gauss

    if zoom > 30:

        ripple_threshold = 131
        ripples = hillshade(rasterdata, 315, 90).astype(rasterio.uint8)
        ripples[ripples == 0] = 255
        ripples[ripples < ripple_threshold] = 1
        ripples[ripples >= ripple_threshold] = 255
    
        gauss = ndimage.gaussian_filter(ripples, 1)
    
        ripples = gauss
    
        out = ndimage.gaussian_filter(ripples, 1)
    
        out = np.minimum(hs, ripples)
    
    else:

        out = hs

    

#    if isinstance(out, np.ndarray):
#        extension = metatilematrix.format.extension
#
#        create_and_clean_dirs(metatile, params, extension)
#        out_tile = tile_path(metatile, params, extension)
#        try:
#            write_raster_window(out_tile, metatilematrix, metatile, metadata,
#                [out], pixelbuffer=0)
#        except:
#            raise

    for tile in tiles:
        zoom, row, col = tile

        if isinstance(out, np.ndarray):
            # Create directories.
            extension = metatilematrix.format.extension

            create_and_clean_dirs(tile, params, extension)

            out_tile = tile_path(tile, params, extension)

            try:
                write_raster_window(out_tile, tilematrix, tile, metadata,
                    [out], pixelbuffer=0)
            except:
                raise
        else:
            pass