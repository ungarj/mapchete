#!/usr/bin/env python


def config_subparser(merge_dem_parser):

    merge_dem_parser.add_argument("--raster", required=True, nargs=1, type=str, dest="input_files")


def process(metatile, parsed, metatilematrix):
    pass

    # Load and rescale primary DEM.

    # Load coastline if NODATA pixel exists.

    # Check for voids within land mass (using coastline).

    # If voids, load next DEM.