#!/usr/bin/env python


def config_subparser(merge_dem_parser):

    merge_dem_parser.add_argument("--raster", required=True, nargs=1, type=str, dest="input_files")


def process(metatile, parsed, metatilematrix):
    pass

    