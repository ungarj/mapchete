#!/usr/bin/env python

import os
import sys
import argparse

from mapchete import *

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("input_raster", type=str)
    parser.add_argument(
        "pyramid_type",
        type=str,
        choices=["geodetic", "mercator"]
    )
    parser.add_argument("output_dir", type=str)
    parser.add_argument(
        "resampling_method",
        type=str,
        choices=[
            "nearest",
            "bilinear",
            "cubic",
            "cubic_spline",
            "lanczos",
            "average",
            "mode"
        ],
        default="nearest"
    )
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument(
        "--bounds",
        "-b",
        type=float,
        nargs='*',
        help='left bottom right top in pyramid CRS (i.e. either EPSG:4326 for \
        geodetic or EPSG:3857 for mercator)'
    )
    parser.add_argument("--log", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parsed = parser.parse_args(args)

    pyramid = MapchetePyramid(
        parsed.input_raster,
        parsed.pyramid_type,
        parsed.output_dir,
        resampling=parsed.resampling_method,
        zoom=parsed.zoom,
        bounds=parsed.bounds,
        overwrite=parsed.overwrite
    )

    pyramid.seed()

if __name__ == "__main__":
    main()
