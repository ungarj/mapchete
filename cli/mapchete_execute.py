#!/usr/bin/env python

import os
import sys
import argparse
import imp
import yaml

from mapchete import *
from tilematrix import TilePyramid

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("process", type=str)
    parser.add_argument("config_yaml", type=str)
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument("--bounds", "-b", type=float, nargs='*')
    parsed = parser.parse_args(args)

    try:
        config = get_clean_configuration(
            parsed.process,
            parsed.config_yaml,
            zoom=parsed.zoom,
            bounds=parsed.bounds
            )
        tile_pyramid = TilePyramid("4326")
        for zoom in config["zoom_levels"]:
            bbox = config["process_bounds"][zoom]
            print zoom, bbox.wkt
            print tile_pyramid.tiles_from_geom(bbox, zoom)
    except Exception as e:
        #sys.exit(e)
        raise


    # process_name = os.path.splitext(os.path.basename(process_file))[0]
    #
    # # Load source process from python file and initialize.
    # new_process = imp.load_source(process_name + ".Process", process_file)
    # user_defined_process = new_process.Process(config_yaml)
    #
    # print "processing", user_defined_process.identifier
    #
    # # Determine tiles to be processed, depending on:
    # # - zoom level and
    # # - input files bounds OR user defined bounds
    #
    # for zoom in range(0, 13):
    #     print zoom, user_defined_process.execute(zoom)

if __name__ == "__main__":
    main(sys.argv[1:])
