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
    except Exception as e:
        #sys.exit(e)
        raise

    # Determine tiles to be processed, depending on:
    # - zoom level and
    # - input files bounds OR user defined bounds
    work_tiles = []
    for zoom in config["zoom_levels"]:
        bbox = config["process_area"][zoom]
        if not bbox.is_empty:
            work_tiles.extend(tile_pyramid.tiles_from_geom(bbox, zoom))

    print len(work_tiles), "tiles to be processed"

    process_name = os.path.splitext(os.path.basename(parsed.process))[0]

    # Load source process from python file and initialize.
    new_process = imp.load_source(
        process_name + "Process",
        parsed.process
        )
    user_defined_process = new_process.Process(parsed.config_yaml)

    print "processing", user_defined_process.identifier



if __name__ == "__main__":
    main(sys.argv[1:])
