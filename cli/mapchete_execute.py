#!/usr/bin/env python

import os
import sys
import argparse
import imp

from mapchete import *

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("--zoom", "-z", type=int)
    parser.add_argument("process", type=str)
    parser.add_argument("config_yaml", type=str)
    parsed = parser.parse_args(args)
    zoom = parsed.zoom
    process_file = parsed.process
    config_yaml = parsed.config_yaml

    process_name = os.path.splitext(os.path.basename(process_file))[0]

    # Load source process from python file and initialize.
    new_process = imp.load_source(process_name + ".Process", process_file)
    user_defined_process = new_process.Process(config_yaml)

    print "processing", user_defined_process.identifier

    # Analyze extent of input files and get tile indices.

    for zoom in range(0, 13):
        print zoom, user_defined_process.execute(zoom)


if __name__ == "__main__":
    main(sys.argv[1:])
