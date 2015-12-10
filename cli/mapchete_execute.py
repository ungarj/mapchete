#!/usr/bin/env python

import os
import sys
import argparse
import imp

# import local modules
current_dir = os.path.dirname(os.path.realpath(__file__))
root_dir = os.path.split(current_dir)[0]
sys.path.insert(0, root_dir)
from src.mapchete import *

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("process", type=str)
    parser.add_argument("config", type=str)
    parsed = parser.parse_args(args)
    process_file = parsed.process
    config_yaml = parsed.config

    process_name = os.path.splitext(os.path.basename(process_file))[0]

    # Load source process and initialize.
    new_process = imp.load_source(process_name + ".Process", process_file)
    config = "herbert"
    user_defined_process = new_process.Process(config_yaml)

    print "processing", user_defined_process.identifier

    for zoom in range(0, 21):
        print zoom, user_defined_process.execute(zoom)
        # user_defined_process.execute(zoom)


if __name__ == "__main__":
    main(sys.argv[1:])
