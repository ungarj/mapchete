#!/usr/bin/env python

import os
import sys
import argparse

import imp


def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("process", type=str)
    parsed = parser.parse_args(args)
    process_file = parsed.process

    process_name = os.path.splitext(os.path.basename(process_file))[0]

    # Load source process and initialize.
    foo = imp.load_source(process_name + ".Process", process_file)
    user_defined_process = foo.Process()

    print "processing", user_defined_process.identifier

    # Parse configuration.
    ## Check if input files exist.

    tile = 3, 3, 3
    zoom, row, col = tile
    print user_defined_process.execute(zoom)


if __name__ == "__main__":
    main(sys.argv[1:])
