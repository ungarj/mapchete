#!/usr/bin/env python

import sys
import os
import argparse
import fiona
from shapely.geometry import *
from shapely.wkt import *
from shapely.ops import cascaded_union
import math
import imp

from tilematrix import *
from mapchete import *

ROUND = 10

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true")
    parsed = parser.parse_args(args)
    global debug
    debug = parsed.debug

    scriptdir = os.path.dirname(os.path.realpath(__file__))


    # YAML configuration
    #===================

    # Load source process from python file and initialize.
    process_file = os.path.join(scriptdir, "example_process.py")
    config_yaml = os.path.join(scriptdir, "example_configuration.yaml")

    config = MapcheteConfig(config_yaml)

    try:
        # Check configuration at zoom level 5
        zoom5 = config.at_zoom(5)
        input_files = zoom5["input_files"]
        assert input_files["file_group_1"]["file1"] == "/path/to/group1/file1"
        assert input_files["file_group_1"]["file2"] == "/path/to/group1/file2"
        assert input_files["file_group_1"]["file3"] == {}
        assert input_files["file_group_2"]["file1"] == "/path/to/group2/file1"
        assert input_files["file_group_2"]["file2"] == "/path/to/group2/file2"
        assert input_files["file_group_2"]["file3"] == "/path/to/group2/file3"
        assert input_files["file_group_2"]["file4"] == "/path/to/group2/file4"
        assert zoom5["some_integer_parameter"] == 12
        assert zoom5["some_string_parameter"] == "string1"

        # Check configuration at zoom level 11
        zoom11 = config.at_zoom(11)
        input_files = zoom11["input_files"]
        assert input_files["file_group_1"]["file1"] == "/path/to/group1/file1"
        assert input_files["file_group_1"]["file2"] == "/path/to/group1/file2"
        assert input_files["file_group_1"]["file3"] == "/path/to/group1/file3"
        assert input_files["file_group_2"]["file1"] == "/path/to/group2/file1"
        assert input_files["file_group_2"]["file2"] == "/path/to/group2/file2"
        assert input_files["file_group_2"]["file3"] == "/path/to/group2/file3"
        assert input_files["file_group_2"]["file4"] == "/path/to/group2/file4"
        assert zoom11["some_integer_parameter"] == 12
        assert zoom11["some_string_parameter"] == "string2"
    except:
        print "FAILED: configuration parsing"
        raise
    else:
        print "OK: configuration parsing"



    # process_name = os.path.splitext(os.path.basename(process_file))[0]
    # new_process = imp.load_source(process_name + ".Process", process_file)
    # user_defined_process = new_process.Process(config_yaml)
    #
    # print "processing", user_defined_process.identifier
    #
    # # Analyze extent of input files and get tile indices.
    # zoom = 5
    # configuration = user_defined_process.execute(zoom)
    # for zoom in range(0, 13):
    #     print zoom, user_defined_process.execute(zoom)


if __name__ == "__main__":
    main(sys.argv[1:])
