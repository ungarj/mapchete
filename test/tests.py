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
        assert input_files["file1"] == None
        assert input_files["file2"] == "testdata/dummy2.tif"
        assert zoom5["some_integer_parameter"] == 12
        assert zoom5["some_string_parameter"] == "string1"

        # Check configuration at zoom level 11
        zoom11 = config.at_zoom(11)
        input_files = zoom11["input_files"]
        assert input_files["file1"] == "testdata/dummy1.tif"
        assert input_files["file2"] == "testdata/dummy2.tif"
        assert zoom11["some_integer_parameter"] == 12
        assert zoom11["some_string_parameter"] == "string2"
    except:
        print "FAILED: basic configuration parsing"
        print input_files
        raise
    else:
        print "OK: basic configuration parsing"


    # Validate configuration constructor
    ## basic run through
    try:
        config = get_clean_configuration(
            process_file,
            config_yaml
            )
        print "OK: basic configuraiton constructor run through"
    except:
        print "FAILED: basic configuraiton constructor run through"
        raise
    ## read zoom level from config file
    config_yaml = os.path.join(scriptdir, "testdata/zoom.yaml")
    try:
        config = get_clean_configuration(
            process_file,
            config_yaml
            )
        assert config["zoom_levels"] == [5]
        print "OK: read zoom level from config file"
    except:
        print "FAILED: read zoom level from config file"
        print config_yaml
        raise
    ## read min/max zoom levels from config file
    config_yaml = os.path.join(scriptdir, "testdata/minmax_zoom.yaml")
    config = get_clean_configuration(
        process_file,
        config_yaml
        )
    try:
        assert config["zoom_levels"] == [7, 8, 9, 10]
        print "OK: read  min/max zoom levels from config file"
    except:
        print "FAILED: read  min/max zoom levels from config file"
        raise
    ## zoom levels override
    config_yaml = os.path.join(scriptdir, "testdata/minmax_zoom.yaml")
    config = get_clean_configuration(
        process_file,
        config_yaml,
        zoom=[1, 4]
        )
    try:
        assert config["zoom_levels"] == [1, 2, 3, 4]
        print "OK: zoom levels override"
    except:
        print "FAILED: zoom levels override"
        raise
    ## read bounds from config file
    config_yaml = os.path.join(scriptdir, "testdata/zoom.yaml")
    config = get_clean_configuration(
        process_file,
        config_yaml
        )
    try:
        test_polygon = Polygon([[1, 4], [3, 4], [3, 2], [1, 2], [1, 4]])
        assert config["process_bounds"][5].equals(test_polygon)
        print "OK: read bounds from config file"
    except:
        print "FAILED: read bounds from config file"
        print config["process_bounds"][5]
        raise
    ## override bounds
    config_yaml = os.path.join(scriptdir, "testdata/zoom.yaml")
    config = get_clean_configuration(
        process_file,
        config_yaml,
        bounds=[5, 6, 7, 8.0]
        )
    try:
        test_polygon = Polygon([[5, 8], [7, 8], [7, 6], [5, 6], [5, 8]])
        assert config["process_bounds"][5].equals(test_polygon)
        print "OK: override bounds"
    except:
        print "FAILED: override bounds"
        print config["process_bounds"][5]
        raise
    ## read bounds from input files
    config_yaml = os.path.join(scriptdir, "testdata/files_bounds.yaml")
    config = get_clean_configuration(
        process_file,
        config_yaml
        )

    try:
        test_polygon = Polygon(
        [[3, 2], [4, 2], [4, 1], [3, 1], [2, 1], [2, 4], [3, 4], [3, 2]]
        )
        assert config["process_bounds"][10].equals(test_polygon)
        print "OK: read bounds from input files"
    except:
        print "FAILED: read bounds from input files"
        print config["process_bounds"][10], test_polygon
        raise
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
