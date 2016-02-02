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
    mapchete_file = os.path.join(scriptdir, "example.mapchete")

    config = MapcheteConfig(mapchete_file)

    try:
        # Check configuration at zoom level 5
        zoom5 = config.at_zoom(5)
        input_files = zoom5["input_files"]
        assert input_files["file1"] == None
        assert input_files["file2"] == "testdata/dummy2.tif"
        assert zoom5["some_integer_parameter"] == 12
        assert zoom5["some_float_parameter"] == 5.3
        assert zoom5["some_string_parameter"] == "string1"
        assert zoom5["some_bool_parameter"] == True

        # Check configuration at zoom level 11
        zoom11 = config.at_zoom(11)
        input_files = zoom11["input_files"]
        assert input_files["file1"] == "testdata/dummy1.tif"
        assert input_files["file2"] == "testdata/dummy2.tif"
        assert zoom11["some_integer_parameter"] == 12
        assert zoom11["some_float_parameter"] == 5.3
        assert zoom11["some_string_parameter"] == "string2"
        assert zoom11["some_bool_parameter"] == True
    except:
        print "FAILED: basic configuration parsing"
        print input_files
        raise
    else:
        print "OK: basic configuration parsing"


    # Validate configuration constructor
    ## basic run through
    try:
        config = get_clean_configuration(mapchete_file)
        print "OK: basic configuraiton constructor run through"
    except:
        print "FAILED: basic configuraiton constructor run through"
        raise
    ## read zoom level from config file
    mapchete_file = os.path.join(scriptdir, "testdata/zoom.mapchete")
    try:
        config = get_clean_configuration(mapchete_file)
        assert 5 in config["zoom_levels"]
        print "OK: read zoom level from config file"
    except:
        print "FAILED: read zoom level from config file"
        print mapchete_file
        raise
    ## read min/max zoom levels from config file
    mapchete_file = os.path.join(scriptdir, "testdata/minmax_zoom.mapchete")
    config = get_clean_configuration(mapchete_file)
    try:
        for zoom in [7, 8, 9, 10]:
            assert zoom in config["zoom_levels"]
        print "OK: read  min/max zoom levels from config file"
    except:
        print "FAILED: read  min/max zoom levels from config file"
        raise
    ## zoom levels override
    mapchete_file = os.path.join(scriptdir, "testdata/minmax_zoom.mapchete")
    config = get_clean_configuration(
        mapchete_file,
        zoom=[1, 4]
        )
    try:
        for zoom in [1, 2, 3, 4]:
            assert zoom in config["zoom_levels"]
        print "OK: zoom levels override"
    except:
        print "FAILED: zoom levels override"
        raise
    ## read bounds from config file
    mapchete_file = os.path.join(scriptdir, "testdata/zoom.mapchete")
    config = get_clean_configuration(mapchete_file)
    try:
        test_polygon = Polygon([
            [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]
            ])
        assert config["zoom_levels"][5]["process_area"].equals(test_polygon)
        print "OK: read bounds from config file"
    except:
        print "FAILED: read bounds from config file"
        print config["zoom_levels"][5]["process_area"]
        raise
    ## override bounds
    mapchete_file = os.path.join(scriptdir, "testdata/zoom.mapchete")
    config = get_clean_configuration(
        mapchete_file,
        bounds=[3, 2, 3.5, 1.5]
        )
    try:
        test_polygon = Polygon([
            [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]
            ])
        assert config["zoom_levels"][5]["process_area"].equals(test_polygon)
        print "OK: override bounds"
    except:
        print "FAILED: override bounds"
        print config["zoom_levels"][5]["process_area"]
        raise
    ## read bounds from input files
    mapchete_file = os.path.join(scriptdir, "testdata/files_bounds.mapchete")
    config = get_clean_configuration(mapchete_file)
    try:
        test_polygon = Polygon(
        [[3, 2], [4, 2], [4, 1], [3, 1], [2, 1], [2, 4], [3, 4], [3, 2]]
        )
        assert config["zoom_levels"][10]["process_area"].equals(test_polygon)
        print "OK: read bounds from input files"
    except:
        print "FAILED: read bounds from input files"
        print config["zoom_levels"][10]["process_area"], test_polygon
        raise
    # process_name = os.path.splitext(os.path.basename(process_file))[0]
    # new_process = imp.load_source(process_name + ".Process", process_file)
    # user_defined_process = new_process.Process(mapchete_file)
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
