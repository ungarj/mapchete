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
    test_polygon = Polygon([
        [3, 1.5],
        [3, 1.545454545454545],
        [3, 1.636363636363636],
        [3, 1.727272727272727],
        [3, 1.818181818181818],
        [3, 1.909090909090909],
        [3, 2],
        [3.090909090909091, 2],
        [3.181818181818182, 2],
        [3.272727272727272, 2],
        [3.363636363636364, 2],
        [3.454545454545455, 2],
        [3.5, 2],
        [3.5, 1.5],
        [3, 1.5]
    ])
    try:
        assert config["zoom_levels"][5]["process_area"].equals_exact(
            test_polygon,
            tolerance=0.001
            )
        print "OK: read bounds from config file"
    except:
        print "FAILED: read bounds from config file"
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
        assert config["zoom_levels"][5]["process_area"].difference(
            test_polygon
            ).is_empty
        print "OK: override bounds"
    except:
        print "FAILED: override bounds"
        print config["zoom_levels"][5]["process_area"].difference(test_polygon)
        raise
    ## read bounds from input files
    mapchete_file = os.path.join(scriptdir, "testdata/files_bounds.mapchete")
    config = get_clean_configuration(mapchete_file)
    test_polygon = Polygon([
        [3, 2],
        [3.090909090909091, 2],
        [3.181818181818182, 2],
        [3.272727272727272, 2],
        [3.363636363636364, 2],
        [3.454545454545455, 2],
        [3.545454545454545, 2],
        [3.636363636363636, 2],
        [3.727272727272728, 2],
        [3.818181818181818, 2],
        [3.909090909090909, 2],
        [4, 2],
        [4, 1.909090909090909],
        [4, 1.818181818181818],
        [4, 1.727272727272727],
        [4, 1.636363636363636],
        [4, 1.545454545454545],
        [4, 1.454545454545455],
        [4, 1.363636363636364],
        [4, 1.272727272727273],
        [4, 1.181818181818182],
        [4, 1.090909090909091],
        [4, 1],
        [3.909090909090909, 1],
        [3.818181818181818, 1],
        [3.727272727272728, 1],
        [3.636363636363636, 1],
        [3.545454545454545, 1],
        [3.454545454545455, 1],
        [3.363636363636364, 1],
        [3.272727272727272, 1],
        [3.181818181818182, 1],
        [3.090909090909091, 1],
        [3, 1],
        [2.909090909090909, 1],
        [2.818181818181818, 1],
        [2.727272727272728, 1],
        [2.636363636363636, 1],
        [2.545454545454545, 1],
        [2.454545454545455, 1],
        [2.363636363636364, 1],
        [2.272727272727272, 1],
        [2.181818181818182, 1],
        [2.090909090909091, 1],
        [2, 1],
        [2, 1.1],
        [2, 1.2],
        [2, 1.3],
        [2, 1.4],
        [2, 1.5],
        [2, 1.6],
        [2, 1.7],
        [2, 1.8],
        [2, 1.9],
        [2, 2],
        [2, 2.1],
        [2, 2.2],
        [2, 2.3],
        [2, 2.4],
        [2, 2.5],
        [2, 2.6],
        [2, 2.7],
        [2, 2.8],
        [2, 2.9],
        [2, 3],
        [2, 3.1],
        [2, 3.2],
        [2, 3.3],
        [2, 3.4],
        [2, 3.5],
        [2, 3.6],
        [2, 3.700000000000001],
        [2, 3.8],
        [2, 3.9],
        [2, 4],
        [2.090909090909091, 4],
        [2.181818181818182, 4],
        [2.272727272727272, 4],
        [2.363636363636364, 4],
        [2.454545454545455, 4],
        [2.545454545454545, 4],
        [2.636363636363636, 4],
        [2.727272727272728, 4],
        [2.818181818181818, 4],
        [2.909090909090909, 4],
        [3, 4],
        [3, 3.9],
        [3, 3.8],
        [3, 3.700000000000001],
        [3, 3.6],
        [3, 3.5],
        [3, 3.4],
        [3, 3.3],
        [3, 3.2],
        [3, 3.1],
        [3, 3],
        [3, 2.9],
        [3, 2.8],
        [3, 2.7],
        [3, 2.6],
        [3, 2.5],
        [3, 2.4],
        [3, 2.3],
        [3, 2.2],
        [3, 2.1],
        [3, 2]
    ])
    try:
        assert config["zoom_levels"][10]["process_area"].equals_exact(
            test_polygon,
            tolerance=0.001
            )
        print "OK: read bounds from input files"
    except:
        print "FAILED: read bounds from input files"
        print config["zoom_levels"][10]["process_area"].difference(test_polygon)
        raise

if __name__ == "__main__":
    main(sys.argv[1:])
