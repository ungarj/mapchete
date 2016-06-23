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
    mapchete = Mapchete(MapcheteConfig(mapchete_file))

    dummy1_abspath = os.path.join(scriptdir, "testdata/dummy1.tif")
    dummy2_abspath = os.path.join(scriptdir, "testdata/dummy2.tif")

    # Validate configuration constructor
    ## basic run through
    try:
        config = mapchete.config
        print "OK: basic configuraiton constructor run through"
    except:
        print "FAILED: basic configuraiton constructor run through"
        raise

    try:
        # Check configuration at zoom level 5
        zoom5 = config.at_zoom(5)
        input_files = zoom5["input_files"]
        assert input_files["file1"] == None
        assert input_files["file2"] == dummy2_abspath
        assert zoom5["some_integer_parameter"] == 12
        assert zoom5["some_float_parameter"] == 5.3
        assert zoom5["some_string_parameter"] == "string1"
        assert zoom5["some_bool_parameter"] == True

        # Check configuration at zoom level 11
        zoom11 = config.at_zoom(11)
        input_files = zoom11["input_files"]
        assert input_files["file1"] == dummy1_abspath
        assert input_files["file2"] == dummy2_abspath
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

    ## read zoom level from config file
    mapchete_file = os.path.join(scriptdir, "testdata/zoom.mapchete")
    config = Mapchete(MapcheteConfig(mapchete_file)).config
    try:
        assert 5 in config.zoom_levels
        print "OK: read zoom level from config file"
    except:
        print "FAILED: read zoom level from config file"
        print mapchete_file
        raise
    ## read min/max zoom levels from config file
    mapchete_file = os.path.join(scriptdir, "testdata/minmax_zoom.mapchete")
    config = Mapchete(MapcheteConfig(mapchete_file)).config
    try:
        for zoom in [7, 8, 9, 10]:
            assert zoom in config.zoom_levels
        print "OK: read  min/max zoom levels from config file"
    except:
        print "FAILED: read  min/max zoom levels from config file"
        raise
    ## zoom levels override
    mapchete_file = os.path.join(scriptdir, "testdata/minmax_zoom.mapchete")
    config = Mapchete(MapcheteConfig(mapchete_file, zoom=[1, 4])).config
    try:
        for zoom in [1, 2, 3, 4]:
            assert zoom in config.zoom_levels
        print "OK: zoom levels override"
    except:
        print "FAILED: zoom levels override"
        raise
    ## read bounds from config file
    mapchete_file = os.path.join(scriptdir, "testdata/zoom.mapchete")
    config = Mapchete(MapcheteConfig(mapchete_file)).config
    try:
        test_polygon = Polygon([
            [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]
            ])
        assert config.process_area(5).equals(test_polygon)
        print "OK: read bounds from config file"
    except:
        print "FAILED: read bounds from config file"
        print config.process_area(5), test_polygon
        raise
    ## override bounds
    mapchete_file = os.path.join(scriptdir, "testdata/zoom.mapchete")
    config = Mapchete(MapcheteConfig(
        mapchete_file,
        bounds=[3, 2, 3.5, 1.5]
        )).config
    try:
        test_polygon = Polygon([
            [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]
            ])
        assert config.process_area(5).equals(test_polygon)
        print "OK: override bounds"
    except:
        print "FAILED: override bounds"
        print config.process_area(5)
        raise
    ## read bounds from input files
    mapchete_file = os.path.join(scriptdir, "testdata/files_bounds.mapchete")
    config = Mapchete(MapcheteConfig(mapchete_file)).config
    try:
        test_polygon = Polygon(
        [[3, 2], [4, 2], [4, 1], [3, 1], [2, 1], [2, 4], [3, 4], [3, 2]]
        )
        assert config.process_area(10).equals(test_polygon)
        print "OK: read bounds from input files"
    except:
        print "FAILED: read bounds from input files"
        print config.process_area(10), test_polygon
        raise
    ## read .mapchete files as input files
    mapchete_file = os.path.join(scriptdir, "testdata/mapchete_input.mapchete")
    config = Mapchete(MapcheteConfig(mapchete_file)).config
    area = config.process_area(5)
    testpolygon = "POLYGON ((3 2, 3.5 2, 3.5 1.5, 3 1.5, 3 1, 2 1, 2 4, 3 4, 3 2))"
    try:
        assert area.equals(loads(testpolygon))
        print "OK: read bounding box from .mapchete subfile"
    except:
        print "FAILED: read bounding box from .mapchete subfile"
        raise


    from mapchete.formats import MapcheteOutputFormat
    import yaml

    mapchete_file = os.path.join(scriptdir, "testdata/gtiff.mapchete")
    with open(mapchete_file, "r") as config_file:
        raw_config = yaml.load(config_file.read())
    out_format = MapcheteOutputFormat(raw_config["output"])


if __name__ == "__main__":
    main(sys.argv[1:])
