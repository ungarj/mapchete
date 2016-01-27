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
    process_file = os.path.join(scriptdir, "merge_dems.py")
    config_yaml = os.path.join(scriptdir, "merge_dems.yaml")

    config = MapcheteConfig(config_yaml)

    # Check configuration at zoom level 5
    zoom5 = config.at_zoom(5)
    assert zoom5["primary_dems"]["ASTER"] == "/path/to/aster"
    assert zoom5["primary_dems"]["SRTM30"] == "/path/to/srtm30"
    assert zoom5['primary_dems']['EUDEM'] == {}
    assert zoom5["secondary_dems"]["local_DEM1"] == "/path/to/local/dem1"
    assert zoom5["secondary_dems"]["local_DEM2"] == "/path/to/local/dem2"
    assert zoom5["secondary_dems"]["local_DEM3"] == "/path/to/local/dem3"
    assert zoom5["secondary_dems"]["local_DEM4"] == "/path/to/local/dem4"
    assert zoom5["base_level"] == 12
    assert zoom5["resampling"] == "nearest"
    assert zoom5["generalization"] == 5
    assert zoom5["output_file"] == "/path/to/gpkg"
    assert zoom5["output_profile"] == "EOGeopackage"

    # Check configuration at zoom level 11
    zoom11 = config.at_zoom(11)
    assert zoom11["primary_dems"]["ASTER"] == "/path/to/aster"
    assert zoom11["primary_dems"]["SRTM30"] == "/path/to/srtm30"
    assert zoom11['primary_dems']['EUDEM'] == "/path/to/eudem"
    assert zoom11["secondary_dems"]["local_DEM1"] == "/path/to/local/dem1"
    assert zoom11["secondary_dems"]["local_DEM2"] == "/path/to/local/dem2"
    assert zoom11["secondary_dems"]["local_DEM3"] == "/path/to/local/dem3"
    assert zoom11["secondary_dems"]["local_DEM4"] == "/path/to/local/dem4"
    assert zoom11["base_level"] == 12
    assert zoom11['resampling'] == 'bilinear'
    assert zoom11["generalization"] == 5
    assert zoom11["output_file"] == "/path/to/gpkg"
    assert zoom11["output_profile"] == "EOGeopackage"


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
