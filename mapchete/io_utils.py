#!/usr/bin/env python

import mapchete
from tilematrix import *

def read_raster(process, input_file):
    print process.tile
    print process.tile_pyramid.srs
    print input_file
