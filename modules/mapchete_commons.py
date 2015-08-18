#!/usr/bin/env python

import os
import sys


def create_and_clean_dirs(tile, parsed, extension):

    output_folder = parsed.output_folder[0]
    zoom, col, row = tile

    # Create output folders if not existing.
    # OGC Standard:
    # {TileMatrixSet}/{TileMatrix}/{TileRow}/{TileCol}.png
    basedir = output_folder
    zoomdir = os.path.join(basedir, str(zoom))
    rowdir = os.path.join(zoomdir, str(row))
    out_tile = tile_path(tile, parsed, extension)
    # Create output folders
    if not os.path.exists(basedir):
        os.makedirs(basedir)
    if not os.path.exists(zoomdir):
        os.makedirs(zoomdir)
    if not os.path.exists(rowdir):
        os.makedirs(rowdir)

    if os.path.exists(out_tile):
        os.remove(out_tile)


def tile_path(tile, parsed, extension):

    output_folder = parsed.output_folder[0]
    zoom, col, row = tile

    basedir = output_folder
    zoomdir = os.path.join(basedir, str(zoom))
    rowdir = os.path.join(zoomdir, str(row))
    out_tile = os.path.join(rowdir, (str(col) + extension))

    return out_tile


def hillshade(array, azimuth, angle_altitude):
    # from http://geoexamples.blogspot.co.at/2014/03/shaded-relief-images-\
    # using-gdal-python.html
    from numpy import gradient
    from numpy import pi
    from numpy import arctan
    from numpy import arctan2
    from numpy import sin
    from numpy import cos
    from numpy import sqrt
    from numpy import zeros
    from numpy import uint8

    x, y = gradient(array)  
    slope = pi/2. - arctan(sqrt(x*x + y*y))  
    aspect = arctan2(-x, y)  
    azimuthrad = azimuth*pi / 180.  
    altituderad = angle_altitude*pi / 180.  
       
   
    shaded = sin(altituderad) * sin(slope)\
     + cos(altituderad) * cos(slope)\
     * cos(azimuthrad - aspect)  
    return 255*(shaded + 1)/2


class MapcheteConfig(object):

    def load_from_argparse(self, parsed):
        self.epsg = str(parsed.EPSG[0])
        self.zoom = parsed.zoom[0]
        self.output_folder = parsed.output_folder[0]
        self.profile = parsed.format[0]
        self.dtype = parsed.dtype
        self.metatiling = parsed.metatiling[0]
        self.parallel = parsed.parallel[0]
        self.bounds = parsed.bounds
        self.create_vrt = parsed.create_vrt
        self.debug = parsed.debug

    def load_from_yaml(self, yaml_process):
        self.epsg = str(yaml_process["EPSG"])
        self.output_folder = yaml_process["output_folder"]
        self.profile = yaml_process["profile"]
        self.metatiling = yaml_process["metatiling"]
        self.parallel = yaml_process["parallel"]
        self.create_vrt = yaml_process["create_vrt"]
