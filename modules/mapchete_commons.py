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