#!/usr/bin/env python

import sys
import os
import fiona
from shapely.geometry import *
from shapely.wkt import loads
from tilematrix import *


ROUND = 10

def main(args):

    scriptdir = os.path.dirname(os.path.realpath(__file__))
    testdata_directory = os.path.join(scriptdir, "testdata")
    outdata_directory = os.path.join(testdata_directory, "out")
    wgs84 = TileMatrix("4326")

    # tiles per zoomlevel
    try:
        tiles = wgs84.tiles_for_zoom(5)
        assert tiles == (64, 32)
        print "tiles per zoomlevel OK"
    except:
        print "tiles per zoomlevel FAILED"

    # top left coordinate
    try:
        tl = wgs84.top_left_tile_coords(3, 3, 5)
        print tl
        assert tl == (-163.125, 73.125)
        print "top left coordinate OK"
    except:
        print "top left coordinate FAILED"


    # tile boundaries
    try:
        boundaries = wgs84.tile_bounds(3, 3, 5)
        testpolygon = Polygon([[-163.125, 73.125], [-157.5, 73.125],
            [-157.5, 67.5], [-163.125, 67.5], [-163.125, 73.125]])
        assert boundaries.equals(testpolygon)
        print "tile boundaries OK"
    except:
        print "tile boundaries FAILED"
    

    # test bounding box
    bbox_location = os.path.join(testdata_directory, "bbox.geojson")
    tiled_out = os.path.join(outdata_directory, "bbox_tiles.geojson")
    zoom = 5
    testtiles = [(33, 5), (33, 6), (33, 7), (33, 8), (33, 9), (33, 10),
        (34, 5), (34, 6), (34, 7), (34, 8), (34, 9), (34, 10), (35, 5),
        (35, 6), (35, 7), (35, 8), (35, 9), (35, 10), (36, 5), (36, 6),
        (36, 7), (36, 8), (36, 9), (36, 10), (37, 5), (37, 6), (37, 7),
        (37, 8), (37, 9), (37, 10), (38, 5), (38, 6), (38, 7), (38, 8),
        (38, 9), (38, 10), (39, 5), (39, 6), (39, 7), (39, 8), (39, 9),
        (39, 10), (40, 5), (40, 6), (40, 7), (40, 8), (40, 9), (40, 10),
        (41, 5), (41, 6), (41, 7), (41, 8), (41, 9), (41, 10)]
    with fiona.open(bbox_location) as bbox_file:
        try:
            bbox_tiles = wgs84.tiles_from_bbox(bbox_file, zoom)
            assert bbox_tiles == testtiles
            print "bounding box OK"
        except:
            print "bounding box FAILED"
    ## write debug output
    schema = {
        'geometry': 'Polygon',
        'properties': {'col': 'int', 'row': 'int'}
    }
    try:
        os.remove(tiled_out)
    except:
        pass
    with fiona.open(tiled_out, 'w', 'GeoJSON', schema) as sink:
        for tile in bbox_tiles:
            col, row = tile
            feature = {}
            feature['geometry'] = mapping(wgs84.tile_bounds(col, row, zoom))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)

    # tiles from point
    point_location = os.path.join(testdata_directory, "point.geojson")
    tiled_out = os.path.join(outdata_directory, "point_tiles.geojson")
    zoom = 6
    testtile = [(69, 14)]
    with fiona.open(point_location) as point_file:
        point = shape(point_file[0]["geometry"])
        try:
            point_tile = wgs84.tiles_from_geom(point, zoom)
            assert bbox_tiles == testtiles
            print "Point OK"
        except:
            print "Point FAILED"
    ## write debug output
    schema = {
        'geometry': 'Point',
        'properties': {'col': 'int', 'row': 'int'}
    }
    try:
        os.remove(tiled_out)
    except:
        pass
    with fiona.open(tiled_out, 'w', 'GeoJSON', schema) as sink:
        col, row = point_tile[0]
        feature = {}
        feature['geometry'] = mapping(wgs84.tile_bounds(col, row, zoom))
        feature['properties'] = {}
        feature['properties']['col'] = col
        feature['properties']['row'] = row
        sink.write(feature)

    #print "tiles from geometry: %s" %(tiles_from_geom(wgs84, polygon, 2))

if __name__ == "__main__":
    main(sys.argv[1:])