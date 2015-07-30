#!/usr/bin/env python

import sys
import os
import fiona
from shapely.geometry import *
from shapely.wkt import *

# import local modules independent from script location
scriptdir = os.path.dirname(os.path.realpath(__file__))
rootdir = os.path.split(scriptdir)[0]
sys.path.append(os.path.join(rootdir, 'modules'))
from tilematrix import *

ROUND = 10

def main(args):

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

    # tiles from Point
    point_location = os.path.join(testdata_directory, "point.geojson")
    tiled_out = os.path.join(outdata_directory, "point_tiles.geojson")
    zoom = 6
    testtile = [(69, 14)]
    with fiona.open(point_location) as point_file:
        point = shape(point_file[0]["geometry"])
        try:
            point_tile = wgs84.tiles_from_geom(point, zoom)
            assert point_tile == testtile
            print "Point OK"
        except:
            print "Point FAILED"
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
        col, row = point_tile[0]
        feature = {}
        feature['geometry'] = mapping(wgs84.tile_bounds(col, row, zoom))
        feature['properties'] = {}
        feature['properties']['col'] = col
        feature['properties']['row'] = row
        sink.write(feature)

    # tiles from MultiPoint
    multipoint_location = os.path.join(testdata_directory,
        "multipoint.geojson")
    tiled_out = os.path.join(outdata_directory, "multipoint_tiles.geojson")
    zoom = 9
    testtiles = [(553, 113), (558, 118)]
    with fiona.open(multipoint_location) as multipoint_file:
        multipoint = shape(multipoint_file[0]["geometry"])
        try:
            multipoint_tiles = wgs84.tiles_from_geom(multipoint, zoom)
            assert multipoint_tiles == testtiles
            print "MultiPoint OK"
        except:
            print "MultiPoint FAILED"
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
        for tile in multipoint_tiles:
            col, row = tile
            feature = {}
            feature['geometry'] = mapping(wgs84.tile_bounds(col, row, zoom))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)

    # tiles from LineString
    linestring_location = os.path.join(testdata_directory,
        "linestring.geojson")
    tiled_out = os.path.join(outdata_directory, "linestring_tiles.geojson")
    zoom = 6
    testtiles = [(66, 14), (67, 14), (68, 14), (69, 14), (70, 14), (70, 15),
        (71, 15), (71, 16), (72, 16), (73, 15), (73, 16), (74, 15)]
    with fiona.open(linestring_location) as linestring_file:
        linestring = shape(linestring_file[0]["geometry"])
        try:
            linestring_tiles = wgs84.tiles_from_geom(linestring, zoom)
            assert linestring_tiles == testtiles
            print "LineString OK"
        except:
            print "LineString FAILED"
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
        for tile in linestring_tiles:
            col, row = tile
            feature = {}
            feature['geometry'] = mapping(wgs84.tile_bounds(col, row, zoom))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)

    # tiles from MultiLineString
    multilinestring_location = os.path.join(testdata_directory,
        "multilinestring.geojson")
    tiled_out = os.path.join(outdata_directory,
        "multilinestring_tiles.geojson")
    zoom = 6
    testtiles = [(66, 14), (67, 14), (68, 14), (69, 14), (70, 14), (70, 15),
       (71, 15), (71, 16), (72, 16), (73, 15), (73, 16), (74, 15), (74, 21),
       (74, 22), (74, 24), (74, 25), (74, 28), (74, 29), (75, 20), (75, 21),
       (75, 22), (75, 23), (75, 24), (75, 25), (75, 26), (75, 27), (75, 28),
       (75, 29), (75, 30), (75, 31), (76, 25)]
    with fiona.open(multilinestring_location) as multilinestring_file:
        multilinestring = shape(multilinestring_file[0]["geometry"])
        try:
            multilinestring_tiles = wgs84.tiles_from_geom(multilinestring,
                zoom)
            assert multilinestring_tiles == testtiles
            print "MultiLineString OK"
        except:
            print "MultiLineString FAILED"
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
        for tile in multilinestring_tiles:
            col, row = tile
            feature = {}
            feature['geometry'] = mapping(wgs84.tile_bounds(col, row, zoom))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)

    # tiles from Polygon
    polygon_location = os.path.join(testdata_directory,
        "polygon.geojson")
    tiled_out = os.path.join(outdata_directory, "polygon_tiles.geojson")
    zoom = 8
    testtiles = [(269, 60), (269, 61), (270, 60), (270, 61), (271, 60),
        (271, 61), (272, 60), (272, 61), (273, 60), (273, 61), (274, 59),
        (274, 60), (274, 61), (275, 58), (275, 59), (275, 60), (275, 61),
        (276, 58), (276, 59), (276, 60), (276, 61), (276, 62), (277, 58),
        (277, 59), (277, 60), (277, 61), (278, 58), (278, 59), (278, 60),
        (278, 61), (279, 58), (279, 59), (279, 60), (279, 61), (280, 58),
        (280, 59), (280, 60)]
    with fiona.open(polygon_location) as polygon_file:
        polygon = shape(polygon_file[0]["geometry"])
        polygon_tiles = wgs84.tiles_from_geom(polygon, zoom)
        try:
            assert polygon_tiles == testtiles
            print "Polygon OK"
        except:
            print "Polygon FAILED"
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
        for tile in polygon_tiles:
            col, row = tile
            feature = {}
            feature['geometry'] = mapping(wgs84.tile_bounds(col, row, zoom))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)

    # tiles from MultiPolygon
    multipolygon_location = os.path.join(testdata_directory,
        "multipolygon.geojson")
    tiled_out = os.path.join(outdata_directory, "multipolygon_tiles.geojson")
    zoom = 10
    testtiles = [(1081, 243), (1081, 244), (1081, 245), (1082, 242),
        (1082, 243), (1082, 244), (1082, 245), (1083, 241), (1083, 242),
        (1083, 243), (1083, 244), (1083, 245), (1084, 241), (1084, 242),
        (1084, 243), (1084, 244), (1084, 245), (1085, 241), (1085, 242),
        (1085, 243), (1085, 244), (1085, 245), (1086, 241), (1086, 242),
        (1086, 243), (1086, 244), (1086, 245), (1087, 242), (1087, 243),
        (1087, 244), (1087, 245), (1088, 241), (1088, 242), (1088, 243),
        (1088, 244), (1089, 241), (1089, 242), (1089, 243), (1089, 244),
        (1090, 241), (1090, 242), (1090, 243), (1090, 244), (1091, 241),
        (1091, 242), (1091, 243), (1091, 244), (1092, 241), (1092, 242),
        (1092, 243), (1092, 244), (1093, 240), (1093, 241), (1093, 242),
        (1093, 244), (1093, 245), (1094, 240), (1094, 241), (1094, 242),
        (1094, 243), (1094, 244), (1094, 245), (1094, 246), (1095, 240),
        (1095, 241), (1095, 242), (1095, 243), (1095, 244), (1095, 245),
        (1095, 246), (1096, 241), (1096, 244), (1096, 245), (1096, 246),
        (1097, 245), (1097, 246)]
    with fiona.open(multipolygon_location) as multipolygon_file:
        multipolygon = shape(multipolygon_file[0]["geometry"])
        multipolygon_tiles = wgs84.tiles_from_geom(multipolygon, zoom)
        try:
            assert multipolygon_tiles == testtiles
            print "MultiPolygon OK"
        except:
            print "MultiPolygon FAILED"
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
        for tile in multipolygon_tiles:
            col, row = tile
            feature = {}
            feature['geometry'] = mapping(wgs84.tile_bounds(col, row, zoom))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)

if __name__ == "__main__":
    main(sys.argv[1:])