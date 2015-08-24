#!/usr/bin/env python

import sys
import os
import argparse
import fiona
from shapely.geometry import *
from shapely.wkt import *
from shapely.ops import cascaded_union
import math

# import local modules independent from script location
scriptdir = os.path.dirname(os.path.realpath(__file__))
rootdir = os.path.split(scriptdir)[0]
sys.path.append(os.path.join(rootdir, 'modules'))
from tilematrix import *
from tilematrix_io import *
from mapchete_commons import *

ROUND = 10

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true")
    parsed = parser.parse_args(args)
    global debug
    debug = parsed.debug

    testdata_directory = os.path.join(scriptdir, "testdata")
    outdata_directory = os.path.join(testdata_directory, "out")
    wgs84 = TileMatrix("4326")
    wgs84.set_format("GTiff", dtype="uInt16")
    wgs84_meta = MetaTileMatrix(wgs84, 23)
    print wgs84_meta.format.profile["dtype"]

    # tilematrix
    #===========

    # tiles per zoomlevel
    try:
        tiles = wgs84.tiles_per_zoom(5)
        assert tiles == (64, 32)
        print "tiles per zoomlevel OK"
    except:
        print "tiles per zoomlevel FAILED"
        raise


    # top left coordinate
    try:
        tl = wgs84.top_left_tile_coords(5, 3, 3)
        assert tl == (-163.125, 73.125)
        print "top left coordinate OK"
    except:
        print "top left coordinate FAILED"
        raise


    # tile bounding box
    try:
        bbox = wgs84.tile_bbox(5, 3, 3)
        testpolygon = Polygon([[-163.125, 73.125], [-157.5, 73.125],
            [-157.5, 67.5], [-163.125, 67.5], [-163.125, 73.125]])
        assert bbox.equals(testpolygon)
        print "tile bounding box OK"
    except:
        print "tile bounding box FAILED"
        raise


    # tile bounding box with buffer
    try:
        bbox = wgs84.tile_bbox(5, 3, 3, 1)
        testpolygon = Polygon([[-163.14697265625, 73.14697265625],
            [-157.47802734375, 73.14697265625],
            [-157.47802734375, 67.47802734375],
            [-163.14697265625, 67.47802734375],
            [-163.14697265625, 73.14697265625]])
        assert bbox.equals(testpolygon)
        print "tile bounding box with buffer OK"
    except:
        print "tile bounding box with buffer FAILED"
        raise
    

    # tile bounds
    try:
        bounds = wgs84.tile_bounds(5, 3, 3)
        testbounds = (-163.125, 67.5, -157.5, 73.125)
        assert bounds == testbounds
        print "tile bounds OK"
    except:
        print "tile bounds FAILED"
        raise


    # tile bounds buffer
    try:
        bounds = wgs84.tile_bounds(5, 3, 3, 1)
        testbounds = (-163.14697265625, 67.47802734375, -157.47802734375,
            73.14697265625)
        assert bounds == testbounds
        print "tile bounds with buffer OK"
    except:
        print "tile bounds wigh buffer FAILED"
        raise


    # test bounding box
    bbox_location = os.path.join(testdata_directory, "bbox.geojson")
    tiled_out = os.path.join(outdata_directory, "bbox_tiles.geojson")
    zoom = 5
    testtiles = [(5, 5, 33), (5, 6, 33), (5, 7, 33), (5, 8, 33), (5, 9, 33), (5, 10, 33),
        (5, 5, 34), (5, 6, 34), (5, 7, 34), (5, 8, 34), (5, 9, 34), (5, 10, 34), (5, 5, 35),
        (5, 6, 35), (5, 7, 35), (5, 8, 35), (5, 9, 35), (5, 10, 35),(5, 5, 36), (5, 6, 36),
        (5, 7, 36), (5, 8, 36), (5, 9, 36), (5, 10, 36), (5, 5, 37), (5, 6, 37), (5, 7, 37),
        (5, 8, 37), (5, 9, 37), (5, 10, 37),(5, 5, 38), (5, 6, 38), (5, 7, 38), (5, 8, 38),
        (5, 9, 38), (5, 10, 38), (5, 5, 39), (5, 6, 39), (5, 7, 39), (5, 8, 39), (5, 9, 39),
        (5, 10, 39), (5, 5, 40), (5, 6, 40), (5, 7, 40), (5, 8, 40), (5, 9, 40), (5, 10, 40),
        (5, 5, 41), (5, 6, 41), (5, 7, 41), (5, 8, 41), (5, 9, 41), (5, 10, 41)]
    with fiona.open(bbox_location) as bbox_file:
        try:
            bbox_tiles = wgs84.tiles_from_bbox(bbox_file, zoom)
            assert len(set(bbox_tiles).symmetric_difference(set(testtiles))) == 0
            print "bounding box OK"
        except:
            print "bounding box FAILED"
            raise
    if debug:
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
                zoom, row, col = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, row, col))
                feature['properties'] = {}
                feature['properties']['col'] = col
                feature['properties']['row'] = row
                sink.write(feature)


    # tiles from Point
    point_location = os.path.join(testdata_directory, "point.geojson")
    tiled_out = os.path.join(outdata_directory, "point_tiles.geojson")
    zoom = 6
    testtile = [(6, 14, 69)]
    with fiona.open(point_location) as point_file:
        point = shape(point_file[0]["geometry"])
        try:
            point_tile = wgs84.tiles_from_geom(point, zoom)
            assert point_tile == testtile
            print "Point OK"
        except:
            print "Point FAILED"
            raise
    if debug:
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
            zoom, row, col = point_tile[0]
            feature = {}
            feature['geometry'] = mapping(wgs84.tile_bbox(zoom, row, col))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)

    # tiles from MultiPoint
    multipoint_location = os.path.join(testdata_directory,
        "multipoint.geojson")
    tiled_out = os.path.join(outdata_directory, "multipoint_tiles.geojson")
    zoom = 9
    testtiles = [(9, 113, 553), (9, 118, 558)]
    with fiona.open(multipoint_location) as multipoint_file:
        multipoint = shape(multipoint_file[0]["geometry"])
        try:
            multipoint_tiles = wgs84.tiles_from_geom(multipoint, zoom)
            assert multipoint_tiles == testtiles
            print "MultiPoint OK"
        except:
            print "MultiPoint FAILED"
            print multipoint_tiles
            print testtiles
            raise
    if debug:
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
                zoom, row, col = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, row, col))
                feature['properties'] = {}
                feature['properties']['col'] = col
                feature['properties']['row'] = row
                sink.write(feature)

    # tiles from LineString
    linestring_location = os.path.join(testdata_directory,
        "linestring.geojson")
    tiled_out = os.path.join(outdata_directory, "linestring_tiles.geojson")
    zoom = 6
    testtiles = [(6, 14, 66), (6, 14, 67), (6, 14, 68), (6, 14, 69), (6, 14, 70), (6, 15, 70),
        (6, 15, 71), (6, 16, 71), (6, 16, 72), (6, 15, 73), (6, 16, 73), (6, 15, 74)]
    with fiona.open(linestring_location) as linestring_file:
        linestring = shape(linestring_file[0]["geometry"])
        try:
            linestring_tiles = wgs84.tiles_from_geom(linestring, zoom)
            assert len(set(linestring_tiles).symmetric_difference(set(testtiles))) == 0
            print "LineString OK"
        except:
            print "LineString FAILED"
            raise
    if debug:
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
                zoom, row, col = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, row, col))
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
    testtiles = [(6, 14, 66), (6, 14, 67), (6, 14, 68), (6, 14, 69), (6, 14, 70), (6, 15, 70),
       (6, 15, 71), (6, 16, 71), (6, 16, 72), (6, 15, 73), (6, 16, 73), (6, 15, 74), (6, 21, 74),
       (6, 22, 74), (6, 24, 74), (6, 25, 74), (6, 28, 74), (6, 29, 74), (6, 20, 75), (6, 21, 75),
       (6, 22, 75), (6, 23, 75), (6, 24, 75), (6, 25, 75), (6, 26, 75), (6, 27, 75), (6, 28, 75),
       (6, 29, 75), (6, 30, 75), (6, 31, 75), (6, 25, 76)]
    with fiona.open(multilinestring_location) as multilinestring_file:
        multilinestring = shape(multilinestring_file[0]["geometry"])
        try:
            multilinestring_tiles = wgs84.tiles_from_geom(multilinestring,
                zoom)
            assert len(set(multilinestring_tiles).symmetric_difference(set(testtiles))) == 0
            print "MultiLineString OK"
        except:
            print "MultiLineString FAILED"
            raise
    if debug:
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
                zoom, row, col = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, row, col))
                feature['properties'] = {}
                feature['properties']['col'] = col
                feature['properties']['row'] = row
                sink.write(feature)

    # tiles from Polygon
    polygon_location = os.path.join(testdata_directory,
        "polygon.geojson")
    tiled_out = os.path.join(outdata_directory, "polygon_tiles.geojson")
    zoom = 8
    testtiles = [(8, 60, 269), (8, 61, 269), (8, 60, 270), (8, 61, 270), (8, 60, 271),
        (8, 61, 271), (8, 60, 272), (8, 61, 272), (8, 60, 273), (8, 61, 273), (8, 59, 274),
        (8, 60, 274), (8, 61, 274), (8, 58, 275), (8, 59, 275), (8, 60, 275), (8, 61, 275),
        (8, 58, 276), (8, 59, 276), (8, 60, 276), (8, 61, 276), (8, 62, 276), (8, 58, 277),
        (8, 59, 277), (8, 60, 277), (8, 61, 277), (8, 58, 278), (8, 59, 278), (8, 60, 278),
        (8, 61, 278), (8, 58, 279), (8, 59, 279), (8, 60, 279), (8, 61, 279), (8, 58, 280),
        (8, 59, 280), (8, 60, 280)]
    with fiona.open(polygon_location) as polygon_file:
        polygon = shape(polygon_file[0]["geometry"])
        polygon_tiles = wgs84.tiles_from_geom(polygon, zoom)
        try:
            assert len(set(polygon_tiles).symmetric_difference(set(testtiles))) == 0
            print "Polygon OK"
        except:
            print "Polygon FAILED"
            raise
    if debug:
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
                zoom, row, col = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, row, col))
                feature['properties'] = {}
                feature['properties']['col'] = col
                feature['properties']['row'] = row
                sink.write(feature)

    # tiles from MultiPolygon
    multipolygon_location = os.path.join(testdata_directory,
        "multipolygon.geojson")
    tiled_out = os.path.join(outdata_directory, "multipolygon_tiles.geojson")
    zoom = 10
    testtiles = [(10, 243, 1081), (10, 244, 1081), (10, 245, 1081), (10, 242, 1082),
        (10, 243, 1082), (10, 244, 1082), (10, 245, 1082), (10, 241, 1083), (10, 242, 1083),
        (10, 243, 1083), (10, 244, 1083), (10, 245, 1083), (10, 241, 1084), (10, 242, 1084),
        (10, 243, 1084), (10, 244, 1084), (10, 245, 1084), (10, 241, 1085), (10, 242, 1085),
        (10, 243, 1085), (10, 244, 1085), (10, 245, 1085), (10, 241, 1086), (10, 242, 1086),
        (10, 243, 1086), (10, 244, 1086), (10, 245, 1086), (10, 242, 1087), (10, 243, 1087),
        (10, 244, 1087), (10, 245, 1087), (10, 241, 1088), (10, 242, 1088), (10, 243, 1088),
        (10, 244, 1088), (10, 241, 1089), (10, 242, 1089), (10, 243, 1089), (10, 244, 1089),
        (10, 241, 1090), (10, 242, 1090), (10, 243, 1090), (10, 244, 1090), (10, 241, 1091),
        (10, 242, 1091), (10, 243, 1091), (10, 244, 1091), (10, 241, 1092), (10, 242, 1092),
        (10, 243, 1092), (10, 244, 1092), (10, 240, 1093), (10, 241, 1093), (10, 242, 1093),
        (10, 244, 1093), (10, 245, 1093), (10, 240, 1094), (10, 241, 1094), (10, 242, 1094),
        (10, 243, 1094), (10, 244, 1094), (10, 245, 1094), (10, 246, 1094), (10, 240, 1095),
        (10, 241, 1095), (10, 242, 1095), (10, 243, 1095), (10, 244, 1095), (10, 245, 1095),
        (10, 246, 1095), (10, 241, 1096), (10, 244, 1096), (10, 245, 1096), (10, 246, 1096),
        (10, 245, 1097), (10, 246, 1097)]
    with fiona.open(multipolygon_location) as multipolygon_file:
        multipolygon = shape(multipolygon_file[0]["geometry"])
        multipolygon_tiles = wgs84.tiles_from_geom(multipolygon, zoom)
        try:
            assert len(set(multipolygon_tiles).symmetric_difference(set(testtiles))) == 0
            print "MultiPolygon OK"
        except:
            print "MultiPolygon FAILED"
            raise
    if debug:
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
                zoom, row, col = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, row, col))
                feature['properties'] = {}
                feature['properties']['col'] = col
                feature['properties']['row'] = row
                sink.write(feature)


    if debug:
        # writing output test files
        col, row = 2, 2
        zoom = 5
        metatiling = 2
        wgs84_meta = MetaTileMatrix(wgs84, metatiling)
        antimeridian_location = os.path.join(testdata_directory,
            "antimeridian.geojson")
        with fiona.open(antimeridian_location) as antimeridian_file:
            geometries = []
            for feature in antimeridian_file:
                geometries.append(shape(feature["geometry"]))
        antimeridian = cascaded_union(geometries)
        print "top left tile coordinates:"
        print "metatilematrix: %s" %([wgs84_meta.top_left_tile_coords(zoom, row, col)])
        print "tile bounding box"
        print "metatilematrix: %s" %([mapping(wgs84.tile_bbox(zoom, row, col))])
        print "tile bounds"
        print "metatilematrix: %s" %([wgs84_meta.tile_bounds(zoom, row, col)])
        print "tiles from bbox"
        #print "metatilematrix: %s" %([wgs84_meta.tiles_from_bbox(antimeridian, zoom)])
        print "tiles from geometry"
    
        ## write debug output
        tiled_out = os.path.join(outdata_directory, "tile_antimeridian_tiles.geojson")
        schema = {
            'geometry': 'Polygon',
            'properties': {'col': 'int', 'row': 'int'}
        }
        try:
            os.remove(tiled_out)
        except:
            pass
        tiles = wgs84.tiles_from_geom(antimeridian, zoom)
        print "tilematrix: %s" %(len(tiles))
        with fiona.open(tiled_out, 'w', 'GeoJSON', schema) as sink:
            for tile in tiles:
                zoom, row, col = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, row, col))
                feature['properties'] = {}
                feature['properties']['col'] = col
                feature['properties']['row'] = row
                sink.write(feature)
        ## write debug output
        metatiled_out = os.path.join(outdata_directory, "metatile_antimeridian_tiles.geojson")
        schema = {
            'geometry': 'Polygon',
            'properties': {'col': 'int', 'row': 'int'}
        }
        try:
            os.remove(metatiled_out)
        except:
            pass
        metatiles = wgs84_meta.tiles_from_geom(antimeridian, zoom)
        print "metatilematrix: %s" %(len(metatiles))
        with fiona.open(metatiled_out, 'w', 'GeoJSON', schema) as sink:
            for metatile in metatiles:
                zoom, row, col = metatile
                feature = {}
                feature['geometry'] = mapping(wgs84_meta.tile_bbox(zoom, row, col))
                feature['properties'] = {}
                feature['properties']['col'] = col
                feature['properties']['row'] = row
                sink.write(feature)

    
    for metatiling in range(1, 21):
        wgs84_meta = MetaTileMatrix(wgs84, metatiling)
        for zoom in range(22):
            tilematrix_tiles = wgs84.tiles_per_zoom(zoom)
            metatilematrix_tiles = wgs84_meta.tiles_per_zoom(zoom)
            we_metatiles = metatilematrix_tiles[0]
            we_control = int(math.ceil(float(tilematrix_tiles[0])/float(metatiling)))
            ns_metatiles = metatilematrix_tiles[1]
            ns_control = int(math.ceil(float(tilematrix_tiles[1])/float(metatiling)))
            try:
                assert we_metatiles == we_control
                assert ns_metatiles == ns_control
            except:
                print "ERROR: metatile number"
                print metatiling, zoom
                print we_metatiles, we_control
                print ns_metatiles, ns_control
                raise


    for metatiling in range(1, 21):
        wgs84_meta = MetaTileMatrix(wgs84, metatiling)
        for zoom in range(21):
            # check tuple
            assert len(wgs84_meta.tiles_per_zoom(zoom)) == 2

            # check metatile size
            metatile_wesize, metatile_nssize = wgs84_meta.tilesize_per_zoom(zoom)
            metatile_wesize = round(metatile_wesize, ROUND)
            metatile_nssize = round(metatile_nssize, ROUND)
            ## assert metatile size equals tilematrix width and height at zoom 0
            if zoom == 0:
                try:
                    if metatiling == 1:
                        assert (metatile_wesize * 2) == wgs84.wesize
                    else:
                        assert metatile_wesize == wgs84.wesize
                    assert metatile_nssize == wgs84.nssize
                except:
                    print metatiling, zoom
                    print "ERROR: zoom 0 metatile size not correct"
                    print metatile_wesize, wgs84.wesize
                    print metatile_nssize, wgs84.nssize
            ## assert metatile size within tilematrix bounds
            try:
                assert (metatile_wesize > 0.0) and (metatile_wesize <= wgs84.wesize)
                assert (metatile_nssize > 0.0) and (metatile_nssize <= wgs84.nssize)
            except:
                print "ERROR: metatile size"
                print zoom
                print metatile_wesize, wgs84_meta.wesize
                print metatile_nssize, wgs84_meta.wesize
            ## calculate control size from tiles

            tile_wesize, tile_nssize = wgs84.tilesize_per_zoom(zoom)
            we_control_size = round(tile_wesize * float(metatiling), ROUND)
            if we_control_size > wgs84.wesize:
                we_control_size = wgs84.wesize
            ns_control_size = round(tile_nssize * float(metatiling), ROUND)

            if ns_control_size > wgs84.nssize:
                ns_control_size = wgs84.nssize
            try:
                assert metatile_wesize == we_control_size
                assert metatile_nssize == ns_control_size
            except:
                print "ERROR: metatile size and control sizes"
                print metatiling, zoom
                print metatile_wesize, we_control_size
                print metatile_nssize, ns_control_size

            # check metatile pixelsize (resolution)
            try:
                assert round(wgs84.pixelsize(zoom), ROUND) == round(wgs84_meta.pixelsize(zoom), ROUND)
            except:
                print "ERROR: metatile pixel size"
                print zoom, metatiling
                print wgs84_meta.tilesize_per_zoom(zoom), float(wgs84_meta.px_per_tile)
                print round((wgs84_meta.tilesize_per_zoom(zoom)[0] / float(wgs84_meta.px_per_tile)), ROUND)
                print round(wgs84.pixelsize(zoom), ROUND), round(wgs84_meta.pixelsize(zoom), ROUND)


    if debug:
        fiji_borders = os.path.join(testdata_directory, "fiji.geojson")
        with fiona.open(fiji_borders, "r") as fiji:
            geometries = []
            for feature in fiji:
                geometry = shape(feature['geometry'])
                geometries.append(geometry)
        union = cascaded_union(geometries)
        # tiles
        fiji_tiles = os.path.join(outdata_directory, "fiji_tiles.geojson")
        schema = {
            'geometry': 'Polygon',
            'properties': {'col': 'int', 'row': 'int'}
        }
        try:
            os.remove(fiji_tiles)
        except:
            pass
        metatiling = 5
        zoom = 10
        tiles = wgs84.tiles_from_geom(union, zoom)
        with fiona.open(fiji_tiles, 'w', 'GeoJSON', schema) as sink:
            for tile in tiles:
                zoom, row, col = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, row, col))
                feature['properties'] = {}
                feature['properties']['col'] = col
                feature['properties']['row'] = row
                sink.write(feature)
    
        # metatiles
        fiji_metatiles = os.path.join(outdata_directory, "fiji_metatiles.geojson")
        schema = {
            'geometry': 'Polygon',
            'properties': {'col': 'int', 'row': 'int'}
        }
        try:
            os.remove(fiji_metatiles)
        except:
            pass
        wgs84_meta = MetaTileMatrix(wgs84, metatiling)
        metatiles = wgs84_meta.tiles_from_geom(union, zoom)
        with fiona.open(fiji_metatiles, 'w', 'GeoJSON', schema) as sink:
            for metatile in metatiles:
                zoom, row, col = metatile
                feature = {}
                feature['geometry'] = mapping(wgs84_meta.tile_bbox(zoom, row, col))
                feature['properties'] = {}
                feature['properties']['col'] = col
                feature['properties']['row'] = row
                sink.write(feature)


    # tilematrix-io
    #==============

#    raster_file = "../../terrain/aster.tif"
#    tileindex = (1077, 268, 10)
#
#    out_tile = "scaled.tif"
#    try:
#        os.remove(out_tile)
#    except:
#        pass
#    metadata, data = read_raster_window(raster_file, wgs84, tileindex, pixelbuffer=1)
#    with rasterio.open(out_tile, 'w', **metadata) as destination:
#        destination.write_band(1, data)
#
#    out_tile = "unscaled.tif"
#    try:
#        os.remove(out_tile)
#    except:
#        pass
#    metadata, data = read_raster_window(raster_file, wgs84, tileindex, pixelbuffer=1, tilify=False)
#    with rasterio.open(out_tile, 'w', **metadata) as destination:
#        destination.write_band(1, data)

    config = MapcheteConfig()

    wgs84 = TileMatrix("4326")
    wgs84_meta = MetaTileMatrix(wgs84, 4)
    wgs84.set_format("GTiff")

    tile = (11, 574, 2144)

    zoom, row, col = tile
    tilebbox = wgs84.tile_bbox(zoom, row, col, pixelbuffer=1)
    metatiles = wgs84_meta.tiles_from_geom(tilebbox, zoom)

    tiles = []
    for metatile in metatiles:
        tiles.extend(wgs84_meta.tiles_from_tilematrix(*metatile))


    #from git import *
    #repo = Repo(".")
    #print repo.commit()
    #print repo.is_dirty()
    #for commit in repo.iter_commits():
    #    print commit



    # overwrite check
    import yaml
    with open("configs/dem_check.yaml", 'r') as stream:
        config = yaml.load(stream)
    params = MapcheteConfig()
    params.load_from_yaml(config["required_process"])
    tilematrix = TileMatrix("4326")
    tilematrix.set_format("GTiff")
    metatilematrix = MetaTileMatrix(tilematrix, 4)

    tile = (8, 63, 272)
    bbox = tilematrix.tile_bbox(*tile, pixelbuffer=1)

    print bbox

    # Check if tiles exist.
    metatiles = metatilematrix.tiles_from_geom(bbox, 8)

    extension = metatilematrix.format.extension
    tbd_metatiles = []
    for metatile in metatiles:
        tiles = metatilematrix.tiles_from_tilematrix(*metatile)
        process = False
        for tile in tiles:
            out_tile = tile_path(tile, params, extension)
            if not os.path.exists(out_tile):
                process = True
        if process == True:
            print "rendering metatile ", metatile
            tbd_metatiles.append(metatile)

    print len(tbd_metatiles)
    tiles = []
    for tbd_metatile in tbd_metatiles:
        print tbd_metatile
        tiles.extend(metatilematrix.tiles_from_tilematrix(*tbd_metatile))
    print tiles

    schema = {
            'geometry': 'Polygon',
            'properties': {'col': 'int', 'row': 'int'}
        }
    temp_out = "/home/ungarj/terrain/out/dem/test.geojson"
    try:
        os.remove(temp_out)
    except:
        pass
    with fiona.open(temp_out, 'w', 'GeoJSON', schema) as sink:
        for tile in tiles:
            zoom, row, col = tile
            feature = {}
            feature['geometry'] = mapping(tilematrix.tile_bbox(*tile))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)


if __name__ == "__main__":
    main(sys.argv[1:])