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
    wgs84_meta = MetaTileMatrix(wgs84, 23)


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
                col, row = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
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
            col, row = point_tile[0]
            feature = {}
            feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
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
                col, row = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
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
                col, row = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
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
                col, row = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
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
                col, row = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
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
                col, row = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
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
        print "metatilematrix: %s" %([wgs84_meta.top_left_tile_coords(zoom, col, row)])
        print "tile bounding box"
        print "metatilematrix: %s" %([mapping(wgs84.tile_bbox(zoom, col, row))])
        print "tile bounds"
        print "metatilematrix: %s" %([wgs84_meta.tile_bounds(zoom, col, row)])
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
                col, row = tile
                feature = {}
                feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
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
                col, row = metatile
                feature = {}
                feature['geometry'] = mapping(wgs84_meta.tile_bbox(zoom, col, row))
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


    print wgs84.pixelsize(8)
    print wgs84.tile_bbox(8, 268, 67, 5)

if __name__ == "__main__":
    main(sys.argv[1:])