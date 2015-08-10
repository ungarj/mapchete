#!/usr/bin/env python

import sys
import os
import argparse
import fiona
from shapely.geometry import *
from shapely.ops import cascaded_union
import rasterio
from rasterio.warp import *
import numpy

# import local modules independent from script location
scriptdir = os.path.dirname(os.path.realpath(__file__))
rootdir = os.path.split(scriptdir)[0]
sys.path.append(os.path.join(rootdir, 'modules'))
from tilematrix import *
from tilematrix_io import *

ROUND = 20

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("zoom", nargs=1, type=int)
    parser.add_argument("coastline", nargs=1, type=str)
    parser.add_argument("output_folder", nargs=1, type=str)
    parser.add_argument("-d", "--debug", action="store_true")
    parsed = parser.parse_args(args)
    zoom = parsed.zoom[0]
    coastline_path = parsed.coastline[0]
    output_folder = parsed.output_folder[0]
    global debug
    debug = parsed.debug

    # Initialize Tile Matrix.
    wgs84 = TileMatrix("4326")
    wgs84_meta = MetaTileMatrix(wgs84, 8)

    raster_file = "../../../terrain/01_corsica/aster.tif"

    # Read input DEM metadata.
    with rasterio.open(raster_file) as aster:
        tl = [aster.bounds.left, aster.bounds.top]
        tr = [aster.bounds.right, aster.bounds.top]
        br = [aster.bounds.right, aster.bounds.bottom]
        bl = [aster.bounds.left, aster.bounds.bottom]
        aster_envelope = Polygon([tl, tr, br, bl])

    # TODO: default to union of input DEM bounding boxes; optional command
    # line parameter
    bounding_box = aster_envelope

    # Intersect bounding box with coastline dataset to determine footprint.
    with fiona.open(coastline_path, 'r') as coastline:
        geometries = []
        print "creating footprint using coastline ..."
        for feature in coastline:
            geometry = shape(feature['geometry'])
            if geometry.intersects(bounding_box):
                intersect = geometry.intersection(bounding_box)
                geometries.append(intersect)
        footprint = cascaded_union(geometries)
        print "done."

    # Get metatiles to be processed by footprint.
    metatiles = wgs84_meta.tiles_from_geom(footprint, zoom)
    #tiles = [(2150, 541)]
    #print "%s tiles to be processed" %(str(len(tiles)))
    zoomstring = "zoom%s" %(str(zoom))

    if debug:
        ## Write debug output.
        print "write tiling debug file ..."
        tiled_out_filename = zoomstring + ".geojson"
        tiled_out_path = os.path.join(output_folder, tiled_out_filename)
        schema = {
            'geometry': 'Polygon',
            'properties': {'col': 'int', 'row': 'int', 'metatile': 'str'}
        }
        try:
            os.remove(tiled_out_path)
        except:
            pass
        with fiona.open(tiled_out_path, 'w', 'GeoJSON', schema) as sink:
            for metatile in metatiles:
                zoom, col, row = metatile
                tiles = wgs84_meta.tiles_from_tilematrix(zoom, col, row, footprint)
                for tile in tiles:
                    zoom, col, row = tile
                    feature = {}
                    feature['geometry'] = mapping(wgs84.tile_bbox(zoom, col, row))
                    feature['properties'] = {}
                    feature['properties']['col'] = col
                    feature['properties']['row'] = row
                    feature['properties']['metatile'] = str(metatile)
                    sink.write(feature)
        print "done."

    # Do the processing.
    for metatile in metatiles:
        # resample_dem(metatile, footprint, zoom)
        zoom, col, row = metatile
        tiles = wgs84_meta.tiles_from_tilematrix(zoom, col, row, footprint)

        metadata, rasterdata = read_raster_window(raster_file, wgs84_meta, metatile,
            pixelbuffer=5)

        out_metatile_folder = os.path.join(output_folder+"/metatile", zoomstring)
        metatile_name = "%s%s.tif" %(col, row)
        out_metatile = os.path.join(out_metatile_folder, metatile_name)
        if not os.path.exists(out_metatile_folder):
            os.makedirs(out_metatile_folder)
        try:
            os.remove(out_metatile)
        except:
            pass

        write_raster_window(out_metatile, wgs84_meta, metatile, metadata,
            rasterdata, pixelbuffer=5)

        for tile in tiles:
            zoom, col, row = tile
    
            tileindex = zoom, col, row
    
            out_tile_folder = os.path.join(output_folder, zoomstring)
            tile_name = "%s%s.tif" %(col, row)
            out_tile = os.path.join(out_tile_folder, tile_name)
            if not os.path.exists(out_tile_folder):
                os.makedirs(out_tile_folder)
            try:
                os.remove(out_tile)
            except:
                pass
    
            if isinstance(rasterdata, np.ndarray):
                write_raster_window(out_tile, wgs84, tileindex, metadata,
                    rasterdata, pixelbuffer=0)
            else:
                print "empty!"


if __name__ == "__main__":
    main(sys.argv[1:])