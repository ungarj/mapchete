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

    # Get tiles to be processed by footprint.
    tiles = wgs84.tiles_from_geom(footprint, zoom)
    #tiles = [(2150, 541)]
    print "%s tiles to be processed" %(str(len(tiles)))
    ## Write debug output.
    zoomstring = "zoom%s" %(str(zoom))
    tiled_out_filename = zoomstring + ".geojson"
    tiled_out_path = os.path.join(output_folder, tiled_out_filename)
    print tiled_out_path
    schema = {
        'geometry': 'Polygon',
        'properties': {'col': 'int', 'row': 'int'}
    }
    try:
        os.remove(tiled_out_path)
    except:
        pass
    with fiona.open(tiled_out_path, 'w', 'GeoJSON', schema) as sink:
        for tile in tiles:
            col, row = tile
            feature = {}
            feature['geometry'] = mapping(wgs84.tile_bbox(col, row, zoom))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)


    # Do the processing.
    for tile in tiles:
        col, row = tile

        tileindex = zoom, col, row
        print tileindex

        out_tile_folder = os.path.join(output_folder, zoomstring)
        tile_name = "%s%s.tif" %(col, row)
        out_tile = os.path.join(out_tile_folder, tile_name)
        if not os.path.exists(out_tile_folder):
            os.makedirs(out_tile_folder)
        try:
            os.remove(out_tile)
        except:
            pass
        metadata, data = read_raster_window(raster_file, wgs84, tileindex,
            pixelbuffer=0)
        if isinstance(data, np.ndarray):
            with rasterio.open(out_tile, 'w', **metadata) as dst:
                dst.write_band(1, data)
        else:
            "empty!"


if __name__ == "__main__":
    main(sys.argv[1:])