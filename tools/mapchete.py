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

ROUND = 10

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("zoom", nargs=1, type=int)
    parser.add_argument("coastline", nargs=1, type=str)
    parser.add_argument("output_folder", nargs=1, type=str)

    parsed = parser.parse_args(args)

    zoom = parsed.zoom[0]
    coastline_path = parsed.coastline[0]
    output_folder = parsed.output_folder[0]

    tilematrix = TileMatrix("4326")

    input_dem = {}

    input_dem[0] = "../../../terrain/aster.tif"

    with rasterio.open(input_dem[0]) as aster:
        # 
        tl = [aster.bounds.left, aster.bounds.top]
        tr = [aster.bounds.right, aster.bounds.top]
        br = [aster.bounds.right, aster.bounds.bottom]
        bl = [aster.bounds.left, aster.bounds.bottom]
        aster_envelope = Polygon([tl, tr, br, bl])

        aster_resolution = aster.affine[0]

        src_crs = aster.crs
        src_transform = aster.affine

        band = aster.read(1)

    with fiona.open(coastline_path, 'r') as coastline:
        geometries = []
        print "creating footprint using coastline ..."
        for feature in coastline:
            geometries.append(shape(feature['geometry']))
        footprint = cascaded_union(geometries)


    tiles = tilematrix.tiles_from_geom(footprint, zoom)
    tiles = [(2150, 541)]

    print "%s tiles to be processed" %(str(len(tiles)))

    ## write debug output
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
            feature['geometry'] = mapping(tilematrix.tile_bounds(col, row, zoom))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)

    pixelbuffer = 10
    coordbuffer = pixelbuffer * aster_resolution

    with rasterio.open(input_dem[0]) as aster:

        for tile in tiles:
            col, row = tile
            print zoom, col, row
            tile_bbox = tilematrix.tile_bounds(col, row, zoom)
            left = tile_bbox.bounds[0]
            bottom = tile_bbox.bounds[1]
            right = tile_bbox.bounds[2]
            top = tile_bbox.bounds[3]
            dst_shape = (tilematrix.px_per_tile, tilematrix.px_per_tile)
            pxsize = tilematrix.pixelsize(zoom)
            dst_transform = [left, pxsize, 0.0, top, 0.0, -pxsize]
            dst_crs = tilematrix.crs
            destination = numpy.zeros(dst_shape, numpy.int16)

            out_left, out_bottom, out_right, out_top = transform_bounds(
                src_crs, dst_crs, left, bottom, right, top, densify_pts=21)

            nspixels = (out_top - out_bottom) / pxsize
            wepixels = (out_right - out_left) / pxsize

            width, height = dst_shape
            dst_affine, dst_width, dst_height = calculate_default_transform(
                src_crs,
                dst_crs,
                width,
                height,
                left,
                bottom,
                right,
                top,
                resolution=(pxsize, pxsize))

            kwargs = aster.meta
            kwargs['transform'] = dst_affine
            kwargs['height'] = dst_height
            kwargs['width'] = dst_width
            kwargs['compress'] = "lzw"

            try:
                reproject(
                    band,
                    destination,
                    src_transform=src_transform,
                    src_crs=src_crs,
                    dst_transform=dst_affine,
                    dst_crs=dst_crs,
                    resampling=RESAMPLING.lanczos)
            except:
                destination = None
                raise
 

            out_tile_folder = os.path.join(output_folder, zoomstring)
            tile_name = "%s%s.tif" %(col, row)
            out_tile = os.path.join(out_tile_folder, tile_name)
            if not os.path.exists(out_tile_folder):
                os.makedirs(out_tile_folder)
            try:
                os.remove(out_tile)
            except:
                pass

            with rasterio.open(out_tile, 'w', **kwargs) as dst:
                dst.write_band(1, destination)



    # usage: mapchete.py <process file> --[bbox|geom]
    
    # read process.json
    ## determine input files
    ## get tiles according to input files bboxes, or
    ## according to input bbox/geometry
    ## determine plugin to be used
    ## determine cores to be used

    # use plugin, tiles and multi parameter to start multiprocessing


if __name__ == "__main__":
    main(sys.argv[1:])