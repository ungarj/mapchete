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

    # Initialize Tile Matrix.
    tilematrix = TileMatrix("4326")

    input_dem = {}
    input_dem[0] = "../../../terrain/aster.tif"

    # Read input DEM metadata.
    with rasterio.open(input_dem[0]) as aster:
        tl = [aster.bounds.left, aster.bounds.top]
        tr = [aster.bounds.right, aster.bounds.top]
        br = [aster.bounds.right, aster.bounds.bottom]
        bl = [aster.bounds.left, aster.bounds.bottom]
        aster_envelope = Polygon([tl, tr, br, bl])
        src_crs = aster.crs
        src_affine = aster.affine
        src_meta = aster.meta
        src_shape = aster.shape
        src_nodata = int(aster.nodatavals[0])

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
    tiles = tilematrix.tiles_from_geom(footprint, zoom)
    #tiles = [(2150, 541)]
    print "%s tiles to be processed" %(str(len(tiles)))
    ## Write debug output.
    zoomstring = "zoom%s" %(str(zoom))
    tiled_out_filename = zoomstring + ".geojson"
    tiled_out_path = os.path.join(output_folder, tiled_out_filename)
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
            feature['geometry'] = mapping(tilematrix.tile_bbox(col, row, zoom))
            feature['properties'] = {}
            feature['properties']['col'] = col
            feature['properties']['row'] = row
            sink.write(feature)


    # Do the processing.
    for tile in tiles:
        col, row = tile

        print zoom, col, row

        tile_bbox_shape = shape(tilematrix.tile_bbox(col, row, zoom))
        left = tile_bbox_shape.bounds[0]
        bottom = tile_bbox_shape.bounds[1]
        right = tile_bbox_shape.bounds[2]
        top = tile_bbox_shape.bounds[3]

        pixelsize = tilematrix.pixelsize(zoom)
        dst_size = (tilematrix.px_per_tile, tilematrix.px_per_tile)
        dst_crs = tilematrix.crs
        dst_data = numpy.zeros(dst_size, numpy.int16)

        out_left, out_bottom, out_right, out_top = transform_bounds(
            src_crs, dst_crs, left, bottom, right, top, densify_pts=21)
        
        nspixels = (out_top - out_bottom) / pixelsize
        wepixels = (out_right - out_left) / pixelsize
        width, height = dst_size

        # Get destination affine transformation.
        dst_affine, dst_width, dst_height = calculate_default_transform(
            src_crs,
            dst_crs,
            width,
            height,
            left,
            bottom,
            right,
            top,
            resolution=(pixelsize, pixelsize))

        #print aster.index(8.4998611,  41.0001389)

        #print "herbert %s %s" %(aster.index(out_left, out_top)[0], aster.index(out_left, out_bottom)[0])

        # Get pixel coordinates of tile in source dataset.
        #print (out_left, out_top)
        #print (out_right, out_bottom)
        # .index --> row, column
        minrow, mincol = aster.index(out_left, out_top)
        maxrow, maxcol = aster.index(out_right, out_bottom)

        #print ((out_top, out_bottom), (out_left, out_right))

        rows = (minrow, maxrow)
        cols = (mincol, maxcol)
        #print "old: %s, %s" %(rows, cols)
        window_offset_row = minrow
        window_offset_col = mincol


        minrow, minrow_offset = clean_pixel_coordinates(minrow, src_shape[0])
        maxrow, maxrow_offset = clean_pixel_coordinates(maxrow, src_shape[0])
        mincol, mincol_offset = clean_pixel_coordinates(mincol, src_shape[1])
        maxcol, maxcol_offset = clean_pixel_coordinates(maxcol, src_shape[1])

        rows = (minrow, maxrow)
        cols = (mincol, maxcol)
        #print "new: %s, %s" %(rows, cols)

        with rasterio.open(input_dem[0]) as aster:
            window_data = aster.read(1, window=(rows, cols))
            if minrow_offset:
                nullarray = np.empty((minrow_offset, window_data.shape[1]), dtype="int16")
                nullarray[:] = src_nodata
                newarray = np.concatenate((nullarray, window_data), axis=0)
                window_data = newarray
            if maxrow_offset:
                nullarray = np.empty((maxrow_offset, window_data.shape[1]), dtype="int16")
                nullarray[:] = src_nodata
                newarray = np.concatenate((window_data, nullarray), axis=0)
                window_data = newarray
            if mincol_offset:
                nullarray = np.empty((window_data.shape[0], mincol_offset), dtype="int16")
                nullarray[:] = src_nodata
                newarray = np.concatenate((nullarray, window_data), axis=1)
                window_data = newarray
            if maxcol_offset:
                nullarray = np.empty((window_data.shape[0], maxcol_offset), dtype="int16")
                nullarray[:] = src_nodata
                newarray = np.concatenate((nullarray, window_data), axis=1)
                window_data = newarray

            debug_tile_name = "debug_%s%s.tif" %(col, row)
            debug_tile = os.path.join(output_folder, debug_tile_name)
            try:
                os.remove(debug_tile_name)
            except:
                pass
            window_vector_affine = src_affine.translation(window_offset_col, window_offset_row)
            window_affine = src_affine * window_vector_affine
            window_meta = src_meta
            window_meta['transform'] = window_affine
            window_meta['height'] = window_data.shape[0]
            window_meta['width'] = window_data.shape[1]
            window_meta['compress'] = "lzw"
            with rasterio.open(debug_tile, 'w', **window_meta) as window:
                window.write_band(1, window_data)

            dst_meta = src_meta
            dst_meta['transform'] = dst_affine
            dst_meta['height'] = height
            dst_meta['width'] = width
            dst_meta['compress'] = "lzw"

            try:
                reproject(
                    window_data,
                    dst_data,
                    src_transform=window_affine,
                    src_crs=src_crs,
                    dst_transform=dst_affine,
                    dst_crs=dst_crs,
                    resampling=RESAMPLING.lanczos)
            except:
                dst_data = None
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
            with rasterio.open(out_tile, 'w', **dst_meta) as dst:
                dst.write_band(1, dst_data)



    # usage: mapchete.py <process file> --[bbox|geom]
    
    # read process.json
    ## determine input files
    ## get tiles according to input files bboxes, or
    ## according to input bbox/geometry
    ## determine plugin to be used
    ## determine cores to be used

    # use plugin, tiles and multi parameter to start multiprocessing


def clean_pixel_coordinates(coordinate, maximum):
    # Crops pixel coordinate to 0 or maximum (array.shape) if necessary
    # and returns an offset if necessary.
    offset = None
    if coordinate < 0:
        offset = -coordinate
        coordinate = 0
    if coordinate > maximum:
        offset = coordinate - maximum
        coordinate = maximum
    return coordinate, offset



if __name__ == "__main__":
    main(sys.argv[1:])