#!/usr/bin/env python

import sys
import os
import argparse
import fiona
from shapely.geometry import mapping, shape, box, Point, LineString, MultiLineString, Polygon, MultiPolygon, GeometryCollection
from shapely.ops import cascaded_union
from shapely.prepared import prep
from shapely.validation import explain_validity
from itertools import product

ROUND = 10
RESOLUTION = 256

def main(args):
    global cleaning_failed
    
    parser = argparse.ArgumentParser()
    parser.add_argument("zoom", nargs=1, type=int)
    parser.add_argument("outtiles", nargs=1, type=str)
    parser.add_argument("outfolder", nargs=1, type=str)
    parser.add_argument("metatile", nargs=1, type=int)
    parser.add_argument('--bbox', nargs=4, type=float, help='ulx uly lrx lry')
    parser.add_argument('--infile', nargs=1, type=str, help='subset geometry')

    parsed = parser.parse_args(args)
    zoom = parsed.zoom[0]
    outtiles = parsed.outtiles[0]
    outfolder = parsed.outfolder[0]
    metatile = parsed.metatile[0]

    if parsed.bbox:
        ulx, uly, lrx, lry = parsed.bbox
    else:
        ulx, uly, lrx, lry = -180.0, 90.0, 180.0, -90.0

    if parsed.infile:
        input_file = parsed.infile[0]

    # check if outfolder exists and create if not
    if not os.path.exists(outfolder):
        os.makedirs(outfolder)

    zoomfolder = os.path.join(outfolder, str(zoom))
    if not os.path.exists(zoomfolder):
        os.makedirs(zoomfolder)



    # WMTS tileset definition
    TileMatrixLeft = float(-180)
    TileMatrixTop = float(90)
    TileMatrixRight = float(180)
    TileMatrixBottom = float(-90)

    TileMatrixWESize = float(round(TileMatrixRight - TileMatrixLeft, ROUND))
    TileMatrixNSSize = float(round(TileMatrixTop - TileMatrixBottom, ROUND))

    TileNumbers = tile_numbers(zoom)

    TileNSSize = float(round(TileMatrixNSSize/TileNumbers[1], ROUND))
    TileWESize = float(round(TileMatrixWESize/TileNumbers[0], ROUND))

    TileMatrix = (TileMatrixLeft, TileMatrixTop, TileMatrixRight,
        TileMatrixBottom, TileMatrixWESize, TileMatrixNSSize, TileNSSize,
        TileWESize, TileNumbers)


    with fiona.open(input_file) as input_shape:
        geoms = []
        for feature in input_shape:
            geoms.append(shape(feature['geometry']))
        union_geometry = cascaded_union(geoms)

        # write WMTS tiles
        outtiles_schema = {
            'geometry': 'Polygon',
            'properties': {'column': 'str', 'row': 'str'},
        }
        with fiona.open(outtiles, 'w', 'GeoJSON', outtiles_schema) as sink:
            tiles = get_tiles(TileMatrix, union_geometry, parsed)
            for tile in tiles:
                col, row = tile
                feature = {}
                feature['properties'] = {}
                feature['properties']['column'] = col
                feature['properties']['row'] = row
                tilegeom = get_boundaries(TileMatrix, col, row)
                feature['geometry'] = mapping(tilegeom)
                sink.write(feature)



        # write tiled output
        outfiles_schema = {
            'geometry': union_geometry.geom_type,
            'properties': {'column': 'str', 'row': 'str'},
        }
        merged_outfile = os.path.join(zoomfolder, str(zoom)+".geojson")
        try:
            os.remove(merged_outfile)
        except:
            pass
        with fiona.open(merged_outfile, 'w', 'GeoJSON', outfiles_schema) as merged_sink:
            cleaning_failed = 0
            for feature in input_shape:
                feature_geometry = shape(feature['geometry'])
                tiles = get_tiles(TileMatrix, feature_geometry, parsed)
                for tile in tiles:
                    col, row = tile
                    tilegeom = get_boundaries(TileMatrix, col, row)
                    intersection = feature_geometry.intersection(tilegeom)
                    simplified = simplify_geometry(TileMatrix, col, row,
                        intersection, RESOLUTION)
                    try:
                        simplified_shape = shape(simplified)
                        colfolder = os.path.join(zoomfolder, str(col))
                        if not os.path.exists(colfolder):
                            os.makedirs(colfolder)
                        outfile = os.path.join(colfolder, (str(row)+".geojson"))
                        try:
                            os.remove(outfile)
                        except:
                            pass
                        with fiona.open(outfile, 'w', 'GeoJSON', outfiles_schema) as sink:
                            feature = {}
                            feature['properties'] = {}
                            feature['properties']['column'] = col
                            feature['properties']['row'] = row
                            feature['geometry'] = mapping(simplified)
                            sink.write(feature)
                        merged_sink.write(feature)
                    except:
                        print "empty output geometry"

            print "%s geometries failed to be cleaned" %(cleaning_failed)



def simplify_geometry(TileMatrix, col, row, geometry, resolution):
    # get geometry, loop through coordinates and subgeometry

    # get tile boundaries
    tile = get_boundaries(TileMatrix, col, row)
    left = tile.bounds[0]
    bottom = tile.bounds[1]

    #print "input geometry type: %s" %(geometry.type)

    # process different geometry types

    if isinstance(geometry, Point):
        coordinates = mapping(geometry)['coordinates']
        out_coordinates = snap_point_to_grid(TileMatrix, left, bottom, coordinates,
            resolution)
        out_geometry = Point(out_coordinates)

    elif isinstance(geometry, LineString):
        coordinates = mapping(geometry)['coordinates']
        out_coordinates = []
        for point in coordinates:
            out_point = snap_point_to_grid(TileMatrix, left, bottom, point,
                resolution)
            out_coordinates.append(out_point)
        out_geometry = LineString(out_coordinates)

    elif isinstance(geometry, MultiLineString):
        out_linestrings = []
        for linestring in geometry:
            coordinates = mapping(linestring)['coordinates']
            out_coordinates = []
            for point in coordinates:
                out_point = snap_point_to_grid(TileMatrix, left, bottom, point,
                    resolution)
                out_coordinates.append(out_point)
            out_linestring = LineString(out_coordinates)
            out_linestrings.append(out_linestring)
        out_geometry = MultiLineString(out_linestrings)

    elif isinstance(geometry, Polygon):
        polygon = geometry
        out_polygon = simplify_geometry_polygon(TileMatrix, col, row, polygon,
            resolution)
        if out_polygon:
            out_geometry = out_polygon
        else:
            out_geometry = None

    elif isinstance(geometry, MultiPolygon):
        out_polygons = []
        for polygon in geometry:
            out_polygon = simplify_geometry_polygon(TileMatrix, col, row,
                polygon, resolution)
            if out_polygon:
                # TODO: check validity of Polygon/MultiPolygon switch
                if isinstance(out_polygon, Polygon):
                    out_polygons.append(out_polygon)
                if isinstance(out_polygon, MultiPolygon):
                    #print "%s new subpolygons created" %(len(out_polygon))
                    for sub_polygon in out_polygon:
                        out_polygons.append(sub_polygon)
        if len(out_polygons) > 0:
            out_multipolygon = MultiPolygon(out_polygons)
        else:
            out_geometry = None

        out_geometry = out_multipolygon

    elif isinstance(geometry, GeometryCollection):
        out_subgeometries = []
        for subgeometry in geometry:
            if isinstance(subgeometry, Polygon) or isinstance(subgeometry, MultiPolygon):
                out_subgeometry = simplify_geometry(TileMatrix, col, row,
                    subgeometry, resolution)
                if out_subgeometry:
                    out_subgeometries.append(out_subgeometry)
            if len(out_subgeometries) > 0:
                out_geometrycollection = MultiPolygon(out_subgeometries)
                out_geometry = out_geometrycollection
            else:
                out_geometry = None


    #if out_geometry:
    #    if not out_geometry.is_valid:
    #        clean = out_geometry.buffer(0)
    #        if isinstance(out_geometry, Polygon):
    #            print len(out_geometry.exterior.coords), len(clean.exterior.coords)
    #        out_geometry = clean

    #print "output geometry type: %s" %(out_geometry.type)

    return out_geometry


def simplify_geometry_polygon(TileMatrix, col, row, polygon, resolution):

    global cleaning_failed

    (TileMatrixLeft, TileMatrixTop, TileMatrixRight, TileMatrixBottom,
        TileMatrixWESize, TileMatrixNSSize, TileNSSize, TileWESize, TileNumbers
        ) = TileMatrix

    pxresolution = TileWESize/resolution

    assert isinstance(polygon, Polygon)

    # get tile boundaries
    tile = get_boundaries(TileMatrix, col, row)
    left = tile.bounds[0]
    bottom = tile.bounds[1]

    # polygon external boundaries
    exterior_ring_coordinates = polygon.exterior.coords
    out_exterior_ring_coordinates = snap_coordinates_to_grid(TileMatrix,
        left, bottom, exterior_ring_coordinates, resolution)
    # polygon interior rings
    interior_rings = polygon.interiors
    out_interior_rings = []
    for interior_ring in interior_rings:
        interior_ring_coordinates = interior_ring.coords
        out_interior_ring_coordinates = snap_coordinates_to_grid(
            TileMatrix, left, bottom, interior_ring_coordinates,
            resolution)
        if len(out_interior_ring_coordinates) >=3:
            out_interior_rings.append(out_interior_ring_coordinates)
    if len(out_exterior_ring_coordinates) >= 3:
        out_polygon = Polygon(out_exterior_ring_coordinates,
            out_interior_rings)#.simplify(pxresolution/10)
        if out_polygon.area == 0:
            out_polygon = None
        else:
            if not out_polygon.is_valid:
                clean = out_polygon.buffer(0)
                # dirty hack; some polygons disappear while cleaning with buffer(0)
                if not clean.is_empty:
                    out_polygon = clean
                else:
                    print "cleaning failed, POLYGON empty:"
                    print out_polygon
                    print explain_validity(clean)
                    cleaning_failed += 1
                    out_polygon = None
            
    else:
        out_polygon = None

    return out_polygon


def snap_coordinates_to_grid(TileMatrix, left, bottom, coordinates,
    resolution):

    out_coordinates = []
    prior_point = None
    for point in coordinates:
        out_point = snap_point_to_grid(TileMatrix, left, bottom, point,
            resolution)
        if out_point != prior_point:
            out_coordinates.append(out_point)
        prior_point = out_point

    if len(out_coordinates) > 0:
        return out_coordinates
    else:
        return None


def snap_point_to_grid(TileMatrix, left, bottom, coordinates, resolution):

    (TileMatrixLeft, TileMatrixTop, TileMatrixRight, TileMatrixBottom,
        TileMatrixWESize, TileMatrixNSSize, TileNSSize, TileWESize, TileNumbers
        ) = TileMatrix

    lon, lat = coordinates
    pxresolution = TileWESize/resolution
    lonpx = round(((lon - left)/TileWESize)*resolution)
    latpx = round(((lat - bottom)/TileNSSize)*resolution)
    outlon = (lonpx*pxresolution)+left
    outlat = (latpx*pxresolution)+bottom
    out_coordinates = outlon, outlat
     
    return out_coordinates


def get_tiles(TileMatrix, geometry, parsed):
    # returns tiles intersecting with input geometry

    (TileMatrixLeft, TileMatrixTop, TileMatrixRight, TileMatrixBottom,
        TileMatrixWESize, TileMatrixNSSize, TileNSSize, TileWESize, TileNumbers
        ) = TileMatrix

    tilelist = []

    if parsed.bbox:

        tilelist = get_tiles_from_bbox(TileMatrix, geometry)

    elif geometry.geom_type == "Point":

        lon, lat = list(geometry.coords)[0]

        tilelon = TileMatrixLeft
        tilelat = TileMatrixTop

        col = -1
        row = -1
    
        while tilelon < lon:
            tilelon += TileWESize
            col += 1
    
        while tilelat > lat:
            tilelat -= TileNSSize
            row += 1
    
        tilelist.append((col, row))

    elif geometry.geom_type in ("LineString", "MultiLineString", "Polygon",
        "MultiPolygon", "MultiPoint"):

        prepared_geometry = prep(geometry)
        bbox_tilelist = get_tiles_from_bbox(TileMatrix, geometry)  
        for tile in bbox_tilelist:
            col, row = tile
            geometry = get_boundaries(TileMatrix, col, row)
            if prepared_geometry.intersects(geometry):
                tilelist.append((col, row))

    else:
        print "ERROR: no valid geometry"
        sys.exit(0)

    return tilelist


def get_tiles_from_bbox(TileMatrix, geometry):

    (TileMatrixLeft, TileMatrixTop, TileMatrixRight, TileMatrixBottom,
        TileMatrixWESize, TileMatrixNSSize, TileNSSize, TileWESize, TileNumbers
        ) = TileMatrix

    tilelist = []

    l, b, r, t = geometry.bounds
    
    tilelon = TileMatrixLeft
    tilelat = TileMatrixTop
    
    cols = []
    rows = []
    
    col = -1
    row = -1
    
    while tilelon <= l:
        tilelon += TileWESize
        col += 1
    cols.append(col)
    while tilelon < r:
        tilelon+=TileWESize
        col += 1
        cols.append(col)
    
    while tilelat >= t:
        tilelat -= TileNSSize
        row += 1
    rows.append(row)
    while tilelat > b:
        tilelat -= TileNSSize
        row += 1
        rows.append(row)
    
    tilelist = list(product(cols, rows))   

    return tilelist


def get_boundaries(TileMatrix, TileCol, TileRow):
    # returns tile boundaries

    (TileMatrixLeft, TileMatrixTop, TileMatrixRight, TileMatrixBottom,
        TileMatrixWESize, TileMatrixNSSize, TileNSSize, TileWESize, TileNumbers
        ) = TileMatrix

    ul = top_left(TileMatrix, TileCol, TileRow)
    ur = ul[0]+TileWESize, ul[1]
    lr = ul[0]+TileWESize, ul[1]-TileNSSize
    ll = ul[0], ul[1]-TileNSSize

    return Polygon([ul, ur, lr, ll])

    
def top_left(TileMatrix, TileCol, TileRow):
    # returns top left coordinates of tile

    (TileMatrixLeft, TileMatrixTop, TileMatrixRight, TileMatrixBottom,
        TileMatrixWESize, TileMatrixNSSize, TileNSSize, TileWESize, TileNumbers
        ) = TileMatrix

    if (TileCol+1 > TileNumbers[0]) or (TileRow+1 > TileNumbers[1]):
        print "no tile indices available on this zoom"
    else:
        TileLeft = float(round(TileMatrixLeft+((TileCol)*TileWESize), ROUND))
        TileTop = float(round(TileMatrixTop-((TileRow)*TileNSSize), ROUND))
        return TileLeft, TileTop


def tile_numbers(zoom):
    # returns tile numbers

    WETiles = 2**(zoom+1)
    NSTiles = WETiles/2

    return WETiles, NSTiles


if __name__ == "__main__":
    main(sys.argv[1:])