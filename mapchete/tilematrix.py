#!/usr/bin/env python

import sys
from shapely.geometry import Polygon, shape
from itertools import product
from shapely.prepared import prep

ROUND = 10

class TileMatrix:

    def __init__(self, projection):
        projections = ("4326", "3857")
        try:
            assert projection in projections
        except:
            print "WMTS tileset '%s' not found. Use one of %s" %(projection,
                projections)
            sys.exit(0)
        self.projection = projection
        if projection == "4326":
            self.left = float(-180)
            self.top = float(90)
            self.right = float(180)
            self.bottom = float(-90)
            self.wesize = float(round(self.right - self.left, ROUND))
            self.nssize = float(round(self.top - self.bottom, ROUND))

    def tiles_for_zoom(self, zoom):
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        if self.projection == "4326":
            wetiles = 2**(zoom+1)
            nstiles = wetiles/2
            return wetiles, nstiles

    def tilesize_for_zoom(self, zoom):
        nstiles, wetiles = self.tiles_for_zoom(zoom)
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        tile_nssize = float(round(self.nssize/nstiles, ROUND))
        tile_wesize = float(round(self.wesize/wetiles, ROUND))
        return tile_wesize, tile_nssize

    def top_left_tile_coords(self, col, row, zoom):
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        tile_wesize, tile_nssize = self.tilesize_for_zoom(zoom)
        if (col+1 > self.tiles_for_zoom(zoom)[0]) or (row+1 > self.tiles_for_zoom(zoom)[1]):
            print "no tile indices available on this zoom"
        else:
            left = float(round(self.left+((col)*tile_wesize), ROUND))
            upper = float(round(self.top-((row)*tile_nssize), ROUND))
            return left, upper

    def tile_bounds(self, col, row, zoom):
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        ul = self.top_left_tile_coords(col, row, zoom)
        ur = ul[0] + self.wesize, ul[1]
        lr = ul[0] + self.wesize, ul[1] - self.nssize
        ll = ul[0], ul[1] - self.nssize
    
        return Polygon([ul, ur, lr, ll])


def tiles_from_bbox(tilematrix, geometry, zoom):
    try:
        assert isinstance(zoom, int)
    except:
        print "Zoom (%s) must be an integer." %(zoom)
        sys.exit(0)
    tile_wesize, tile_nssize = tilematrix.tilesize_for_zoom(zoom)
    tilelist = []
    l, b, r, t = geometry.bounds
    tilelon = tilematrix.left
    tilelat = tilematrix.top
    cols = []
    rows = []
    col = -1
    row = -1
    while tilelon <= l:
        tilelon += tile_wesize
        col += 1
    cols.append(col)
    while tilelon < r:
        tilelon += tile_wesize
        col += 1
        cols.append(col)
    while tilelat >= t:
        tilelat -= tile_nssize
        row += 1
    rows.append(row)
    while tilelat > b:
        tilelat -= tile_nssize
        row += 1
        rows.append(row)
    tilelist = list(product(cols, rows))   
    
    return tilelist


def tiles_from_geom(tilematrix, geometry, zoom):
    # returns tiles intersecting with input geometry

    tilelist = []

    if geometry.almost_equals(geometry.envelope, ROUND):

        tilelist = tiles_from_bbox(tilematrix, geometry, zoom)
        print "pompete"

    elif geometry.geom_type == "Point":

        lon, lat = list(geometry.coords)[0]

        tilelon = tilematrix.left
        tilelat = tilematrix.top

        col = -1
        row = -1
    
        while tilelon < lon:
            tilelon += tile.wesize
            col += 1
    
        while tilelat > lat:
            tilelat -= tile.nssize
            row += 1
    
        tilelist.append((col, row))

    elif geometry.geom_type in ("LineString", "MultiLineString", "Polygon",
        "MultiPolygon", "MultiPoint"):

        prepared_geometry = prep(geometry)
        bbox_tilelist = tiles_from_bbox(tilematrix, geometry, zoom)  
        for tile in bbox_tilelist:
            col, row = tile
            print tile
            geometry = tilematrix.tile_bounds(col, row, zoom)
            if prepared_geometry.intersects(geometry):
                tilelist.append((col, row))

    else:
        print "ERROR: no valid geometry"
        sys.exit(0)

    return tilelist