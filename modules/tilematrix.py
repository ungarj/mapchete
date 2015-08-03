#!/usr/bin/env python

import sys
from shapely.geometry import *
from shapely.validation import *
from shapely.prepared import prep
from itertools import product
import math

ROUND = 20


class TileMatrix(object):

    def __init__(self, projection, px_per_tile=256):
        projections = ("4326", "3857")
        try:
            assert projection in projections
        except:
            print "WMTS tileset '%s' not found. Use one of %s" %(projection,
                projections)
            sys.exit(0)
        self.projection = projection
        self.px_per_tile = px_per_tile
        if projection == "4326":
            self.left = float(-180)
            self.top = float(90)
            self.right = float(180)
            self.bottom = float(-90)
            self.wesize = float(round(self.right - self.left, ROUND))
            self.nssize = float(round(self.top - self.bottom, ROUND))
            self.crs = {'init': u'EPSG:4326'}

    def tiles_per_zoom(self, zoom):
        # Numbers of tiles at zoom level.
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        if self.projection == "4326":
            wetiles = 2**(zoom+1)
            nstiles = wetiles/2
        return wetiles, nstiles

    def tilesize_per_zoom(self, zoom):
        # Tile size in coordinates at zoom level.
        wetiles, nstiles = self.tiles_per_zoom(zoom)
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        tile_wesize = float(round(self.wesize/wetiles, ROUND))
        tile_nssize = float(round(self.nssize/nstiles, ROUND))
        return tile_wesize, tile_nssize

    def pixelsize(self, zoom):
        pixelsize = float(round(self.tilesize_per_zoom(zoom)[0] /
            self.px_per_tile, ROUND))
        return pixelsize

    def top_left_tile_coords(self, col, row, zoom):        
        left, upper = top_left_tile_coords(self, col, row, zoom)
        return left, upper

    def tile_bounds(self, col, row, zoom, pixelbuffer=None):
        bounds = tile_bounds(self, col, row, zoom, pixelbuffer=pixelbuffer)
        return bounds

    def tile_bbox(self, col, row, zoom, pixelbuffer=None):
        bbox = tile_bbox(self, col, row, zoom, pixelbuffer=pixelbuffer)
        return bbox

    def tiles_from_bbox(self, geometry, zoom):
        tilelist = tiles_from_bbox(self, geometry, zoom)        
        return tilelist    
    
    def tiles_from_geom(self, geometry, zoom):
        # returns tiles intersecting with input geometry    
        tilelist = tiles_from_geom(self, geometry, zoom)    
        return tilelist


class MetaTileMatrix(TileMatrix):

    def __init__(self, tilematrix, metatiles=1):
        assert isinstance(tilematrix, TileMatrix)
        assert isinstance(metatiles, int)
        assert metatiles > 0
        self.tilematrix = tilematrix
        self.metatiles = metatiles
        self.metatilematrix = TileMatrix(self.tilematrix.projection)
        self.projection = tilematrix.projection
        self.px_per_tile = tilematrix.px_per_tile * metatiles
        self.left = tilematrix.left
        self.top = tilematrix.top
        self.right  = tilematrix.right 
        self.bottom = tilematrix.bottom
        self.wesize = tilematrix.wesize
        self.nssize = tilematrix.nssize
        self.crs = tilematrix.crs

    def tiles_per_zoom(self, zoom):
        # Numbers of metatiles at zoom level.
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        if self.projection == "4326":
            wetiles, nstiles = self.tilematrix.tiles_per_zoom(zoom)
            wetiles = math.ceil(wetiles / float(self.metatiles))
            if wetiles < 1:
                wetiles = 1
            nstiles = math.ceil(nstiles / float(self.metatiles))
            if nstiles < 1:
                nstiles = 1
        return int(wetiles), int(nstiles)

    def tilesize_per_zoom(self, zoom):
        # Metatile size in coordinates at zoom level.
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        tile_wesize, tile_nssize = self.tilematrix.tilesize_per_zoom(zoom)
        tile_wesize = tile_wesize * float(self.metatiles)
        if tile_wesize > self.wesize:
            tile_wesize = self.wesize
        tile_nssize = tile_nssize * float(self.metatiles)
        if tile_nssize > self.nssize:
            tile_nssize = self.nssize
        return tile_wesize, tile_nssize

    def pixelsize(self, zoom):
        pixelsize = self.tilematrix.pixelsize(zoom)
        return round(pixelsize, ROUND)

    def top_left_tile_coords(self, col, row, zoom):        
        try:
            left, upper = top_left_tile_coords(self, col, row, zoom)
            return left, upper
        except:
            print "ERROR determining tile coordinates."
            raise

    def tile_bounds(self, col, row, zoom, pixelbuffer=None):
        bounds = tile_bounds(self, col, row, zoom, pixelbuffer=pixelbuffer)
        return bounds

    def tile_bbox(self, col, row, zoom, pixelbuffer=None):
        bbox = tile_bbox(self, col, row, zoom, pixelbuffer=pixelbuffer)
        return bbox

    def tiles_from_bbox(self, geometry, zoom):
        tilelist = tiles_from_bbox(self, geometry, zoom)        
        return tilelist    
    
    def tiles_from_geom(self, geometry, zoom):
        # returns tiles intersecting with input geometry    
        tilelist = tiles_from_geom(self, geometry, zoom)    
        return tilelist

    def tiles_from_tilematrix(self, col, row, zoom):
        tilematrix = self.tilematrix
        metatile_bbox = self.tile_bbox(col, row, zoom)
        tilelist = tilematrix.tiles_from_bbox(metatile_bbox, zoom)
        return tilelist


# shared methods for TileMatrix and MetaTileMatrix
def tile_bounds(tilematrix, col, row, zoom, pixelbuffer=None):
    try:
        assert isinstance(zoom, int)
    except:
        print "Zoom (%s) must be an integer." %(zoom)
        sys.exit(0)
    tile_wesize, tile_nssize = tilematrix.tilesize_per_zoom(zoom)
    ul = tilematrix.top_left_tile_coords(col, row, zoom)
    left = ul[0]
    bottom = ul[1] - tile_nssize
    right = ul[0] + tile_wesize
    top = ul[1]
    if pixelbuffer:
        assert isinstance(pixelbuffer, int)
        offset = tilematrix.pixelsize(zoom)
        left -= offset
        bottom -= offset
        right += offset
        top += offset
    if right > tilematrix.right:
        right = tilematrix.right
    if bottom < tilematrix.bottom:
        bottom = tilematrix.bottom
    return (left, bottom, right, top)


def tile_bbox(tilematrix, col, row, zoom, pixelbuffer=None):
    try:
        assert isinstance(zoom, int)
    except:
        print "Zoom (%s) must be an integer." %(zoom)
        sys.exit(0)
    left, bottom, right, top = tilematrix.tile_bounds(col, row, zoom,
        pixelbuffer=pixelbuffer)
    ul = left, top
    ur = right, top
    lr = right, bottom
    ll = left, bottom
    return Polygon([ul, ur, lr, ll])


def top_left_tile_coords(tilematrix, col, row, zoom):
    try:
        assert isinstance(zoom, int)
    except:
        print "Zoom (%s) must be an integer." %(zoom)
        sys.exit(0)
    tile_wesize, tile_nssize = tilematrix.tilesize_per_zoom(zoom)
    wetiles, nstiles = tilematrix.tiles_per_zoom(zoom)

    if (col > wetiles) or (row > nstiles):
        print "no tile indices available on this zoom"
        print zoom, col, row
        print tilematrix.tiles_per_zoom(zoom)
    else:
        left = float(round(tilematrix.left+((col)*tile_wesize), ROUND))
        upper = float(round(tilematrix.top-((row)*tile_nssize), ROUND))
        return left, upper


def tiles_from_bbox(tilematrix, geometry, zoom):
    try:
        assert isinstance(zoom, int)
    except:
        print "Zoom (%s) must be an integer." %(zoom)
        sys.exit(0)
    tile_wesize, tile_nssize = tilematrix.tilesize_per_zoom(zoom)
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

    try:
        assert geometry.is_valid
    except:
        print "WARNING: geometry seems not to be valid"
        print explain_validity(geometry)
        try:
            clean = geometry.buffer(0.0)
            assert clean.is_valid
            assert clean.area > 0
            geometry = clean
            print "... cleaning successful"
        except:
            print "... geometry could not be fixed"
            print explain_validity(clean)
    
    if geometry.almost_equals(geometry.envelope, ROUND):
        tilelist = tilematrix.tiles_from_bbox(geometry, zoom)

    elif geometry.geom_type == "Point":
        lon, lat = list(geometry.coords)[0]
        tilelon = tilematrix.left
        tilelat = tilematrix.top
        tile_wesize, tile_nssize = tilematrix.tilesize_per_zoom(zoom)
        col = -1
        row = -1
        while tilelon < lon:
            tilelon += tile_wesize
            col += 1
        while tilelat > lat:
            tilelat -= tile_nssize
            row += 1
        tilelist.append((col, row))

    elif geometry.geom_type in ("LineString", "MultiLineString", "Polygon",
        "MultiPolygon", "MultiPoint"):
        prepared_geometry = prep(geometry)
        bbox_tilelist = tilematrix.tiles_from_bbox(geometry, zoom)  
        for tile in bbox_tilelist:
            col, row = tile
            geometry = tilematrix.tile_bbox(col, row, zoom)
            if prepared_geometry.intersects(geometry):
                tilelist.append((col, row))

    else:
        print "ERROR: no valid geometry"
        sys.exit(0)
    
    return tilelist
