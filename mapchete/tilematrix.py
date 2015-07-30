#!/usr/bin/env python

import sys
from shapely.geometry import *
from shapely.validation import *
from shapely.prepared import prep
from itertools import product

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
        wetiles, nstiles = self.tiles_for_zoom(zoom)
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        tile_wesize = float(round(self.wesize/wetiles, ROUND))
        tile_nssize = float(round(self.nssize/nstiles, ROUND))
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
        tile_wesize, tile_nssize = self.tilesize_for_zoom(zoom)
        ul = self.top_left_tile_coords(col, row, zoom)
        ur = ul[0] + tile_wesize, ul[1]
        lr = ul[0] + tile_wesize, ul[1] - tile_nssize
        ll = ul[0], ul[1] - tile_nssize
    
        return Polygon([ul, ur, lr, ll])


    def tiles_from_bbox(self, geometry, zoom):
        try:
            assert isinstance(zoom, int)
        except:
            print "Zoom (%s) must be an integer." %(zoom)
            sys.exit(0)
        tile_wesize, tile_nssize = self.tilesize_for_zoom(zoom)
        tilelist = []
        l, b, r, t = geometry.bounds
        tilelon = self.left
        tilelat = self.top
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
    
    
    def tiles_from_geom(self, geometry, zoom):
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
    
            tilelist = self.tiles_from_bbox(geometry, zoom)

    
        elif geometry.geom_type == "Point":
    
            lon, lat = list(geometry.coords)[0]
    
            tilelon = self.left
            tilelat = self.top
    
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
            bbox_tilelist = self.tiles_from_bbox(geometry, zoom)  
            for tile in bbox_tilelist:
                col, row = tile
                geometry = self.tile_bounds(col, row, zoom)
                if prepared_geometry.intersects(geometry):
                    tilelist.append((col, row))
    
        else:
            print "ERROR: no valid geometry"
            sys.exit(0)
    
        return tilelist