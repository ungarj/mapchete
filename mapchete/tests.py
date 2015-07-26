#!/usr/bin/env python

import sys
from shapely.geometry import Polygon, shape
from shapely.wkt import loads
from tilematrix import *

ROUND = 10

def main(args):

    wgs84 = TileMatrix("4326")

    print "tiles for zoomlevel: %s, %s" %(wgs84.tiles_for_zoom(5))
    print "top left coordinate of tile: %s, %s" %(wgs84.top_left_tile_coords(3, 3, 5))
    print "tile boundaries: %s" %(wgs84.tile_bounds(3, 3, 5))
    bbox_polygon = loads("POLYGON ((-146.25 81.5625, 213.75 81.5625, 213.75 -98.4375, -146.25 -98.4375, -146.25 81.5625))")
    print "tiles from bbox: %s" %(tiles_from_bbox(wgs84, bbox_polygon, 2))
    polygon = loads("POLYGON ((-146.25 81.5625, 213.75 81.5625, 213.75 -98.4375, -146.25 -98.4375, -146.25 81.5625))")
    print "tiles from geometry: %s" %(tiles_from_geom(wgs84, polygon, 2))

if __name__ == "__main__":
    main(sys.argv[1:])