#!/usr/bin/env python

import os
import sys
import argparse
import BaseHTTPServer
import SimpleHTTPServer
import SocketServer
import yaml

# import local modules independent from script location
scriptdir = os.path.dirname(os.path.realpath(__file__))
rootdir = os.path.split(os.path.split(scriptdir)[0])[0]
sys.path.append(os.path.join(rootdir, 'modules'))
from tilematrix import *
from tilematrix_io import *
from mapchete_commons import *

# import local modules independent from script location
scriptdir = os.path.dirname(os.path.realpath(__file__))
rootdir = os.path.split(os.path.split(scriptdir)[0])[0]
sys.path.append(os.path.join(rootdir, 'tools'))
from mapchete import mapchete

# http://localhost:8000/client/local_wmts/1.0.0/corsica_hillshade/default/WGS84/8/70/272.png

PORT = 8000

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("config", nargs=1, type=str)
    parsed = parser.parse_args(args)

    yaml_config = parsed.config[0]

    global config

    with open(yaml_config, 'r') as stream:
        config = yaml.load(stream)

    #print config
    try:
        print "hosting ..."
        run()
    except KeyboardInterrupt:
        httpd.shutdown()    


def seed_missing_data(tile):

    '''
    if "required_process" in config.keys():
        # Check if sourcedata exists. Get tile and neighbor tiles by adding
        # a pixelbuffer, loop through tiles and seed missing ones.

        ## Read input parameters.
        params = MapcheteConfig()
        params.load_from_yaml(config["required_process"])
        epsg = params.epsg
        profile = params.profile
        output_folder = params.output_folder
        metatiling = params.metatiling

        ## Create TileMatrix and MetaTileMatrix.
        tilematrix = TileMatrix(epsg)
        tilematrix.set_format(profile)
        metatilematrix = MetaTileMatrix(tilematrix, metatiling)
        extension = tilematrix.format.extension

        ## Determine metatiles to be rendered. In order to be sure no errors
        ## occure at boundaries between a seeded metatile and a not-yet seeded
        ## metatile, get all eight neighbor metatiles as well.
        zoom, row, col = tile
        tilebbox = tilematrix.tile_bbox(zoom, row, col, pixelbuffer=1)
        metatiles = metatilematrix.tiles_from_geom(tilebbox, zoom)
        tiles = []
        for metatile in metatiles:
            tiles.extend(metatilematrix.tiles_from_tilematrix(*metatile))
        for tile in tiles:
            zoom, row, col = tile
            basedir = output_folder
            zoomdir = os.path.join(basedir, str(zoom))
            rowdir = os.path.join(zoomdir, str(row))
            out_tile = tile_path(tile, params, extension)
            if os.path.exists(out_tile):
                pass
            else:
                print "seeding DEM", tile
                params.zoom = zoom
                mapchete(params, tile)
    '''

    '''
    # Process hillshade
    params = MapcheteConfig()
    params.load_from_yaml(config["process"])
    epsg = params.epsg
    profile = params.profile
    output_folder = params.output_folder
    tilematrix = TileMatrix(epsg)
    tilematrix.set_format(profile)
    extension = tilematrix.format.extension
    zoom, row, col = tile
    tilebbox = tilematrix.tile_bbox(zoom, row, col, pixelbuffer=1)
    tiles = tilematrix.tiles_from_geom(tilebbox, zoom)
    for tile in tiles:
        zoom, row, col = tile
        basedir = output_folder
        zoomdir = os.path.join(basedir, str(zoom))
        rowdir = os.path.join(zoomdir, str(row))
        out_tile = tile_path(tile, params, extension)
        if os.path.exists(out_tile):
            #print "hillshade here"
            pass
        else:
            print "seeding hillshade", tile
            params.zoom = zoom
            if os.path.splitext(params.input_files[0])[1] == ".vrt":
                params.input_files[0] = os.path.split(params.input_files[0])[0]
            input_dem = os.path.join(params.input_files[0], (str(zoom) + ".vrt"))
            params.input_files[0] = input_dem
            mapchete(params, tile)

    else:
        pass
    '''
    params = MapcheteConfig()
    params.load_from_yaml(config["process"])
    output_folder = params.output_folder
    epsg = params.epsg
    profile = params.profile
    tilematrix = TileMatrix(epsg)
    tilematrix.set_format(profile)
    extension = tilematrix.format.extension
    zoom, row, col = tile
    basedir = output_folder
    zoomdir = os.path.join(basedir, str(zoom))
    rowdir = os.path.join(zoomdir, str(row))
    out_tile = tile_path(tile, params, extension)
    if os.path.exists(out_tile):
        #print "hillshade here"
        pass
    else:
        print "seeding hillshade", tile
        params.zoom = zoom
        if os.path.splitext(params.input_files[0])[1] == ".vrt":
            params.input_files[0] = os.path.split(params.input_files[0])[0]
        input_dem = os.path.join(params.input_files[0], (str(zoom) + ".vrt"))
        params.input_files[0] = input_dem
        mapchete(params, tile)


class Handler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_GET(self):
        path = self.path[1:]
        if os.path.exists(path):
            return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)
        else:
            # split path into parts
            drive, path_and_file = os.path.splitdrive(path)
            path, file = os.path.split(path_and_file)
            folders = []
            while 1:
                path, folder = os.path.split(path)
            
                if folder != "":
                    folders.append(folder)
                else:
                    if path != "":
                        folders.append(path)            
                    break            
            folders.reverse()

            if valid(folders):
                
                # get zoom, row and col
                zoom = int(folders[-2])
                row = int(folders[-1])
                col = int(os.path.splitext(file)[0])
                tile = zoom, row, col
                try:
                    seed_missing_data(tile)
                    return SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)
                except:
                    raise
            else:
                return None


def valid(folders):
    return len(folders) != 0


def run(server_class=BaseHTTPServer.HTTPServer,
        handler_class=Handler):
    global httpd
    server_address = ('', 8000)
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()

if __name__ == "__main__":
    main(sys.argv[1:])