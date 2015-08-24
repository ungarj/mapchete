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

    zoom, row, col = tile

    if zoom < 7:
        return None

    params = MapcheteConfig()
    params.load_from_yaml(config["process"])
    output_folder = params.output_folder
    epsg = params.epsg
    profile = params.profile
    tilematrix = TileMatrix(epsg)
    tilematrix.set_format(profile)
    metatilematrix = MetaTileMatrix(tilematrix, params.metatiling)
    extension = tilematrix.format.extension
    basedir = output_folder
    zoomdir = os.path.join(basedir, str(zoom))
    rowdir = os.path.join(zoomdir, str(row))
    out_tile = tile_path(tile, params, extension)
    # Check if target tile exists.
    if os.path.exists(out_tile):
        pass
    else:
        if "required_process" in config.keys():
            # Check if sourcedata exists. Get metatile and neighbor metatiles
            # by adding a pixelbuffer. Loop through metatiles and check whether
            # all tiles exist. If not, add them to a TBD list and let mapchete
            # seed it.
    
            ## Read input parameters.
            req_proc_params = MapcheteConfig()
            req_proc_params.load_from_yaml(config["required_process"])
            epsg = req_proc_params.epsg
            profile = req_proc_params.profile
            output_folder = req_proc_params.output_folder
            metatiling = req_proc_params.metatiling
    
            ## Create TileMatrix and MetaTileMatrix.
            req_proc_tilematrix = TileMatrix(epsg)
            req_proc_tilematrix.set_format(profile)
            req_proc_metatilematrix = MetaTileMatrix(req_proc_tilematrix, metatiling)
            extension = req_proc_tilematrix.format.extension
    
            ## Determine metatiles to be rendered. In order to be sure no errors
            ## occure at boundaries between a seeded metatile and a not-yet seeded
            ## metatile, get all eight neighbor metatiles as well.
            ### Get metatile in which input tile belongs.
            tile_bbox = tilematrix.tile_bbox(*tile)
            metatile = metatilematrix.tiles_from_bbox(tile_bbox, zoom)[0]
            ### Determine metatiles of required data using metatile from input
            ### tile including a buffer to get neighbor metatiles as well.
            metatile_bbox = metatilematrix.tile_bbox(*metatile, pixelbuffer=1)
            metatiles = req_proc_metatilematrix.tiles_from_geom(metatile_bbox, zoom)
            tbd_metatiles = []
            for metatile in metatiles:
                tiles = req_proc_metatilematrix.tiles_from_tilematrix(*metatile)
                process = False
                for out_tile in tiles:
                    out_tile_path = tile_path(out_tile, req_proc_params, extension)
                    if not os.path.exists(out_tile_path):
                        process = True
                if process == True:
                    tbd_metatiles.append(metatile)
            if len(tbd_metatiles)>0:
                # Seed required data.
                print "Seeding required data.", tbd_metatiles
                req_proc_params.zoom = zoom
                mapchete(req_proc_params, metatiles=tbd_metatiles)

        print "Seeding hillshade.", tile
        if os.path.splitext(params.input_files[0])[1] == ".vrt":
            params.input_files[0] = os.path.split(params.input_files[0])[0]
        input_dem = os.path.join(params.input_files[0], (str(zoom) + ".vrt"))
        params.input_files[0] = input_dem
        mapchete(params, tiles=[tile])


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