#!/usr/bin/env python

import os
import sys
import argparse
from flask import Flask

from mapchete import *
from tilematrix import TilePyramid, MetaTilePyramid

process_host = None

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("mapchete_file", type=str)
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument("--bounds", "-b", type=float, nargs='*')
    parser.add_argument("--log", action="store_true")
    parsed = parser.parse_args(args)

    try:
        print "preparing process ..."
        process_host = MapcheteHost(
            parsed.mapchete_file,
            zoom=parsed.zoom,
            bounds=parsed.bounds
            )
    except Exception as e:
        raise

    app = Flask(__name__)

    @app.route('/', methods=['GET'])
    def get_tasks():
        return "herbert"

    @app.route('/wmts_simple', methods=['GET'])
    def get_getcapabilities():
        return "getcapabilities"

    @app.route('/wmts_simple/<string:process_id>', methods=['GET'])
    def get_processname(process_id):
        return process_id

    @app.route('/wmts_simple/<string:process_id>/<int:zoom>/<int:row>/<int:col>.png', methods=['GET'])
    def get_tile(process_id, zoom, row, col):
        # return str(zoom), str(row), str(col)
        tileindex = str(zoom), str(row), str(col)
        tile = (zoom, row, col)
        return process_host.get_tile(tile)
        # return str(tileindex)

    app.run(debug=True)

if __name__ == '__main__':
    main(sys.argv[1:])
