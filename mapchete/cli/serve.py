#!/usr/bin/env python
"""Command line utility to serve a Mapchete process."""

import os
import logging
import logging.config
import pkgutil
from traceback import format_exc
from flask import Flask, make_response, render_template_string, abort

import mapchete
from mapchete.tile import BufferedTilePyramid

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def main(args=None, _test=False):
    """
    Serve a Mapchete process.

    Creates the Mapchete host and serves both web page with OpenLayers and the
    WMTS simple REST endpoint.
    """
    app = create_app(args)
    if not _test:
        app.run(
            threaded=True, debug=True, port=args.port,
            extra_files=[args.mapchete_file])


def create_app(args):
    """Configure and create Flask app."""
    if not os.path.splitext(args.mapchete_file)[1] == ".mapchete":
        raise IOError("must be a valid mapchete file")

    if args.debug:
        LOGGER.setLevel(logging.DEBUG)

    mp = mapchete.open(
        args.mapchete_file, zoom=args.zoom, bounds=args.bounds,
        single_input_file=args.input_file, mode=_get_mode(args),
        with_cache=True, debug=args.debug
    )

    app = Flask(__name__)
    web_pyramid = BufferedTilePyramid(mp.config.raw["output"]["type"])

    @app.route('/', methods=['GET'])
    def index():
        """Render and hosts the appropriate OpenLayers instance."""
        return render_template_string(
            pkgutil.get_data('mapchete.static', 'index.html'),
            srid=mp.config.process_pyramid.srid,
            process_bounds=",".join([
                str(i) for i in mp.config.process_bounds()]),
            is_mercator=(mp.config.process_pyramid.srid == 3857)
        )

    tile_base_url = '/wmts_simple/1.0.0/mapchete/default/'
    is_mercator = mp.config.process_pyramid.srid == 3857
    tile_base_url += "g/" if is_mercator else "WGS84/"

    @app.route(
        tile_base_url+'<int:zoom>/<int:row>/<int:col>.png', methods=['GET'])
    def get(zoom, row, col):
        """Return processed, empty or error (in pink color) tile."""
        # convert zoom, row, col into tile object using web pyramid
        web_tile = web_pyramid.tile(zoom, row, col)
        return _tile_response(mp, web_tile, args.debug)

    return app


def _get_mode(parsed):
    if parsed.memory:
        return "memory"
    elif parsed.readonly:
        return "readonly"
    elif parsed.overwrite:
        return "overwrite"
    else:
        return "continue"


def _tile_response(mp, web_tile, debug):
    try:
        LOGGER.debug("getting web tile %s" % str(web_tile.id))
        return _valid_tile_response(mp, mp.get_raw_output(web_tile))
    except Exception:
        if debug:
            LOGGER.debug("getting web tile %s failed" % str(web_tile.id))
            for line in format_exc().split("\n"):
                LOGGER.error(line)
            raise
        else:
            abort(500)


def _valid_tile_response(mp, web_tile):
    response = make_response(mp.config.output.for_web(web_tile.data))
    response.cache_control.no_write = True
    return response


if __name__ == '__main__':
    main()
