#!/usr/bin/env python
"""Command line utility to serve a Mapchete process."""

import logging
import logging.config
import os
import pkgutil
from rasterio.io import MemoryFile
import six
from flask import (
    Flask, send_file, make_response, render_template_string, abort, jsonify)

import mapchete
from mapchete.tile import BufferedTilePyramid

formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.ERROR)
logging.getLogger().addHandler(stream_handler)

logger = logging.getLogger(__name__)


def main(args=None, _test=False):
    """
    Serve a Mapchete process.

    Creates the Mapchete host and serves both web page with OpenLayers and the
    WMTS simple REST endpoint.
    """
    app = create_app(
        mapchete_files=[args.mapchete_file], zoom=args.zoom,
        bounds=args.bounds, single_input_file=args.input_file,
        mode=_get_mode(args), debug=args.debug)
    if not _test:
        app.run(
            threaded=True, debug=True, port=args.port, host='0.0.0.0',
            extra_files=[args.mapchete_file])


def create_app(
    mapchete_files=None, zoom=None, bounds=None, single_input_file=None,
    mode="continue", debug=None
):
    """Configure and create Flask app."""
    if debug:
        logging.getLogger("mapchete").setLevel(logging.DEBUG)
        stream_handler.setLevel(logging.DEBUG)

    app = Flask(__name__)
    mapchete_processes = {
        os.path.splitext(os.path.basename(mapchete_file))[0]: mapchete.open(
            mapchete_file, zoom=zoom, bounds=bounds,
            single_input_file=single_input_file, mode=mode, with_cache=True,
            debug=debug)
        for mapchete_file in mapchete_files
    }

    mp = next(six.iteritems(mapchete_processes))[1]
    pyramid_type = mp.config.process_pyramid.grid
    pyramid_srid = mp.config.process_pyramid.srid
    process_bounds = ",".join([str(i) for i in mp.config.bounds_at_zoom()])
    grid = "g" if pyramid_srid == 3857 else "WGS84"
    web_pyramid = BufferedTilePyramid(pyramid_type)

    @app.route('/', methods=['GET'])
    def index():
        """Render and hosts the appropriate OpenLayers instance."""
        return render_template_string(
            pkgutil.get_data(
                'mapchete.static', 'index.html').decode("utf-8"),
            srid=pyramid_srid,
            process_bounds=process_bounds,
            is_mercator=(pyramid_srid == 3857),
            process_names=six.iterkeys(mapchete_processes)
        )

    @app.route(
        "/".join([
            "", "wmts_simple", "1.0.0", "<string:mp_name>", "default",
            grid, "<int:zoom>", "<int:row>", "<int:col>.<string:file_ext>"]),
        methods=['GET'])
    def get(mp_name, zoom, row, col, file_ext):
        """Return processed, empty or error (in pink color) tile."""
        logger.debug(
            "received tile (%s, %s, %s) for process %s", zoom, row, col,
            mp_name)
        # convert zoom, row, col into tile object using web pyramid
        return _tile_response(
            mapchete_processes[mp_name], web_pyramid.tile(zoom, row, col),
            debug)

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
        logger.debug("getting web tile %s", str(web_tile.id))
        return _valid_tile_response(mp, mp.get_raw_output(web_tile))
    except Exception:
        logger.exception("getting web tile %s failed", str(web_tile.id))
        if debug:
            raise
        else:
            abort(500)


def _valid_tile_response(mp, data):
    out_data, mime_type = mp.config.output.for_web(data)
    logger.debug("create tile response %s", mime_type)
    if isinstance(out_data, MemoryFile):
        response = make_response(send_file(out_data, mime_type))
    elif isinstance(out_data, list):
        response = make_response(jsonify(data))
    else:
        raise TypeError("invalid response type for web")
    response.headers['Content-Type'] = mime_type
    response.cache_control.no_write = True
    return response
