#!/usr/bin/env python
"""Command line utility to serve a Mapchete process."""

import logging
import logging.config
import os
import pkgutil

import click
from rasterio.io import MemoryFile

import mapchete
from mapchete.cli import options
from mapchete.io import MPath
from mapchete.tile import BufferedTilePyramid

logger = logging.getLogger(__name__)


@click.command(help="Serve a process on localhost.")
@options.arg_mapchete_files
@options.opt_port
@options.opt_internal_cache
@options.opt_zoom
@options.opt_bounds
@options.opt_overwrite
@options.opt_readonly
@options.opt_memory
@options.opt_input_file
@options.opt_debug
@options.opt_logfile
def serve(
    mapchete_files,
    port=None,
    internal_cache=None,
    zoom=None,
    bounds=None,
    overwrite=False,
    readonly=False,
    memory=False,
    input_file=None,
    debug=False,
    logfile=None,
):
    """
    Serve a Mapchete process.

    Creates the Mapchete host and serves both web page with OpenLayers and the
    WMTS simple REST endpoint.
    """
    app = create_app(
        mapchete_files=mapchete_files,
        zoom=zoom,
        bounds=bounds,
        single_input_file=input_file,
        mode=_get_mode(memory, readonly, overwrite),
        debug=debug,
    )
    if os.environ.get("MAPCHETE_TEST") == "TRUE":
        logger.debug("don't run flask app, MAPCHETE_TEST environment detected")
    else:  # pragma: no cover
        app.run(
            threaded=True,
            debug=debug,
            port=port,
            host="0.0.0.0",
            extra_files=mapchete_files,
        )


def create_app(
    mapchete_files=None,
    zoom=None,
    bounds=None,
    single_input_file=None,
    mode="continue",
    debug=None,
):
    """Configure and create Flask app."""
    from flask import Flask, render_template_string

    app = Flask(__name__)
    mapchete_processes = {
        str(
            MPath.from_inp(MPath.from_inp(mapchete_file).name).without_suffix()
        ): mapchete.open(
            mapchete_file,
            zoom=zoom,
            bounds=bounds,
            single_input_file=single_input_file,
            mode=mode,
            with_cache=True,
            debug=debug,
        )
        for mapchete_file in mapchete_files
    }

    mp = next(iter(mapchete_processes.values()))
    pyramid_type = mp.config.process_pyramid.grid
    pyramid_srid = mp.config.process_pyramid.crs.to_epsg()
    process_bounds = ",".join([str(i) for i in mp.config.bounds_at_zoom()])
    grid = "g" if pyramid_srid == 3857 else "WGS84"
    web_pyramid = BufferedTilePyramid(pyramid_type)

    @app.route("/", methods=["GET"])
    def index():
        """Render and hosts the appropriate OpenLayers instance."""
        return render_template_string(
            pkgutil.get_data("mapchete.static", "index.html").decode("utf-8"),
            srid=pyramid_srid,
            process_bounds=process_bounds,
            is_mercator=(pyramid_srid == 3857),
            process_names=mapchete_processes.keys(),
        )

    @app.route(
        "/".join(
            [
                "",
                "wmts_simple",
                "1.0.0",
                "<string:mp_name>",
                "default",
                grid,
                "<int:zoom>",
                "<int:row>",
                "<int:col>.<string:file_ext>",
            ]
        ),
        methods=["GET"],
    )
    def get(mp_name, zoom, row, col, file_ext):
        """Return processed, empty or error (in pink color) tile."""
        logger.debug(
            "received tile (%s, %s, %s) for process %s", zoom, row, col, mp_name
        )
        # convert zoom, row, col into tile object using web pyramid
        return _tile_response(
            mapchete_processes[mp_name], web_pyramid.tile(zoom, row, col), debug
        )

    return app


def _get_mode(memory, readonly, overwrite):
    if memory:
        return "memory"
    elif readonly:
        return "readonly"
    elif overwrite:
        return "overwrite"
    else:
        return "continue"


def _tile_response(mp, web_tile, debug):
    try:
        logger.debug("getting web tile %s", str(web_tile.id))
        return _valid_tile_response(mp, mp.get_raw_output(web_tile))
    except Exception:  # pragma: no cover
        logger.exception("getting web tile %s failed", str(web_tile.id))
        if debug:
            raise
        else:
            from flask import abort

            abort(500)


def _valid_tile_response(mp, data):
    from flask import jsonify, make_response
    from flask_rangerequest import RangeRequest

    out_data, mime_type = mp.config.output.for_web(data)
    logger.debug("create tile response %s", mime_type)
    if isinstance(out_data, MemoryFile):
        return RangeRequest(out_data).make_response()
    elif isinstance(out_data, list):
        response = make_response(jsonify(data))
    else:  # pragma: no cover
        response = make_response(out_data)
    response.headers["Content-Type"] = mime_type
    response.cache_control.no_write = True
    return response
