#!/usr/bin/env python
"""Command line utility to serve a Mapchete process."""

import os
import io
import logging
import logging.config
import pkgutil
from PIL import Image, ImageDraw
from flask import Flask, send_file, make_response, render_template_string

from mapchete import Mapchete
from mapchete.config import MapcheteConfig
from mapchete.log import get_log_config
from mapchete.tile import BufferedTilePyramid

LOGGER = logging.getLogger("mapchete")


def main(args=None):
    """
    Serve a Mapchete process.

    Creates the Mapchete host and serves both web page with OpenLayers and the
    WMTS simple REST endpoint.
    """
    parsed = args

    try:
        assert os.path.splitext(parsed.mapchete_file)[1] == ".mapchete"
    except:
        raise IOError("must be a valid mapchete file")

    LOGGER.info("preparing process ...")
    process = Mapchete(
        MapcheteConfig(
            parsed.mapchete_file, zoom=parsed.zoom, bounds=parsed.bounds,
            single_input_file=parsed.input_file, mode=_get_mode(parsed)),
        with_cache=True
        )

    app = Flask(__name__)
    web_pyramid = BufferedTilePyramid(process.config.raw["output"]["type"])

    logging.config.dictConfig(get_log_config(process))

    @app.route('/', methods=['GET'])
    def return_index():
        """Render and hosts the appropriate OpenLayers instance."""
        return render_template_string(
            pkgutil.get_data('mapchete.static', 'index.html'),
            srid=process.config.process_pyramid.srid,
            process_bounds=",".join(map(str, process.config.process_bounds())),
            is_mercator=(process.config.process_pyramid.srid == 3857)
        )

    tile_base_url = '/wmts_simple/1.0.0/mapchete/default/'
    is_mercator = process.config.process_pyramid.srid == 3857
    tile_base_url += "g/" if is_mercator else "WGS84/"

    @app.route(
        tile_base_url+'<int:zoom>/<int:row>/<int:col>.png', methods=['GET'])
    def get(zoom, row, col):
        """Return processed, empty or error (in pink color) tile."""
        # convert zoom, row, col into tile object using web pyramid
        web_tile = web_pyramid.tile(zoom, row, col)
        return _tile_response(process, web_tile)

    app.run(
        threaded=True, debug=True, port=parsed.port,
        extra_files=[parsed.mapchete_file]
        )


def _get_mode(parsed):
    if parsed.memory:
        return "memory"
    elif parsed.readonly:
        return "readonly"
    elif parsed.overwrite:
        return "overwrite"
    else:
        return "continue"


def _tile_response(process, web_tile):
    if process.config.mode in ["continue", "readonly"]:
        if (
            web_tile.tile_pyramid.metatiling ==
            process.config.raw["output"]["metatiling"]
        ):
            try:
                response = make_response(
                    send_file(process.config.output.get_path(web_tile)))
                response.cache_control.no_write = True
                return response
            except Exception:
                return _error_tile_response(process, web_tile)
    try:
        return _valid_tile_response(
            process, process.get_raw_output(web_tile))
    except Exception as e:
        LOGGER.info(
            (process.process_name, "web tile", web_tile.id, "error", e))
        return _error_tile_response(process, web_tile)


def _valid_tile_response(process, web_tile):
    response = make_response(process.config.output.for_web(web_tile.data))
    response.cache_control.no_write = True
    return response


def _error_tile_response(process, web_tile):
    if process.config.output.METADATA["data_type"] == "raster":
        empty_image = Image.new('RGBA', web_tile.shape)
        draw = ImageDraw.Draw(empty_image)
        draw.rectangle([(0, 0), web_tile.shape], fill=(255, 0, 0, 128))
        del draw
        out_img = io.BytesIO()
        empty_image.save(out_img, 'PNG')
        out_img.seek(0)
        resp = make_response(send_file(out_img, mimetype='image/png'))
        resp.cache_control.no_cache = True
        return resp

    elif process.config.output.METADATA["data_type"] == "vector":
        raise NotImplementedError


if __name__ == '__main__':
    main()
