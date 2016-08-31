#!/usr/bin/env python
"""
Command line utility to serve a Mapchete process.
"""

import sys
import argparse
from flask import Flask, send_file, make_response, render_template_string
import threading
from PIL import Image
import io
import logging
import logging.config
import pkgutil

from mapchete import Mapchete, MapcheteConfig, get_log_config

LOGGER = logging.getLogger("mapchete")

def main(args=None):
    """
    Creates the Mapchete host and serves both web page with OpenLayers and the
    WMTS simple REST endpoint.
    """

    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("mapchete_file", type=str)
    parser.add_argument("--port", "-p", type=int, default=5000)
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument("--bounds", "-b", type=float, nargs='*')
    parser.add_argument("--log", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parsed = parser.parse_args(args)

    try:
        LOGGER.info("preparing process ...")
        mapchete = Mapchete(
            MapcheteConfig(
                parsed.mapchete_file,
                zoom=parsed.zoom,
                bounds=parsed.bounds
            )
        )
    except:
        raise

    app = Flask(__name__)

    logging.config.dictConfig(get_log_config(mapchete))
    metatile_cache = {}
    metatile_lock = threading.Lock()

    @app.route('/', methods=['GET'])
    def return_index():
        """
        Renders and hosts the appropriate OpenLayers instance.
        """
        index_html = pkgutil.get_data('static', 'index.html')
        process_bounds = mapchete.config.process_bounds
        if not process_bounds:
            process_bounds = (
                mapchete.tile_pyramid.left,
                mapchete.tile_pyramid.bottom,
                mapchete.tile_pyramid.right,
                mapchete.tile_pyramid.top
            )
        return render_template_string(
            index_html,
            srid=mapchete.tile_pyramid.srid,
            process_bounds=str(list(process_bounds)),
            is_mercator=(mapchete.tile_pyramid.srid == 3857)
        )


    tile_base_url = '/wmts_simple/1.0.0/mapchete/default/'
    if mapchete.tile_pyramid.srid == 3857:
        tile_base_url += "g/"
    else:
        tile_base_url += "WGS84/"
    @app.route(
        tile_base_url+'<int:zoom>/<int:row>/<int:col>.png',
        methods=['GET']
        )
    def get(zoom, row, col):
        """
        Returns processed, empty or error (in pink color) tile.
        """
        tile = mapchete.tile_pyramid.tilepyramid.tile(zoom, row, col)
        try:
            metatile = mapchete.tile(
                mapchete.tile_pyramid.tile(
                    tile.zoom,
                    int(tile.row/mapchete.config.metatiling),
                    int(tile.col/mapchete.config.metatiling),
                    )
                )
            with metatile_lock:
                metatile_event = metatile_cache.get(metatile.id)
                if not metatile_event:
                    metatile_cache[metatile.id] = threading.Event()

            if metatile_event:
                LOGGER.info("%s waiting for metatile %s",
                    tile.id,
                    metatile.id
                    )
                metatile_event.wait()
                try:
                    image = mapchete.get(tile)
                except:
                    raise
            else:
                LOGGER.info("%s getting metatile %s",
                    tile.id,
                    metatile.id
                    )
                try:
                    image = mapchete.get(tile, overwrite=parsed.overwrite)
                except:
                    raise
                finally:
                    with metatile_lock:
                        metatile_event = metatile_cache.get(metatile.id)
                        del metatile_cache[metatile.id]
                        metatile_event.set()

            if image:
                resp = make_response(image)
                # set no-cache header:
                resp.cache_control.no_cache = True
                LOGGER.info((tile.id, "ok", "image sent"))
                return resp
            else:
                raise IOError("no image returned")

        except Exception as exception:
            error_msg = (tile.id, "failed", exception)
            LOGGER.error(error_msg)
            size = mapchete.tile_pyramid.tilepyramid.tile_size
            empty_image = Image.new('RGBA', (size, size))
            pixels = empty_image.load()
            for y_idx in xrange(size):
                for x_idx in xrange(size):
                    pixels[x_idx, y_idx] = (255, 0, 0, 128)
            out_img = io.BytesIO()
            empty_image.save(out_img, 'PNG')
            out_img.seek(0)
            resp = make_response(send_file(out_img, mimetype='image/png'))
            resp.cache_control.no_cache = True
            return resp

    app.run(
        threaded=True,
        debug=True,
        port=parsed.port,
        extra_files=[parsed.mapchete_file]
        )


if __name__ == '__main__':
    main()
