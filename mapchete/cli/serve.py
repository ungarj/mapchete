#!/usr/bin/env python
"""Command line utility to serve a Mapchete process."""

import os
import argparse
import threading
import io
import logging
import logging.config
import pkgutil
from PIL import Image, ImageDraw
from flask import Flask, send_file, make_response, render_template_string
from cachetools import LRUCache
from tilematrix import TilePyramid, Tile

from mapchete import Mapchete
from mapchete.config import MapcheteConfig
from mapchete.log import get_log_config
from mapchete.io import raster
from mapchete.tile import BufferedTile

LOGGER = logging.getLogger("mapchete")


def main(args=None):
    """
    Serve a Mapchete process.

    Creates the Mapchete host and serves both web page with OpenLayers and the
    WMTS simple REST endpoint.
    """
    if isinstance(args, argparse.Namespace):
        parsed = args
    else:
        raise RuntimeError("invalid arguments for mapchete serve")

    try:
        assert os.path.splitext(parsed.mapchete_file)[1] == ".mapchete"
    except:
        raise IOError("must be a valid mapchete file")

    try:
        LOGGER.info("preparing process ...")
        process = Mapchete(
            MapcheteConfig(
                parsed.mapchete_file,
                zoom=parsed.zoom,
                bounds=parsed.bounds,
                single_input_file=parsed.input_file
            )
        )
    except:
        raise

    app = Flask(__name__)
    web_pyramid = TilePyramid(process.config.raw["output"]["type"])

    logging.config.dictConfig(get_log_config(process))
    output_tile_locker = {}
    output_tile_lock = threading.Lock()
    output_tile_cache = LRUCache(maxsize=parsed.internal_cache)

    @app.route('/', methods=['GET'])
    def return_index():
        """Render and hosts the appropriate OpenLayers instance."""
        index_html = pkgutil.get_data('mapchete.static', 'index.html')
        process_bounds = process.config.process_bounds()
        if not process_bounds:
            process_bounds = (
                process.config.process_pyramid.left,
                process.config.process_pyramid.bottom,
                process.config.process_pyramid.right,
                process.config.process_pyramid.top)
        return render_template_string(
            index_html, srid=process.config.process_pyramid.srid,
            process_bounds=",".join(map(str, process_bounds)),
            is_mercator=(process.config.process_pyramid.srid == 3857)
        )
    tile_base_url = '/wmts_simple/1.0.0/mapchete/default/'
    if process.config.process_pyramid.srid == 3857:
        tile_base_url += "g/"
    else:
        tile_base_url += "WGS84/"

    @app.route(
        tile_base_url+'<int:zoom>/<int:row>/<int:col>.png', methods=['GET'])
    def get(zoom, row, col):
        """Return processed, empty or error (in pink color) tile."""
        print output_tile_cache
        # convert zoom, row, col into tile object using web pyramid
        web_tile = web_pyramid.tile(zoom, row, col)
        output_tile_id = process.config.output_pyramid.intersecting(
            web_tile)[0].id
        if output_tile_id in output_tile_cache:
            return _valid_tile_response(
                web_tile, output_tile_cache[output_tile_id])
        try:
            # get output_tile id and wait if locked
            with output_tile_lock:
                output_tile_event = output_tile_locker.get(output_tile_id)
                # if not locked, lock output_tile id
                if not output_tile_event:
                    output_tile_locker[output_tile_id] = threading.Event()
            if output_tile_event:
                # wait if output_tile is locked and return web tile when ready
                LOGGER.info(
                    "web tile %s waiting for output tile %s" %
                    (web_tile.id, output_tile_id))
                output_tile_event.wait()
                return _valid_tile_response(
                    web_tile, output_tile_cache[output_tile_id])
            else:
                LOGGER.info(
                    "web tile %s getting output tile %s" %
                    (web_tile.id, output_tile_id))
                try:
                    output_tile = process.get_raw_output(
                        output_tile_id, overwrite=parsed.overwrite,
                        no_write=True)
                    output_tile_cache[output_tile_id] = output_tile
                    return _valid_tile_response(
                        web_tile, output_tile_cache[output_tile_id])
                except:
                    raise
                finally:
                    with output_tile_lock:
                        output_tile_event = output_tile_locker.get(
                            output_tile_id)
                        del output_tile_locker[output_tile_id]
                        output_tile_event.set()
        except Exception as e:
            raise
            LOGGER.info(
                "web tile %s error: %s" %
                (web_tile.id, e))
            return _error_tile_response(web_tile)

    def _valid_tile_response(web_tile, output_tile):
        data = process.config.output.for_web(
            _data_window_from_output(web_tile, output_tile))
        response = make_response(data)
        response.cache_control.no_write = True
        return response

    def _error_tile_response(web_tile):
        if process.config.output.METADATA["data_type"] == "raster":
            empty_image = Image.new('RGBA', web_tile.shape())
            draw = ImageDraw.Draw(empty_image)
            draw.rectangle([(0, 0), web_tile.shape()], fill=(255, 0, 0, 128))
            del draw
            out_img = io.BytesIO()
            empty_image.save(out_img, 'PNG')
            out_img.seek(0)
            resp = make_response(send_file(out_img, mimetype='image/png'))
            resp.cache_control.no_cache = True
            return resp

        elif process.config.output.METADATA["data_type"] == "vector":
            raise NotImplementedError

    def _data_window_from_output(web_tile, output_tile):
        if isinstance(web_tile, Tile):
            web_tile = BufferedTile(web_tile)
        if process.config.output.METADATA["data_type"] == "raster":
            return raster.extract_from_tile(output_tile, web_tile)
        elif process.config.output.METADATA["data_type"] == "vector":
            raise NotImplementedError

    app.run(
        threaded=True,
        debug=True,
        port=parsed.port,
        extra_files=[parsed.mapchete_file]
        )
    web_tile_cache.flush_all()

if __name__ == '__main__':
    main()
