"""Test Mapchete commons module."""

import numpy as np
import numpy.ma as ma

import mapchete
from mapchete.processes.examples import example_process
from mapchete.processes import convert


def test_example_process(cleantopo_tl):
    with mapchete.open(cleantopo_tl.path) as mp:
        zoom = max(mp.config.zoom_levels)
        # tile containing data
        tile = next(mp.get_process_tiles(zoom))
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        output = example_process.execute(user_process)
        assert isinstance(output, ma.masked_array)
        # empty tile

        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1,
        )
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        output = example_process.execute(user_process)
        assert output == "empty"


def test_convert(cleantopo_tl, cleantopo_tl_tif, landpoly):
    with mapchete.open(
        dict(cleantopo_tl.dict, input=dict(raster=cleantopo_tl_tif))
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        # execute without clip
        tile = next(mp.get_process_tiles(zoom))
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        assert isinstance(convert.execute(user_process), np.ndarray)
        # execute on empty tile
        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        assert convert.execute(user_process) == "empty"

    with mapchete.open(
        dict(cleantopo_tl.dict, input=dict(raster=cleantopo_tl_tif, clip=landpoly))
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        tile = next(mp.get_process_tiles(zoom))
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        assert isinstance(convert.execute(user_process), np.ndarray)
        # execute on empty tile
        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        assert convert.execute(user_process) == "empty"
