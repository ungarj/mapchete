"""Test Mapchete commons module."""

import numpy as np
import numpy.ma as ma

import mapchete
from mapchete.processes.examples import example_process
from mapchete.processes import contours, convert, hillshade


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


def test_convert_raster(cleantopo_tl, cleantopo_tl_tif, landpoly):
    with mapchete.open(dict(cleantopo_tl.dict, input=dict(inp=cleantopo_tl_tif))) as mp:
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
            mp.config.process_pyramid.matrix_width(zoom) - 1,
        )
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        assert convert.execute(user_process) == "empty"

    with mapchete.open(
        dict(cleantopo_tl.dict, input=dict(inp=cleantopo_tl_tif, clip=landpoly))
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        tile = next(mp.get_process_tiles(zoom))
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        # tile with data
        default = convert.execute(user_process)
        assert isinstance(default, np.ndarray)
        # scale_offset
        offset = convert.execute(user_process, scale_offset=2)
        assert isinstance(offset, np.ndarray)
        # scale_ratio
        ratio = convert.execute(user_process, scale_ratio=0.5)
        assert isinstance(ratio, np.ndarray)
        # clip_to_output_dtype
        clip_dtype = convert.execute(
            user_process, scale_ratio=2, clip_to_output_dtype="uint8"
        )
        assert isinstance(clip_dtype, np.ndarray)
        # execute on empty tile
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
        assert convert.execute(user_process) == "empty"


def test_convert_vector(cleantopo_tl, landpoly):
    with mapchete.open(dict(cleantopo_tl.dict, input=dict(inp=landpoly))) as mp:
        zoom = max(mp.config.zoom_levels)
        # execute without clip
        tile = next(mp.get_process_tiles(zoom))
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        assert isinstance(convert.execute(user_process), list)
        # execute on empty tile
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
        assert convert.execute(user_process) == "empty"


def test_contours(cleantopo_tl, cleantopo_tl_tif, landpoly):
    with mapchete.open(dict(cleantopo_tl.dict, input=dict(dem=cleantopo_tl_tif))) as mp:
        zoom = max(mp.config.zoom_levels)
        # execute without clip
        tile = next(mp.get_process_tiles(zoom))
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        output = contours.execute(user_process)
        assert isinstance(output, list)
        assert output
        # execute on empty tile
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
        assert contours.execute(user_process) == "empty"

    with mapchete.open(
        dict(cleantopo_tl.dict, input=dict(dem=cleantopo_tl_tif, clip=landpoly))
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        tile = next(mp.get_process_tiles(zoom))
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        output = contours.execute(user_process)
        assert isinstance(output, list)
        assert output
        # execute on empty tile
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
        assert contours.execute(user_process) == "empty"


def test_hillshade(cleantopo_tl, cleantopo_tl_tif, landpoly):
    with mapchete.open(dict(cleantopo_tl.dict, input=dict(dem=cleantopo_tl_tif))) as mp:
        zoom = max(mp.config.zoom_levels)
        # execute without clip
        tile = next(mp.get_process_tiles(zoom))
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        assert isinstance(hillshade.execute(user_process), np.ndarray)
        # execute on empty tile
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
        assert hillshade.execute(user_process) == "empty"

    with mapchete.open(
        dict(cleantopo_tl.dict, input=dict(dem=cleantopo_tl_tif, clip=landpoly))
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        tile = next(mp.get_process_tiles(zoom))
        user_process = mapchete.MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(tile.zoom),
            input=mp.config.get_inputs_for_tile(tile),
        )
        assert isinstance(hillshade.execute(user_process), np.ndarray)
        # execute on empty tile
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
        assert hillshade.execute(user_process) == "empty"
