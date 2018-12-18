"""Test Mapchete commons module."""

import numpy as np
import numpy.ma as ma
import rasterio

import mapchete
from mapchete.processes.examples import example_process
from mapchete.processes.pyramid import tilify
from mapchete.processes import convert


def test_example_process(cleantopo_tl):
    with mapchete.open(cleantopo_tl.path) as mp:
        zoom = max(mp.config.zoom_levels)
        # tile containing data
        process_tile = next(mp.get_process_tiles(zoom))
        tile_process = mapchete.MapcheteProcess(
            process_tile, mp.config, mp.config.params_at_zoom(zoom)
        )
        output = example_process.execute(tile_process)
        assert isinstance(output, ma.masked_array)
        # empty tile

        process_tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1,
        )
        tile_process = mapchete.MapcheteProcess(
            process_tile, mp.config, mp.config.params_at_zoom(zoom)
        )
        output = example_process.execute(tile_process)
        assert output == "empty"


def test_tilify(cleantopo_tl, cleantopo_tl_tif):
    with mapchete.open(
        dict(cleantopo_tl.dict, input=dict(raster=cleantopo_tl_tif))
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        # tile containing data
        process_tile = next(mp.get_process_tiles(zoom))
        tile_process = mapchete.MapcheteProcess(
            process_tile, mp.config, mp.config.params_at_zoom(zoom)
        )
        # no scaling
        not_scaled = tilify.execute(tile_process)
        assert isinstance(not_scaled, ma.masked_array)
        # dtype_scale
        dtype_scaled = tilify.execute(
            tile_process, scale_method="dtype_scale", scales_minmax=((0, 10000), )
        )
        assert isinstance(dtype_scaled, ma.masked_array)
        assert not np.array_equal(not_scaled, dtype_scaled)
        assert dtype_scaled.min() == 167
        assert dtype_scaled.max() == 247
        # minmax_scale
        with rasterio.open(cleantopo_tl_tif) as src:
            data_minmax = tuple((band.min(), band.max()) for band in src.read())
        minmax_scaled = tilify.execute(
            tile_process, scale_method="minmax_scale", scales_minmax=data_minmax
        )
        assert isinstance(minmax_scaled, ma.masked_array)
        assert minmax_scaled.min() == 0
        assert minmax_scaled.max() == 255

        # crop
        cropped = tilify.execute(
            tile_process, scale_method="crop", scales_minmax=((7000, 8000), )
        )
        assert isinstance(cropped, ma.masked_array)
        assert cropped.min() == 7000
        assert cropped.max() == 8000

        # empty tile
        process_tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1,
        )
        tile_process = mapchete.MapcheteProcess(
            process_tile, mp.config, mp.config.params_at_zoom(zoom)
        )
        output = tilify.execute(tile_process)
        assert output == "empty"


def test_convert(cleantopo_tl, cleantopo_tl_tif, landpoly):
    with mapchete.open(
        dict(cleantopo_tl.dict, input=dict(raster=cleantopo_tl_tif))
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        # execute without clip
        process_tile = next(mp.get_process_tiles(zoom))
        tile_process = mapchete.MapcheteProcess(
            process_tile, mp.config, mp.config.params_at_zoom(zoom)
        )
        assert isinstance(convert.execute(tile_process), np.ndarray)
        # execute on empty tile
        process_tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        tile_process = mapchete.MapcheteProcess(
            process_tile, mp.config, mp.config.params_at_zoom(zoom)
        )
        assert convert.execute(tile_process) == "empty"

    with mapchete.open(
        dict(
            cleantopo_tl.dict,
            input=dict(raster=cleantopo_tl_tif, clip=landpoly)
        )
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        process_tile = next(mp.get_process_tiles(zoom))
        tile_process = mapchete.MapcheteProcess(
            process_tile, mp.config, mp.config.params_at_zoom(zoom)
        )
        assert isinstance(convert.execute(tile_process), np.ndarray)
        # execute on empty tile
        process_tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        tile_process = mapchete.MapcheteProcess(
            process_tile, mp.config, mp.config.params_at_zoom(zoom)
        )
        assert convert.execute(tile_process) == "empty"
