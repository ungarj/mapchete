"""Test Mapchete commons module."""

import numpy.ma as ma

import mapchete


def test_example_process(cleantopo_tl):
    with mapchete.open(cleantopo_tl.path) as mp:
        zoom = max(mp.config.zoom_levels)
        # tile containing data
        tile = next(mp.get_process_tiles(zoom))
        output = mp.execute(tile)
        assert isinstance(output, ma.masked_array)

        # empty tile
        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1,
        )
        output = mp.execute(tile)
        assert output.mask.all()


def test_convert(cleantopo_tl, cleantopo_landpoly_tif, landpoly):
    with mapchete.open(
        dict(
            cleantopo_tl.dict,
            input=dict(raster=cleantopo_landpoly_tif),
            process="mapchete.processes.convert"
        )
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        # execute without clip
        tile = next(mp.get_process_tiles(zoom))
        output = mp.execute(tile)
        assert isinstance(output, ma.masked_array)
        print(output)
        assert not output.mask.all()

        # execute on empty tile
        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        output = mp.execute(tile)
        assert isinstance(output, ma.masked_array)
        assert output.mask.all()

    with mapchete.open(
        dict(
            cleantopo_tl.dict,
            input=dict(raster=cleantopo_landpoly_tif, clip=landpoly),
            process="mapchete.processes.convert"
        )
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        tile = next(mp.get_process_tiles(zoom))
        output = mp.execute(tile)
        assert isinstance(output, ma.masked_array)
        assert output.mask.any()

        # execute on empty tile
        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        output = mp.execute(tile)
        assert isinstance(output, ma.masked_array)
        assert output.mask.all()


def test_contours(dem_to_contours, landpoly):
    with mapchete.open(dem_to_contours.dict) as mp:
        zoom = max(mp.config.zoom_levels)
        # execute without clip
        tile = next(mp.get_process_tiles(zoom))
        output = mp.execute(tile)
        assert isinstance(output, list)
        assert output
        # execute on empty tile
        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        output = mp.execute(tile)
        assert isinstance(output, list)
        assert not output

    with mapchete.open(
        dict(
            dem_to_contours.dict,
            input=dict(dem_to_contours.dict["input"], clip=landpoly),
            process="mapchete.processes.contours"
        )
    ) as mp:
        zoom = max(mp.config.zoom_levels)
        tile = next(mp.get_process_tiles(zoom))
        output = mp.execute(tile)
        assert isinstance(output, list)
        assert output
        # execute on empty tile
        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        output = mp.execute(tile)
        assert isinstance(output, list)
        assert not output


def test_hillshade(cleantopo_tl, cleantopo_tl_tif, landpoly):
    with mapchete.open(
        dict(
            cleantopo_tl.dict,
            input=dict(dem=cleantopo_tl_tif),
            process="mapchete.processes.hillshade"
        )
    ) as mp:
        zoom = max(mp.config.zoom_levels)

        # execute without clip
        tile = next(mp.get_process_tiles(zoom))
        output = mp.execute(tile)
        assert isinstance(output, ma.masked_array)
        assert not output.mask.all()

        # execute on empty tile
        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        output = mp.execute(tile)
        assert isinstance(output, ma.masked_array)
        assert output.mask.all()

    with mapchete.open(
        dict(
            cleantopo_tl.dict,
            input=dict(dem=cleantopo_tl_tif, clip=landpoly),
            process="mapchete.processes.hillshade"
        )
    ) as mp:
        zoom = max(mp.config.zoom_levels)

        # execute with clip
        tile = next(mp.get_process_tiles(zoom))
        output = mp.execute(tile)
        assert isinstance(output, ma.masked_array)
        assert output.mask.all()

        # execute on empty tile
        tile = mp.config.process_pyramid.tile(
            zoom,
            mp.config.process_pyramid.matrix_height(zoom) - 1,
            mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        output = mp.execute(tile)
        assert isinstance(output, ma.masked_array)
        assert output.mask.all()
