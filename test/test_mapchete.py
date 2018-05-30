#!/usr/bin/env python
"""Test Mapchete main module and processing."""

import pytest
import os
import shutil
import rasterio
import numpy as np
import numpy.ma as ma
import pkg_resources
try:
    from cPickle import dumps
except ImportError:
    from pickle import dumps
from functools import partial
from multiprocessing import Pool
from shapely.geometry import shape

import mapchete
from mapchete.io.raster import create_mosaic
from mapchete.errors import MapcheteProcessOutputError


def test_empty_execute(mp_tmpdir, cleantopo_br):
    """Execute process outside of defined zoom levels."""
    with mapchete.open(cleantopo_br.path) as mp:
        assert mp.execute((6, 0, 0)).mask.all()


def test_read_existing_output(mp_tmpdir, cleantopo_tl):
    """Read existing process output."""
    # raster data
    tile = (5, 0, 0)
    with mapchete.open(cleantopo_tl.path) as mp:
        # process and save
        mp.get_raw_output(tile)
        # read written data from within MapcheteProcess object
        mp_tile = mapchete.MapcheteProcess(
            mp.config.process_pyramid.tile(*tile),
            config=mp.config,
            params=mp.config.params_at_zoom(5)
        )
        data = mp_tile.read()
        assert data.any()
        assert isinstance(data, ma.masked_array)
        assert not data.mask.all()
        # read data from Mapchete class
        data = mp.read(tile)
        assert data.any()
        assert isinstance(data, ma.masked_array)
        assert not data.mask.all()


def test_read_existing_output_buffer(mp_tmpdir, cleantopo_tl):
    """Read existing process output with process buffer."""
    # raster data process buffer > output buffer
    config = cleantopo_tl.dict
    config["output"].update(pixelbuffer=0)
    with mapchete.open(config) as mp:
        tile = next(mp.get_process_tiles(5))
        # process and save
        mp.get_raw_output(tile)
        # read written data from within MapcheteProcess object
        mp_tile = mapchete.MapcheteProcess(
            mp.config.process_pyramid.tile(*tile.id),
            config=mp.config,
            params=mp.config.params_at_zoom(5))
        data = mp_tile.read()
        assert data.any()
        assert isinstance(data, ma.masked_array)
        assert not data.mask.all()


def test_read_existing_output_vector(mp_tmpdir, geojson):
    """Read existing process output with process buffer."""
    with mapchete.open(geojson.path) as mp:
        tile = next(mp.get_process_tiles(4))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read written data from within MapcheteProcess object
        mp_tile = mapchete.MapcheteProcess(
            tile,
            config=mp.config,
            params=mp.config.params_at_zoom(4)
        )
        data = mp_tile.read()
        assert data
        for feature in data:
            assert shape(feature["geometry"]).is_valid


def test_open_data_error(cleantopo_tl):
    """Try to open data not specified as input."""
    tile = (5, 0, 0)
    with mapchete.open(cleantopo_tl.path) as mp:
        # read written data from within MapcheteProcess object
        mp_tile = mapchete.MapcheteProcess(
            mp.config.process_pyramid.tile(*tile),
            config=mp.config,
            params=mp.config.params_at_zoom(5)
        )
        with pytest.raises(ValueError):
            mp_tile.open("invaild_input_id")


def test_get_raw_output_outside(mp_tmpdir, cleantopo_br):
    """Get raw process output outside of zoom levels."""
    with mapchete.open(cleantopo_br.path) as mp:
        assert mp.get_raw_output((6, 0, 0)).mask.all()


def test_get_raw_output_memory(mp_tmpdir, cleantopo_tl):
    """Get raw process output using memory flag."""
    with mapchete.open(cleantopo_tl.path, mode="memory") as mp:
        assert mp.config.mode == "memory"
        assert not mp.get_raw_output((5, 0, 0)).mask.all()


def test_get_raw_output_readonly(mp_tmpdir, cleantopo_tl):
    """Get raw process output using readonly flag."""
    tile = (5, 0, 0)
    readonly_mp = mapchete.open(cleantopo_tl.path, mode="readonly")
    write_mp = mapchete.open(cleantopo_tl.path, mode="continue")

    # read non-existing data (returns empty)
    assert readonly_mp.get_raw_output(tile).mask.all()

    # try to process and save empty data
    with pytest.raises(ValueError):
        readonly_mp.write(tile, readonly_mp.get_raw_output(tile))

    # actually process and save
    write_mp.write(tile, write_mp.get_raw_output(tile))

    # read written output
    assert not readonly_mp.get_raw_output(tile).mask.all()


def test_get_raw_output_continue_raster(mp_tmpdir, cleantopo_tl):
    """Get raw process output using continue flag."""
    with mapchete.open(cleantopo_tl.path) as mp:
        assert mp.config.mode == "continue"
        tile = (5, 0, 0)
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read written data
        assert not mp.get_raw_output(tile).mask.all()


def test_get_raw_output_continue_vector(mp_tmpdir, geojson):
    """Get raw process output using continue flag."""
    with mapchete.open(geojson.path) as mp:
        assert mp.config.mode == "continue"
        tile = next(mp.get_process_tiles(4))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read written data
        assert mp.get_raw_output(tile)


def test_get_raw_output_reproject(mp_tmpdir, cleantopo_tl):
    """Get process output from a different CRS."""
    try:
        with mapchete.open(cleantopo_tl.path) as mp:
            assert mp.config.mode == "continue"
            # TODO implement function
            mp.get_raw_output((5, 0, 0))
    except NotImplementedError:
        pass


def test_baselevels(mp_tmpdir, baselevels):
    """Baselevel interpolation."""
    with mapchete.open(baselevels.path, mode="continue") as mp:
        # process data before getting baselevels
        mp.batch_process()

        # get tile from lower zoom level
        for tile in mp.get_process_tiles(4):
            data = mp.get_raw_output(tile)
            assert not data.mask.all()
            # write for next zoom level
            mp.write(tile, data)
            assert not mp.get_raw_output(tile.get_parent()).mask.all()

        # get tile from higher zoom level
        tile = next(mp.get_process_tiles(6))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert any([
            not mp.get_raw_output(upper_tile).mask.all()
            for upper_tile in tile.get_children()
        ])


def test_baselevels_buffer(mp_tmpdir, baselevels):
    """Baselevel interpolation using buffers."""
    config = baselevels.dict
    config["pyramid"].update(pixelbuffer=10)
    with mapchete.open(config, mode="continue") as mp:
        # get tile from lower zoom level
        lower_tile = next(mp.get_process_tiles(4))
        # process and save
        for tile in lower_tile.get_children():
            mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert not mp.get_raw_output(lower_tile).mask.all()

        # get tile from higher zoom level
        tile = next(mp.get_process_tiles(6))
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert any([
            not mp.get_raw_output(upper_tile).mask.all()
            for upper_tile in tile.get_children()
        ])


def test_baselevels_buffer_antimeridian(mp_tmpdir, baselevels):
    """Baselevel interpolation using buffers."""
    config = baselevels.dict
    config.update(input=None)
    config["pyramid"].update(pixelbuffer=10)
    zoom = 5
    row = 0
    with mapchete.open(config) as mp:
        # write data left and right of antimeridian
        west = mp.config.process_pyramid.tile(zoom, row, 0)
        shape = (3, ) + west.shape
        mp.write(west, np.ones(shape) * 0)
        east = mp.config.process_pyramid.tile(
            zoom, row, mp.config.process_pyramid.matrix_width(zoom) - 1
        )
        mp.write(east, np.ones(shape) * 10)
        # use baselevel generation to interpolate tile and somehow
        # assert no data from across the antimeridian is read.
        lower_tile = mp.get_raw_output(west.get_parent())
        assert np.where(lower_tile != 10, True, False).all()


def test_processing(mp_tmpdir, cleantopo_br, cleantopo_tl):
    """Test correct processing (read and write) outputs."""
    for cleantopo_process in [cleantopo_br.path, cleantopo_tl.path]:
        with mapchete.open(cleantopo_process) as mp:
            for zoom in range(6):
                tiles = []
                for tile in mp.get_process_tiles(zoom):
                    output = mp.execute(tile)
                    tiles.append((tile, output))
                    assert isinstance(output, ma.MaskedArray)
                    assert output.shape == output.shape
                    assert not ma.all(output.mask)
                    mp.write(tile, output)
                mosaic, mosaic_affine = create_mosaic(tiles)
                try:
                    temp_vrt = os.path.join(mp_tmpdir, str(zoom)+".vrt")
                    gdalbuildvrt = "gdalbuildvrt %s %s/%s/*/*.tif > /dev/null" % (
                        temp_vrt, mp_tmpdir, zoom)
                    os.system(gdalbuildvrt)
                    with rasterio.open(temp_vrt, "r") as testfile:
                        for file_item, mosaic_item in zip(
                            testfile.meta["transform"], mosaic_affine
                        ):
                            assert file_item == mosaic_item
                        band = testfile.read(1, masked=True)
                        assert band.shape == mosaic.shape
                        assert ma.allclose(band, mosaic)
                        assert ma.allclose(band.mask, mosaic.mask)
                finally:
                    shutil.rmtree(mp_tmpdir, ignore_errors=True)


def test_multiprocessing(mp_tmpdir, cleantopo_tl):
    """Test parallel tile processing."""
    with mapchete.open(cleantopo_tl.path) as mp:
        assert dumps(mp)
        assert dumps(mp.config)
        assert dumps(mp.config.output)
        for tile in mp.get_process_tiles():
            assert dumps(tile)
        f = partial(_worker, mp)
        try:
            pool = Pool()
            for zoom in reversed(mp.config.zoom_levels):
                for tile, raw_output in pool.imap_unordered(
                    f, mp.get_process_tiles(zoom), chunksize=8
                ):
                    mp.write(tile, raw_output)
        except KeyboardInterrupt:
            pool.terminate()
        finally:
            pool.close()
            pool.join()


def _worker(mp, tile):
    """Multiprocessing worker processing a tile."""
    return tile, mp.execute(tile)


def test_write_empty(mp_tmpdir, cleantopo_tl):
    """Test write function when passing an empty process_tile."""
    with mapchete.open(cleantopo_tl.path) as mp:
        # process and save
        mp.write(mp.config.process_pyramid.tile(5, 0, 0), None)


def test_process_template(dummy1_tif):
    """Template used to create an empty process."""
    process_template = pkg_resources.resource_filename(
        "mapchete.static", "process_template.py")
    mp = mapchete.open(
        dict(
            process_file=process_template,
            pyramid=dict(grid="geodetic"),
            input=dict(file1=dummy1_tif),
            output=dict(
                format="GTiff",
                path=".",
                bands=1,
                dtype="uint8"
            ),
            config_dir=".",
            zoom_levels=4
        ))
    process_tile = next(mp.get_process_tiles(zoom=4))
    # Mapchete throws a RuntimeError if process output is empty
    with pytest.raises(MapcheteProcessOutputError):
        mp.execute(process_tile)


def test_count_tiles(zoom_mapchete):
    """Count tiles function."""
    maxzoom = 13
    conf = zoom_mapchete.dict
    conf.update(
        zoom_levels=dict(max=maxzoom),
        bounds=[14.0625, 47.8125, 16.875, 50.625], input=None)
    conf["pyramid"].update(metatiling=8, pixelbuffer=5)
    for minzoom in range(0, 14):
        conf["zoom_levels"].update(min=minzoom)
        with mapchete.open(conf) as mp:
            assert len(list(mp.get_process_tiles())) == mapchete.count_tiles(
                mp.config.area_at_zoom(), mp.config.process_pyramid, minzoom,
                maxzoom)


def test_batch_process(mp_tmpdir, cleantopo_tl):
    """Test batch_process function."""
    with mapchete.open(cleantopo_tl.path) as mp:
        # invalid parameters errors
        with pytest.raises(ValueError):
            mp.batch_process(zoom=1, tile=(1, 0, 0))
        # process single tile
        mp.batch_process(tile=(2, 0, 0))
        # process using multiprocessing
        mp.batch_process(zoom=2, multi=2)
        # process without multiprocessing
        mp.batch_process(zoom=2, multi=1)


def test_custom_grid(mp_tmpdir, custom_grid):
    """Cutom grid processing."""
    # process and save
    with mapchete.open(custom_grid.dict) as mp:
        mp.batch_process()
    # read written output
    with mapchete.open(custom_grid.dict, mode="readonly") as mp:
        for tile in mp.get_process_tiles(5):
            mp_tile = mapchete.MapcheteProcess(
                tile, config=mp.config, params=mp.config.params_at_zoom(5))
            data = mp_tile.read()
            assert data.any()
            assert isinstance(data, ma.masked_array)
            assert not data.mask.all()


def test_execute_kwargs(example_mapchete, execute_kwargs_py):
    config = example_mapchete.dict
    config.update(process_file=execute_kwargs_py)
    zoom = 7
    with mapchete.open(config) as mp:
        tile = next(mp.get_process_tiles(zoom))
        mp.execute(tile)
