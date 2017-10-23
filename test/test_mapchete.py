#!/usr/bin/env python
"""Test Mapchete main module and processing."""

import pytest
import os
import shutil
import yaml
import rasterio
import numpy as np
import numpy.ma as ma
import pkg_resources
from cPickle import dumps
from functools import partial
from multiprocessing import Pool
from shapely.geometry import shape

import mapchete
from mapchete.io.raster import create_mosaic
from mapchete.errors import MapcheteProcessOutputError
from mapchete import _batch

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


def test_empty_execute():
    """Execute process outside of defined zoom levels."""
    try:
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mp:
            assert mp.execute((6, 0, 0)).mask.all()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_read_existing_output():
    """Read existing process output."""
    # raster data
    try:
        tile = (5, 0, 0)
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete")
        ) as mp:
            # process and save
            mp.get_raw_output(tile)
            # read written data from within MapcheteProcess object
            mp_tile = mapchete.MapcheteProcess(
                mp.config.process_pyramid.tile(*tile),
                config=mp.config,
                params=mp.config.at_zoom(5)
            )
            data = mp_tile.read()
            assert data.any()
            assert isinstance(data, ma.masked_array)
            assert not data.mask.all()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)
    # raster data process buffer > output buffer
    try:
        config = yaml.load(open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete")
        ).read())
        config.update(config_dir=TESTDATA_DIR)
        config["output"].update(pixelbuffer=0)
        with mapchete.open(config) as mp:
            tile = mp.get_process_tiles(5).next()
            # process and save
            mp.get_raw_output(tile)
            # read written data from within MapcheteProcess object
            mp_tile = mapchete.MapcheteProcess(
                mp.config.process_pyramid.tile(*tile.id),
                config=mp.config,
                params=mp.config.at_zoom(5)
            )
            data = mp_tile.read()
            assert data.any()
            assert isinstance(data, ma.masked_array)
            assert not data.mask.all()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)
    # vector data
    try:
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/geojson.mapchete")
        ) as mp:
            tile = mp.get_process_tiles(4).next()
            # process and save
            mp.write(tile, mp.get_raw_output(tile))
            # read written data from within MapcheteProcess object
            mp_tile = mapchete.MapcheteProcess(
                tile,
                config=mp.config,
                params=mp.config.at_zoom(4)
            )
            data = mp_tile.read()
            assert data
            for feature in data:
                assert shape(feature["geometry"]).is_valid
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_open_data_error():
    """Try to open data not specified as input."""
    tile = (5, 0, 0)
    with mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete")
    ) as mp:
        # read written data from within MapcheteProcess object
        mp_tile = mapchete.MapcheteProcess(
            mp.config.process_pyramid.tile(*tile),
            config=mp.config,
            params=mp.config.at_zoom(5)
        )
        with pytest.raises(ValueError):
            mp_tile.open("invaild_input_id")


def test_get_raw_output_outside():
    """Get raw process output outside of zoom levels."""
    try:
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mp:
            assert mp.get_raw_output((6, 0, 0)).mask.all()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_get_raw_output_memory():
    """Get raw process output using memory flag."""
    try:
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"),
            mode="memory"
        ) as mp:
            assert mp.config.mode == "memory"
            assert not mp.get_raw_output((5, 0, 0)).mask.all()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_get_raw_output_readonly():
    """Get raw process output using readonly flag."""
    try:
        tile = (5, 0, 0)
        readonly_mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"),
            mode="readonly")
        write_mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"),
            mode="continue")

        # read non-existing data (returns empty)
        assert readonly_mp.get_raw_output(tile).mask.all()

        # try to process and save empty data
        with pytest.raises(ValueError):
            readonly_mp.write(tile, readonly_mp.get_raw_output(tile))

        # actually process and save
        write_mp.write(tile, write_mp.get_raw_output(tile))

        # read written output
        assert not readonly_mp.get_raw_output(tile).mask.all()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_get_raw_output_continue():
    """Get raw process output using memory flag."""
    try:
        mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
        assert mp.config.mode == "continue"
        tile = (5, 0, 0)
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read written data
        assert not mp.get_raw_output(tile).mask.all()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_get_raw_output_reproject():
    """Get process output from a different CRS."""
    try:
        mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
        assert mp.config.mode == "continue"
        # TODO implement function
        mp.get_raw_output((5, 0, 0))
    except NotImplementedError:
        pass
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_baselevels():
    """Baselevel interpolation."""
    try:
        mp = mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/baselevels.mapchete"),
            mode="continue"
        )
        # process data before getting baselevels
        mp.batch_process(quiet=True)

        # get tile from lower zoom level
        for tile in mp.get_process_tiles(4):
            data = mp.get_raw_output(tile)
            assert not data.mask.all()
            # write for next zoom level
            mp.write(tile, data)
            assert not mp.get_raw_output(tile.get_parent()).mask.all()

        # get tile from higher zoom level
        tile = mp.get_process_tiles(6).next()
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert any([
            not mp.get_raw_output(upper_tile).mask.all()
            for upper_tile in tile.get_children()
        ])
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_baselevels_buffer():
    """Baselevel interpolation using buffers."""
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/baselevels.mapchete"), "r"
        ) as src:
            config = yaml.load(src.read())
            config.update(
                pixelbuffer=10, config_dir=os.path.join(SCRIPTDIR, "testdata")
            )
        mp = mapchete.open(config, mode="continue")
        # get tile from lower zoom level
        lower_tile = mp.get_process_tiles(4).next()
        # process and save
        for tile in lower_tile.get_children():
            mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert not mp.get_raw_output(lower_tile).mask.all()

        # get tile from higher zoom level
        tile = mp.get_process_tiles(6).next()
        # process and save
        mp.write(tile, mp.get_raw_output(tile))
        # read from baselevel
        assert any([
            not mp.get_raw_output(upper_tile).mask.all()
            for upper_tile in tile.get_children()
        ])
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_baselevels_buffer_antimeridian():
    """Baselevel interpolation using buffers."""
    try:
        mp_config = yaml.load(open(
            os.path.join(SCRIPTDIR, "testdata/baselevels.mapchete"),
            "r").read()
        )
        mp_config.update(
            pixelbuffer=10, config_dir=os.path.join(SCRIPTDIR, "testdata"),
            input=None
        )
        zoom = 5
        row = 0
        with mapchete.open(mp_config) as mp:
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
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_processing():
    """Test correct processing (read and write) outputs."""
    for cleantopo_process in [
        "testdata/cleantopo_tl.mapchete", "testdata/cleantopo_br.mapchete"
    ]:
        mp = mapchete.open(os.path.join(SCRIPTDIR, cleantopo_process))
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
                temp_vrt = os.path.join(OUT_DIR, str(zoom)+".vrt")
                gdalbuildvrt = "gdalbuildvrt %s %s/%s/*/*.tif > /dev/null" % (
                    temp_vrt, OUT_DIR, zoom)
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
                shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_processing_as_function():
    """Test correct processing using execute function()."""
    for cleantopo_process in [
        "testdata/cleantopo_tl.mapchete", "testdata/cleantopo_br.mapchete"
    ]:
        mp_config = yaml.load(
            open(os.path.join(SCRIPTDIR, cleantopo_process)).read()
        )
        mp_config.update(
            process_file="../example_process_as_function.py",
            config_dir=TESTDATA_DIR
        )
        mp = mapchete.open(mp_config)
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
                temp_vrt = os.path.join(OUT_DIR, str(zoom)+".vrt")
                gdalbuildvrt = "gdalbuildvrt %s %s/%s/*/*.tif > /dev/null" % (
                    temp_vrt, OUT_DIR, zoom)
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
                shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_multiprocessing():
    """Test parallel tile processing."""
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
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
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def _worker(mp, tile):
    """Multiprocessing worker processing a tile."""
    return tile, mp.execute(tile)


def test_write_empty():
    """Test write function when passing an empty process_tile."""
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
    # process and save
    mp.write(mp.config.process_pyramid.tile(5, 0, 0), None)


def test_process_template():
    """Template used to create an empty process."""
    process_template = pkg_resources.resource_filename(
        "mapchete.static", "process_template.py")
    dummy1 = os.path.join(TESTDATA_DIR, "dummy1.tif")
    mp = mapchete.open(
        dict(
            process_file=process_template,
            input=dict(file1=dummy1),
            output=dict(
                format="GTiff",
                path=".",
                type="geodetic",
                bands=1,
                dtype="uint8"
            ),
            config_dir=".",
            process_zoom=4
        ))
    process_tile = mp.get_process_tiles(zoom=4).next()
    # Mapchete throws a RuntimeError if process output is empty
    try:
        mp.execute(process_tile)
        raise Exception()
    except MapcheteProcessOutputError:
        pass


def test_count_tiles():
    """Count tiles function."""
    maxzoom = 13
    mp_conf = yaml.load(open(
        os.path.join(SCRIPTDIR, "testdata/zoom.mapchete"), "r").read()
    )
    del mp_conf["process_zoom"]
    mp_conf.update(
        process_maxzoom=maxzoom,
        process_bounds=[14.0625, 47.8125, 16.875, 50.625],
        config_dir=TESTDATA_DIR, input=None, metatiling=8, pixelbuffer=5
    )
    # for minzoom in range(0, 14):
    for minzoom in range(0, 14):
        mp_conf.update(process_minzoom=minzoom)
        with mapchete.open(mp_conf) as mp:
            assert len(list(mp.get_process_tiles())) == _batch.count_tiles(
                mp.config.process_area(), mp.config.process_pyramid, minzoom,
                maxzoom
            )


def test_batch_process():
    """Test batch_process function."""
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
    try:
        # invalid parameters errors
        with pytest.raises(ValueError):
            mp.batch_process(zoom=1, tile=(1, 0, 0))
        with pytest.raises(ValueError):
            mp.batch_process(debug=True, quiet=True)
        # process single tile
        mp.batch_process(tile=(2, 0, 0))
        mp.batch_process(tile=(2, 0, 0), quiet=True)
        mp.batch_process(tile=(2, 0, 0), debug=True)
        # process using multiprocessing
        mp.batch_process(zoom=2, multi=2)
        # process without multiprocessing
        mp.batch_process(zoom=2, multi=1)
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)
