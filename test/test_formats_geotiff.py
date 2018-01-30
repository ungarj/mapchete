#!/usr/bin/env python
"""Test GeoTIFF as process output."""

import os
from rasterio.io import MemoryFile
import shutil
import numpy as np
import numpy.ma as ma

import mapchete
from mapchete.formats.default import gtiff
from mapchete.tile import BufferedTilePyramid


def test_output_data(mp_tmpdir):
    """Check GeoTIFF as output data."""
    output_params = dict(
        type="geodetic",
        format="GeoTIFF",
        path=mp_tmpdir,
        pixelbuffer=0,
        metatiling=1,
        bands=1,
        dtype="int16"
    )
    output = gtiff.OutputData(output_params)
    assert output.path == mp_tmpdir
    assert output.file_extension == ".tif"
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    # get_path
    assert output.get_path(tile) == os.path.join(*[
        mp_tmpdir, "5", "5", "5"+".tif"])
    # prepare_path
    try:
        temp_dir = os.path.join(*[mp_tmpdir, "5", "5"])
        output.prepare_path(tile)
        assert os.path.isdir(temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # profile
    assert isinstance(output.profile(tile), dict)
    # write
    try:
        data = np.ones((1, ) + tile.shape)*128
        output.write(tile, data)
        # tiles_exist
        assert output.tiles_exist(tile)
        # read
        data = output.read(tile)
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.any()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # read empty
    data = output.read(tile)
    assert isinstance(data, np.ndarray)
    assert data[0].mask.all()
    # empty
    empty = output.empty(tile)
    assert isinstance(empty, ma.MaskedArray)
    assert not empty.any()
    # deflate with predictor
    output_params.update(compress="deflate", predictor=2)
    output = gtiff.OutputData(output_params)
    assert output.profile(tile)["compress"] == "deflate"
    assert output.profile(tile)["predictor"] == 2
    # using deprecated "compression" property
    output_params.update(compression="deflate", predictor=2)
    output = gtiff.OutputData(output_params)
    assert output.profile(tile)["compress"] == "deflate"
    assert output.profile(tile)["predictor"] == 2


def test_for_web(client, mp_tmpdir):
    """Send GTiff via flask."""
    tile_base_url = '/wmts_simple/1.0.0/cleantopo_br/default/WGS84/'
    for url in ["/"]:
        response = client.get(url)
        assert response.status_code == 200
    for url in [
        tile_base_url+"5/30/62.tif",
        tile_base_url+"5/30/63.tif",
        tile_base_url+"5/31/62.tif",
        tile_base_url+"5/31/63.tif",
    ]:
        response = client.get(url)
        assert response.status_code == 200
        img = response.response.file
        with MemoryFile(img) as memfile:
            with memfile.open() as dataset:
                assert dataset.read().any()


def test_input_data(mp_tmpdir, cleantopo_br):
    """Check GeoTIFF proces output as input data."""
    with mapchete.open(cleantopo_br.path) as mp:
        tp = BufferedTilePyramid("geodetic")
        # TODO tile with existing but empty data
        tile = tp.tile(5, 5, 5)
        output_params = dict(
            type="geodetic",
            format="GeoTIFF",
            path=mp_tmpdir,
            pixelbuffer=0,
            metatiling=1,
            bands=2,
            dtype="int16"
        )
        output = gtiff.OutputData(output_params)
        with output.open(tile, mp, resampling="nearest") as input_tile:
            assert input_tile.resampling == "nearest"
            for data in [
                input_tile.read(), input_tile.read(1), input_tile.read([1]),
                # TODO assert valid indexes are passed input_tile.read([1, 2])
            ]:
                assert isinstance(data, ma.masked_array)
                assert input_tile.is_empty()
        # open without resampling
        with output.open(tile, mp) as input_tile:
            pass
