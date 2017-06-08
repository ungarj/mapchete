#!/usr/bin/env python
"""Test GeoTIFF as process output."""

import os
import shutil
import numpy as np
import numpy.ma as ma

import mapchete
from mapchete.formats.default import gtiff
from mapchete.tile import BufferedTilePyramid

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")


def test_output_data():
    """Check GeoTIFF as output data."""
    output_params = dict(
        type="geodetic",
        format="GeoTIFF",
        path=OUT_DIR,
        pixelbuffer=0,
        metatiling=1,
        bands=1,
        dtype="int16"
    )
    output = gtiff.OutputData(output_params)
    assert output.path == OUT_DIR
    assert output.file_extension == ".tif"
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    # get_path
    assert output.get_path(tile) == os.path.join(*[
        OUT_DIR, "5", "5", "5"+".tif"])
    # prepare_path
    try:
        temp_dir = os.path.join(*[OUT_DIR, "5", "5"])
        output.prepare_path(tile)
        assert os.path.isdir(temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # profile
    assert isinstance(output.profile(tile), dict)
    # write
    try:
        tile.data = np.ones((1, ) + tile.shape)*128
        output.write(tile)
        # tiles_exist
        assert output.tiles_exist(tile)
        # read
        data = output.read(tile).data
        assert isinstance(data, np.ndarray)
        assert not data[0].mask.any()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # read empty
    data = output.read(tile).data
    assert isinstance(data, np.ndarray)
    assert data[0].mask.all()
    # empty
    empty = output.empty(tile)
    assert isinstance(empty, ma.MaskedArray)
    assert not empty.any()
    # deflate with predictor
    output_params.update(compression="deflate", predictor=2)
    output = gtiff.OutputData(output_params)
    assert output.profile(tile)["compress"] == "deflate"
    assert output.profile(tile)["predictor"] == 2


def test_input_data():
    """Check GeoTIFF proces output as input data."""
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete"))
    tp = BufferedTilePyramid("geodetic")
    # TODO tile with existing but empty data
    tile = tp.tile(5, 5, 5)
    output_params = dict(
        type="geodetic",
        format="GeoTIFF",
        path=OUT_DIR,
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
