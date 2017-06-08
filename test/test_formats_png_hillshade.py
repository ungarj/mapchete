#!/usr/bin/env python
"""Test PNG_hillshade as process output."""

import os
import shutil
import numpy as np
import numpy.ma as ma

from mapchete.formats.default import png_hillshade
from mapchete.tile import BufferedTilePyramid

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")


def test_output_data():
    """Check PNG_hillshade as output data."""
    output_params = dict(
        type="geodetic",
        format="PNG_hillshade",
        path=OUT_DIR,
        pixelbuffer=0,
        metatiling=1
    )
    output = png_hillshade.OutputData(output_params)
    assert output.path == OUT_DIR
    assert output.file_extension == ".png"
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    # get_path
    assert output.get_path(tile) == os.path.join(*[
        OUT_DIR, "5", "5", "5"+".png"])
    # prepare_path
    try:
        temp_dir = os.path.join(*[OUT_DIR, "5", "5"])
        output.prepare_path(tile)
        assert os.path.isdir(temp_dir)
        # create again to ensure, no OSError is being thrown
        output.prepare_path(tile)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # profile
    assert isinstance(output.profile(tile), dict)
    # write full array
    try:
        tile.data = np.ones(tile.shape)*128
        output.write(tile)
        # tiles_exist
        assert output.tiles_exist(tile)
        # read
        data = output.read(tile).data
        assert isinstance(data, np.ndarray)
        assert not data.mask.any()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # write half masked array
    try:
        half_shape = (tile.shape[0], tile.shape[1]/2)
        tile.data = ma.masked_array(
            data=np.ones(tile.shape)*128,
            mask=np.concatenate(
                [np.zeros(half_shape), np.ones(half_shape)], axis=1))
        output.write(tile)
        # tiles_exist
        assert output.tiles_exist(tile)
        # read
        data = output.read(tile).data
        assert isinstance(data, np.ndarray)
        assert not data.mask.all()
        assert data.mask.any()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # old_band_num
    output_params.update(old_band_num=True)
    output = png_hillshade.OutputData(output_params)
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    try:
        tile.data = np.ones(tile.shape)*128
        output.write(tile)
        # tiles_exist
        assert output.tiles_exist(tile)
        # read
        data = output.read(tile).data
        assert isinstance(data, np.ndarray)
        assert not data.mask.any()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # empty
    empty = output.empty(tile)
    assert isinstance(empty, ma.MaskedArray)
    assert not empty.any()
    # read non-existing file
    data = output.read(tile).data
    assert data.mask.all()
    # TODO for_web
