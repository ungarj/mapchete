#!/usr/bin/env python
"""Test PNG_hillshade as process output."""

import os
import pytest
import shutil
import numpy as np
import numpy.ma as ma

from mapchete.formats.default import png_hillshade
from mapchete.tile import BufferedTilePyramid


def test_output_data(mp_tmpdir):
    """Check PNG_hillshade as output data."""
    output_params = dict(
        grid="geodetic",
        format="PNG_hillshade",
        path=mp_tmpdir,
        pixelbuffer=0,
        metatiling=1,
    )
    output = png_hillshade.OutputDataWriter(output_params)
    assert output.path == mp_tmpdir
    assert output.file_extension == ".png"
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    # get_path
    assert output.get_path(tile) == os.path.join(*[mp_tmpdir, "5", "5", "5" + ".png"])
    # prepare_path
    try:
        temp_dir = os.path.join(*[mp_tmpdir, "5", "5"])
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
        data = np.ones(tile.shape) * 128
        output.write(tile, data)
        # tiles_exist
        assert output.tiles_exist(tile)
        # read
        data = output.read(tile)
        assert isinstance(data, np.ndarray)
        assert not data.mask.any()
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)
    # write half masked array
    try:
        half_shape = (tile.shape[0], tile.shape[1] // 2)
        data = ma.masked_array(
            data=np.ones(tile.shape) * 128,
            mask=np.concatenate([np.zeros(half_shape), np.ones(half_shape)], axis=1),
        )
        output.write(tile, data)
        # tiles_exist
        assert output.tiles_exist(tile)
        # read
        data = output.read(tile)
        assert isinstance(data, np.ndarray)
        assert not data.mask.all()
        assert data.mask.any()
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)

    # old_band_num
    output_params.update(old_band_num=True)
    output = png_hillshade.OutputDataWriter(output_params)
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    try:
        data = np.ones(tile.shape) * 128
        output.write(tile, data)
        # tiles_exist
        assert output.tiles_exist(tile)
        # read
        data = output.read(tile)
        assert isinstance(data, np.ndarray)
        assert not data.mask.any()
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)
    # empty
    empty = output.empty(tile)
    assert isinstance(empty, ma.MaskedArray)
    assert not empty.any()
    # read non-existing file
    data = output.read(tile)
    assert data.mask.all()
    # TODO for_web


@pytest.mark.remote
def test_s3_write_output_data(mp_s3_tmpdir):
    """Write and read output."""
    output_params = dict(
        grid="geodetic",
        format="PNG_hillshade",
        path=mp_s3_tmpdir,
        pixelbuffer=0,
        metatiling=1,
    )
    output = png_hillshade.OutputDataWriter(output_params)
    assert output.path == mp_s3_tmpdir
    assert output.file_extension == ".png"
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    # get_path
    assert output.get_path(tile) == os.path.join(
        *[mp_s3_tmpdir, "5", "5", "5" + ".png"]
    )
    # profile
    assert isinstance(output.profile(tile), dict)
    # write full array
    data = np.ones(tile.shape) * 128
    output.write(tile, data)
    # tiles_exist
    assert output.tiles_exist(tile)
    # read
    data = output.read(tile)
    assert isinstance(data, np.ndarray)
    assert not data.mask.any()
