#!/usr/bin/env python
"""Test PNG as process output."""

import os
import shutil
import numpy as np
import numpy.ma as ma

from mapchete.formats.default import png
from mapchete.tile import BufferedTilePyramid


def test_output_data(mp_tmpdir):
    """Check PNG as output data."""
    output_params = dict(
        type="geodetic",
        format="PNG",
        path=mp_tmpdir,
        pixelbuffer=0,
        metatiling=1
    )
    output = png.OutputData(output_params)
    assert output.path == mp_tmpdir
    assert output.file_extension == ".png"
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    # get_path
    assert output.get_path(tile) == os.path.join(*[
        mp_tmpdir, "5", "5", "5"+".png"])
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
        assert data.shape[0] == 4
        assert not data[0].mask.any()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # empty
    empty = output.empty(tile)
    assert isinstance(empty, ma.MaskedArray)
    assert not empty.any()
    # TODO for_web
