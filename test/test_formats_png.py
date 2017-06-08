#!/usr/bin/env python
"""Test PNG as process output."""

import os
import shutil
import numpy as np
import numpy.ma as ma

from mapchete.formats.default import png
from mapchete.tile import BufferedTilePyramid

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")


def test_output_data():
    """Check PNG as output data."""
    output_params = dict(
        type="geodetic",
        format="PNG",
        path=OUT_DIR,
        pixelbuffer=0,
        metatiling=1
    )
    output = png.OutputData(output_params)
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
        assert data.shape[0] == 4
        assert not data[0].mask.any()
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    # empty
    empty = output.empty(tile)
    assert isinstance(empty, ma.MaskedArray)
    assert not empty.any()
    # TODO for_web
