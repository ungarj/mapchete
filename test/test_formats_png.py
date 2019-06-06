"""Test PNG as process output."""

import numpy as np
import numpy.ma as ma
import os
import pytest
import shutil

from mapchete.formats.default import png
from mapchete.tile import BufferedTilePyramid


def test_output_data(mp_tmpdir):
    """Check PNG as output data."""
    output_params = dict(
        grid="geodetic",
        format="PNG",
        path=mp_tmpdir,
        pixelbuffer=0,
        metatiling=1
    )
    try:
        output = png.OutputDataWriter(output_params)
        assert output.path == mp_tmpdir
        assert output.file_extension == ".png"
        tp = BufferedTilePyramid("geodetic")
        tile = tp.tile(5, 5, 5)
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)

    # get_path
    try:
        assert output.get_path(tile) == os.path.join(*[mp_tmpdir, "5", "5", "5"+".png"])
    finally:
        shutil.rmtree(mp_tmpdir, ignore_errors=True)

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

    # read empty
    empty = output.read(tp.tile(5, 0, 0))
    assert isinstance(empty, ma.MaskedArray)
    assert not empty.any()
    # write empty
    output.write(tile, np.zeros((3, ) + tile.shape))
    output.write(tile, np.zeros((4, ) + tile.shape))
    with pytest.raises(TypeError):
        output.write(tile, np.zeros((5, ) + tile.shape))


@pytest.mark.remote
def test_s3_write_output_data(mp_s3_tmpdir):
    """Write and read output."""
    output_params = dict(
        grid="geodetic",
        format="PNG",
        path=mp_s3_tmpdir,
        pixelbuffer=0,
        metatiling=1
    )
    output = png.OutputDataWriter(output_params)
    assert output.path == mp_s3_tmpdir
    assert output.file_extension == ".png"
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    # get_path
    assert output.get_path(tile) == os.path.join(*[
        mp_s3_tmpdir, "5", "5", "5"+".png"
    ])
    # profile
    assert isinstance(output.profile(tile), dict)
    # write
    data = np.ones((1, ) + tile.shape)*128
    output.write(tile, data)
    # tiles_exist
    assert output.tiles_exist(tile)
    # read
    data = output.read(tile)
    assert isinstance(data, np.ndarray)
    assert data.shape[0] == 4
    assert not data[0].mask.any()
    # empty
    empty = output.empty(tile)
    assert isinstance(empty, ma.MaskedArray)
    assert not empty.any()
