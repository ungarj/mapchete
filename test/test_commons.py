#!/usr/bin/env python
"""Test Mapchete commons module."""

import os
import numpy as np
import numpy.ma as ma


from mapchete import Mapchete, MapcheteConfig, MapcheteProcess
from mapchete.tile import BufferedTilePyramid

scriptdir = os.path.dirname(os.path.realpath(__file__))


def test_clip():
    """Clip an array with a vector."""
    process = Mapchete(
        os.path.join(scriptdir, "testdata/geojson.mapchete"))
    tile = process.get_process_tiles(zoom=4).next()
    tile_process = MapcheteProcess(tile, params=process.config.at_zoom(4))
    with tile_process.open("file1") as vector_file:
        test_array = ma.masked_array(np.ones(tile_process.tile.shape))
        clipped = tile_process.clip(test_array, vector_file.read())
        # default params
        assert isinstance(clipped, ma.masked_array)
        assert clipped.mask.any()
        assert not clipped.mask.all()
        # inverted clip
        clipped_inverted = tile_process.clip(
            test_array, vector_file.read(), inverted=True)
        assert isinstance(clipped_inverted, ma.masked_array)
        assert clipped_inverted.mask.any()
        assert not clipped_inverted.mask.all()
        # compare results
        assert (clipped+clipped_inverted).mask.all()


def test_contours():
    """Extract contours from array."""
    process = Mapchete(
        os.path.join(scriptdir, "testdata/cleantopo_tl.mapchete"))
    tile = process.get_process_tiles(zoom=4).next()
    tile_process = MapcheteProcess(tile, params=process.config.at_zoom(4))
    with tile_process.open("file1") as dem:
        contours = tile_process.contours(dem.read())
        assert contours
        assert isinstance(contours, list)


def test_hillshade():
    """Render hillshade from array."""
    process = Mapchete(
        os.path.join(scriptdir, "testdata/cleantopo_tl.mapchete"))
    tile = process.get_process_tiles(zoom=4).next()
    tile_process = MapcheteProcess(tile, params=process.config.at_zoom(4))
    with tile_process.open("file1") as dem:
        shade = tile_process.hillshade(dem.read())
        assert isinstance(shade, np.ndarray)
