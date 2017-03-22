#!/usr/bin/env python
"""Test Mapchete io module."""

import os
import numpy.ma as ma
from shapely.geometry import shape

from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTilePyramid
from mapchete.io.raster import read_raster_window
from mapchete.io.vector import read_vector_window

scriptdir = os.path.dirname(os.path.realpath(__file__))
testdata_directory = os.path.join(scriptdir, "testdata")


def test_read_raster_window():
    """Read array with read_raster_window."""
    dummy1 = os.path.join(testdata_directory, "dummy1.tif")
    zoom = 8
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/minmax_zoom.mapchete"))
    raster = config.at_zoom(7)["input_files"]["file1"]
    dummy1_bbox = raster.bbox()

    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = tile_pyramid.tiles_from_geom(dummy1_bbox, zoom)
    resampling = "average"
    width = height = tile_pyramid.tile_size + 2 * pixelbuffer
    for tile in tiles:
        for band in read_raster_window(dummy1, tile, resampling=resampling):
            assert isinstance(band, ma.MaskedArray)
            assert band.shape == (width, height)


def test_read_vector_window():
    """Read vector data from read_vector_window."""
    zoom = 2
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/geojson.mapchete"))
    vector = config.at_zoom(zoom)["input_files"]["file1"]
    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = tile_pyramid.tiles_from_geom(vector.bbox(), zoom)
    feature_count = 0
    for tile in tiles:
        for feature in read_vector_window(vector.path, tile):
            assert "properties" in feature
            assert shape(feature["geometry"]).is_valid
            feature_count += 1
    assert feature_count



# TODO: test other functions
