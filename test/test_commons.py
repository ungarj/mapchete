#!/usr/bin/env python
"""Test Mapchete commons module."""

import os
import numpy as np
import numpy.ma as ma
from shapely.geometry import Point, GeometryCollection

import mapchete
from mapchete import MapcheteProcess

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))


def test_clip():
    """Clip an array with a vector."""
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/geojson.mapchete"))
    tile = mp.get_process_tiles(zoom=4).next()
    tile_process = MapcheteProcess(tile, params=mp.config.at_zoom(4))
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
        # using empty Geometries
        geoms = [dict(geometry=Point())]
        clipped = tile_process.clip(test_array, geoms)
        assert clipped.mask.all()
        # using empty Geometries inverted
        clipped = tile_process.clip(test_array, geoms, inverted=True)
        assert not clipped.mask.any()
        # using Point Geometries
        geoms = [dict(geometry=tile.bbox.centroid)]
        clipped = tile_process.clip(test_array, geoms)
        assert clipped.mask.all()
        # using Geometry Collections
        geoms = [dict(
            geometry=GeometryCollection([tile.bbox.centroid, tile.bbox]))]
        clipped = tile_process.clip(test_array, geoms)
        assert not clipped.mask.any()
        # using 3D array
        test_array = ma.masked_array(np.ones((1, ) + tile_process.tile.shape))
        clipped = tile_process.clip(test_array, vector_file.read())
        assert isinstance(clipped, ma.masked_array)
        assert clipped.mask.any()
        assert not clipped.mask.all()


def test_contours():
    """Extract contours from array."""
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
    tile = mp.get_process_tiles(zoom=4).next()
    tile_process = MapcheteProcess(tile, params=mp.config.at_zoom(4))
    with tile_process.open("file1") as dem:
        contours = tile_process.contours(dem.read())
        assert contours
        assert isinstance(contours, list)
        # no contours
        contours = tile_process.contours(dem.read(), interval=10000)
        assert isinstance(contours, list)
        assert not contours
        # base bigger than values
        contours = tile_process.contours(dem.read(), base=10000)
        assert isinstance(contours, list)
        assert contours


def test_hillshade():
    """Render hillshade from array."""
    mp = mapchete.open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_tl.mapchete"))
    tile = mp.get_process_tiles(zoom=4).next()
    tile_process = MapcheteProcess(tile, params=mp.config.at_zoom(4))
    with tile_process.open("file1") as dem:
        shade = tile_process.hillshade(dem.read())
        assert isinstance(shade, np.ndarray)
