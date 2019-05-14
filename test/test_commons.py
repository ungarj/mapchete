#!/usr/bin/env python
"""Test Mapchete commons module."""

import numpy as np
import numpy.ma as ma
from shapely.geometry import Point, GeometryCollection

import mapchete
from mapchete import MapcheteProcess


def test_clip(geojson):
    """Clip an array with a vector."""
    with mapchete.open(geojson.path) as mp:
        tile = next(mp.get_process_tiles(zoom=4))
        user_process = MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(4),
            input=mp.config.get_inputs_for_tile(tile),
        )
        with user_process.open("file1") as vector_file:
            test_array = ma.masked_array(np.ones(user_process.tile.shape))
            clipped = user_process.clip(test_array, vector_file.read())
            # default params
            assert isinstance(clipped, ma.masked_array)
            assert clipped.mask.any()
            assert not clipped.mask.all()
            # inverted clip
            clipped_inverted = user_process.clip(
                test_array, vector_file.read(), inverted=True)
            assert isinstance(clipped_inverted, ma.masked_array)
            assert clipped_inverted.mask.any()
            assert not clipped_inverted.mask.all()
            # compare results
            assert (clipped+clipped_inverted).mask.all()
            # using empty Geometries
            geoms = [dict(geometry=Point())]
            clipped = user_process.clip(test_array, geoms)
            assert clipped.mask.all()
            # using empty Geometries inverted
            clipped = user_process.clip(test_array, geoms, inverted=True)
            assert not clipped.mask.any()
            # using Point Geometries
            geoms = [dict(geometry=tile.bbox.centroid)]
            clipped = user_process.clip(test_array, geoms)
            assert clipped.mask.all()
            # using Geometry Collections
            geoms = [dict(
                geometry=GeometryCollection([tile.bbox.centroid, tile.bbox]))]
            clipped = user_process.clip(test_array, geoms)
            assert not clipped.mask.any()
            # using 3D array
            test_array = ma.masked_array(
                np.ones((1, ) + user_process.tile.shape))
            clipped = user_process.clip(test_array, vector_file.read())
            assert isinstance(clipped, ma.masked_array)
            assert clipped.mask.any()
            assert not clipped.mask.all()


def test_contours(cleantopo_tl):
    """Extract contours from array."""
    with mapchete.open(cleantopo_tl.path) as mp:
        tile = next(mp.get_process_tiles(zoom=4))
        user_process = MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(4),
            input=mp.config.get_inputs_for_tile(tile),
        )
        with user_process.open("file1") as dem:
            arr = dem.read()
            # valid contours
            contours = user_process.contours(arr)
            assert contours
            assert isinstance(contours, list)
            # no contours
            contours = user_process.contours(arr, interval=10000)
            assert isinstance(contours, list)
            assert not contours
            # base bigger than values
            contours = user_process.contours(arr, base=10000)
            assert isinstance(contours, list)
            assert contours


def test_hillshade(cleantopo_tl):
    """Render hillshade from array."""
    with mapchete.open(cleantopo_tl.path) as mp:
        tile = next(mp.get_process_tiles(zoom=4))
        user_process = MapcheteProcess(
            tile=tile,
            params=mp.config.params_at_zoom(4),
            input=mp.config.get_inputs_for_tile(tile),
        )
        with user_process.open("file1") as dem:
            shade = user_process.hillshade(dem.read())
            assert isinstance(shade, np.ndarray)
