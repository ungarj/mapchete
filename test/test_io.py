#!/usr/bin/env python
"""Test Mapchete io module."""

import os
import rasterio
import tempfile
import numpy as np
import numpy.ma as ma
from shapely.geometry import shape
from rasterio.enums import Compression

from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTilePyramid
from mapchete.io import raster, vector, get_best_zoom_level

scriptdir = os.path.dirname(os.path.realpath(__file__))
testdata_directory = os.path.join(scriptdir, "testdata")


def test_best_zoom_level():
    """Test best zoom level determination."""
    dummy1 = os.path.join(testdata_directory, "dummy1.tif")
    assert get_best_zoom_level(dummy1, "geodetic")
    assert get_best_zoom_level(dummy1, "mercator")


def test_read_raster_window():
    """Read array with read_raster_window."""
    dummy1 = os.path.join(testdata_directory, "dummy1.tif")
    zoom = 8
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/minmax_zoom.mapchete"))
    rasterfile = config.at_zoom(7)["input_files"]["file1"]
    dummy1_bbox = rasterfile.bbox()

    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = tile_pyramid.tiles_from_geom(dummy1_bbox, zoom)
    width = height = tile_pyramid.tile_size + 2 * pixelbuffer
    for tile in tiles:
        for band in raster.read_raster_window(dummy1, tile):
            assert isinstance(band, ma.MaskedArray)
            assert band.shape == (width, height)
        for index in range(4):
            band = raster.read_raster_window(dummy1, tile, index).next()
            assert isinstance(band, ma.MaskedArray)
            assert band.shape == (width, height)
    for resampling in [
        "nearest", "bilinear", "cubic", "cubic_spline", "lanczos", "average",
        "mode"
    ]:
        raster.read_raster_window(dummy1, tile, resampling=resampling)


def test_write_raster_window():
    """Basic output format writing."""
    path = tempfile.NamedTemporaryFile(delete=False).name
    # standard tile
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    tile.data = ma.masked_array(np.ones((2, ) + tile.shape))
    for out_profile in [
        dict(
            driver="GTiff", count=2, dtype="uint8", compress="lzw", nodata=0,
            height=tile.height, width=tile.width, affine=tile.affine),
        dict(
            driver="GTiff", count=2, dtype="uint8", compress="deflate",
            nodata=0, height=tile.height, width=tile.width,
            affine=tile.affine),
        dict(
            driver="PNG", count=2, dtype="uint8", nodata=0, height=tile.height,
            width=tile.width, compress=None, affine=tile.affine),
    ]:
        try:
            raster.write_raster_window(
                in_tile=tile, out_profile=out_profile, out_tile=tile,
                out_path=path)
            with rasterio.open(path, 'r') as src:
                assert src.read().any()
                assert src.meta["driver"] == out_profile["driver"]
                assert src.affine == tile.affine
                if out_profile["compress"]:
                    assert src.compression == Compression(
                        out_profile["compress"].upper())
        except Exception:
            raise
        finally:
            try:
                os.remove(path)
            except Exception:
                pass

    # with metatiling
    tile = BufferedTilePyramid("geodetic", metatiling=4).tile(5, 1, 1)
    tile.data = ma.masked_array(np.ones((2, ) + tile.shape))
    out_tile = BufferedTilePyramid("geodetic").tile(5, 5, 5)
    out_profile = dict(
            driver="GTiff", count=2, dtype="uint8", compress="lzw", nodata=0,
            height=out_tile.height, width=out_tile.width,
            affine=out_tile.affine)
    try:
        raster.write_raster_window(
            in_tile=tile, out_profile=out_profile, out_tile=out_tile,
            out_path=path)
        with rasterio.open(path, 'r') as src:
            assert src.shape == out_tile.shape
            assert src.read().any()
            assert src.meta["driver"] == out_profile["driver"]
            assert src.affine == out_profile["affine"]
    except Exception:
        raise
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


# TODO raster.extract_from_tile()
# TODO raster.extract_from_array()
# TODO raster.resample_from_array()
# TODO raster.create_mosaic()


def test_read_vector_window():
    """Read vector data from read_vector_window."""
    zoom = 4
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/geojson.mapchete"))
    vectorfile = config.at_zoom(zoom)["input_files"]["file1"]
    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = tile_pyramid.tiles_from_geom(vectorfile.bbox(), zoom)
    feature_count = 0
    for tile in tiles:
        for feature in vector.read_vector_window(vectorfile.path, tile):
            assert "properties" in feature
            assert shape(feature["geometry"]).is_valid
            feature_count += 1
    assert feature_count


# TODO vector.reproject_geometry()
# TODO vector.write_vector_window()
# TODO vector.clean_geometry_type()
# TODO vector.extract_from_tile()
