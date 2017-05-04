#!/usr/bin/env python
"""Test Mapchete io module."""

import os
import rasterio
import tempfile
import numpy as np
import numpy.ma as ma
import fiona
from shapely.geometry import shape, box, Polygon
from shapely.ops import unary_union
from rasterio.enums import Compression
from rasterio.crs import CRS
from itertools import product

from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTilePyramid
from mapchete.io import raster, vector, get_best_zoom_level

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


def test_best_zoom_level():
    """Test best zoom level determination."""
    dummy1 = os.path.join(TESTDATA_DIR, "dummy1.tif")
    assert get_best_zoom_level(dummy1, "geodetic")
    assert get_best_zoom_level(dummy1, "mercator")


def test_read_raster_window():
    """Read array with read_raster_window."""
    dummy1 = os.path.join(TESTDATA_DIR, "dummy1.tif")
    zoom = 8
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/minmax_zoom.mapchete"))
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


def test_extract_from_tile():
    """Extract subdata from bigger tile."""
    in_tile = BufferedTilePyramid("geodetic", metatiling=4).tile(5, 5, 5)
    shape = (in_tile.shape[0]/2, in_tile.shape[1])
    in_tile.data = np.concatenate([np.ones(shape), np.ones(shape)*2])
    # intersecting at top
    out_tile = BufferedTilePyramid("geodetic").tile(5, 20, 20)
    out_array = raster.extract_from_tile(in_tile, out_tile)
    assert isinstance(out_array, np.ndarray)
    assert np.all(np.where(out_array == 1, True, False))
    # intersecting at bottom
    out_tile = BufferedTilePyramid("geodetic").tile(5, 22, 20)
    out_array = raster.extract_from_tile(in_tile, out_tile)
    assert isinstance(out_array, np.ndarray)
    assert np.all(np.where(out_array == 2, True, False))
    # not intersecting
    try:
        out_tile = BufferedTilePyramid("geodetic").tile(5, 15, 20)
        out_array = raster.extract_from_tile(in_tile, out_tile)
    except ValueError:
        pass


def test_resample_from_array():
    """Resample array using rasterio reproject()."""
    in_tile = BufferedTilePyramid("geodetic").tile(5, 5, 5)
    in_data = np.ones(in_tile.shape)
    # tile from next toom level
    out_tile = BufferedTilePyramid("geodetic").tile(6, 10, 10)
    out_array = raster.resample_from_array(in_data, in_tile.affine, out_tile)
    assert isinstance(out_array, ma.masked_array)
    assert np.all(np.where(out_array == 1, True, False))
    # not intersecting tile
    out_tile = BufferedTilePyramid("geodetic").tile(7, 0, 0)
    out_array = raster.resample_from_array(in_data, in_tile.affine, out_tile)
    assert isinstance(out_array, ma.masked_array)
    assert out_array.mask.all()


def test_create_mosaic():
    """Create mosaic from multiple tiles."""
    for pixelbuffer in [0, 10]:
        tp = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
        tiles = [
            tp.tile(5, row, col) for row, col in product(range(4), range(4))]
        for tile in tiles:
            tile.data = np.ones(tile.shape)
        # 4x4 top left tiles from zoom 5 equal top left tile from zoom 3
        out_mosaic, out_affine = raster.create_mosaic(tiles)
        assert np.all(np.where(out_mosaic == 1, True, False))
        mosaic_bbox = box(
            out_affine[2], out_affine[5]+out_mosaic.shape[1]*out_affine[4],
            out_affine[2]+out_mosaic.shape[2]*out_affine[0], out_affine[5])
        control_bbox = box(*unary_union([tile.bbox for tile in tiles]).bounds)
        assert mosaic_bbox.equals(control_bbox)


def test_prepare_array_iterables():
    """Convert iterable data into a proper array."""
    # input is iterable
    # iterable contains arrays
    data = [np.zeros((1, 1))]
    # output ndarray
    output = raster.prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = raster.prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # iterable contains masked arrays
    data = [ma.empty((1, 1))]
    output = raster.prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = raster.prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # iterable contains masked arrays with full mask
    data = [ma.masked_array(data=np.ones((1, 1)), mask=np.ones((1, 1)))]
    output = raster.prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = raster.prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)


def test_prepare_array_maskedarrays():
    """Convert masked array data into a proper array."""
    # input is ma.masked_array
    data = ma.empty((1, 1, 1))
    # output ndarray
    output = raster.prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = raster.prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # input is ma.masked_array with full mask
    data = ma.masked_array(data=np.ones((1, 1, 1)), mask=np.ones((1, 1, 1)))
    # output ndarray
    output = raster.prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = raster.prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)


def test_prepare_array_ndarrays():
    """Convert ndarray data into a proper array."""
    # input is np.ndarray
    data = np.zeros((1, 1, 1))
    # output ndarray
    output = raster.prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = raster.prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # input is 2D np.ndarray
    data = np.zeros((1, 1))
    # output ndarray
    output = raster.prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = raster.prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)


def test_prepare_array_errors():
    """Convert ndarray data into a proper array."""
    # input is iterable
    data = [None]
    try:
        raster.prepare_array(data)
        raise Exception("test failed")
    except ValueError:
        pass
    # input is not array
    data = 5
    try:
        raster.prepare_array(data)
        raise Exception("test failed")
    except ValueError:
        pass


def test_read_vector_window():
    """Read vector data from read_vector_window."""
    zoom = 4
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/geojson.mapchete"))
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


def test_reproject_geometry():
    """Reproject geometry."""
    with fiona.open(
        os.path.join(TESTDATA_DIR, "landpoly.geojson"), "r"
    ) as src:
        for feature in src:

            # WGS84 to Spherical Mercator
            out_geom = vector.reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs),
                CRS().from_epsg(3857))
            assert out_geom.is_valid

            # WGS84 to LAEA
            out_geom = vector.reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs),
                CRS().from_epsg(3035))
            assert out_geom.is_valid

            # WGS84 to WGS84
            out_geom = vector.reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs),
                CRS().from_epsg(4326))
            assert out_geom.is_valid

    # WGS84 bounds to Spherical Mercator
    big_box = box(-180, -90, 180, 90)
    vector.reproject_geometry(
        big_box, CRS().from_epsg(4326), CRS().from_epsg(3857))

    # WGS84 bounds to Spherical Mercator raising clip error
    try:
        vector.reproject_geometry(
            big_box, CRS().from_epsg(4326), CRS().from_epsg(3857),
            error_on_clip=True)
        raise Exception
    except RuntimeError:
        pass

    # empty geometry
    assert vector.reproject_geometry(
        Polygon(), CRS().from_epsg(4326), CRS().from_epsg(3857)).is_empty
    assert vector.reproject_geometry(
        Polygon(), CRS().from_epsg(4326), CRS().from_epsg(4326)).is_empty


# TODO vector.write_vector_window()
# TODO vector.clean_geometry_type()
# TODO vector.extract_from_tile()
