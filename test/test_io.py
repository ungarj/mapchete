#!/usr/bin/env python
"""Test Mapchete io module."""

import pytest
import shutil
import rasterio
import tempfile
import numpy as np
import numpy.ma as ma
import fiona
from shapely.geometry import shape, box, Polygon, MultiPolygon
from shapely.ops import unary_union
from rasterio.enums import Compression
from rasterio.crs import CRS
from itertools import product

from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTilePyramid
from mapchete.io import get_best_zoom_level
from mapchete.io.raster import (
    read_raster_window, write_raster_window, extract_from_array,
    resample_from_array, create_mosaic, ReferencedRaster, prepare_array,
    RasterWindowMemoryFile)
from mapchete.io.vector import (
    read_vector_window, reproject_geometry, clean_geometry_type,
    segmentize_geometry)


def test_best_zoom_level(dummy1_tif):
    """Test best zoom level determination."""
    assert get_best_zoom_level(dummy1_tif, "geodetic")
    assert get_best_zoom_level(dummy1_tif, "mercator")


def test_read_raster_window(dummy1_tif, minmax_zoom):
    """Read array with read_raster_window."""
    zoom = 8
    # without reproject
    config = MapcheteConfig(minmax_zoom.path)
    rasterfile = config.params_at_zoom(zoom)["input"]["file1"]
    dummy1_bbox = rasterfile.bbox()

    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = list(tile_pyramid.tiles_from_geom(dummy1_bbox, zoom))
    # add edge tile
    tiles.append(tile_pyramid.tile(8, 0, 0))
    for tile in tiles:
        width, height = tile.shape
        for band in read_raster_window(dummy1_tif, tile):
            assert isinstance(band, ma.MaskedArray)
            assert band.shape == (width, height)
        for index in range(1, 4):
            band = read_raster_window(dummy1_tif, tile, index)
            assert isinstance(band, ma.MaskedArray)
            assert band.shape == (width, height)
        for index in [None, [1, 2, 3]]:
            band = read_raster_window(dummy1_tif, tile, index)
            assert isinstance(band, ma.MaskedArray)
            assert band.ndim == 3
            assert band.shape == (3, width, height)


def test_read_raster_window_reproject(dummy1_3857_tif, minmax_zoom):
    """Read array with read_raster_window."""
    zoom = 8
    # with reproject
    config_raw = minmax_zoom.dict
    config_raw["input"].update(file1=dummy1_3857_tif)
    config = MapcheteConfig(config_raw)
    rasterfile = config.params_at_zoom(zoom)["input"]["file1"]
    dummy1_bbox = rasterfile.bbox()

    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = list(tile_pyramid.tiles_from_geom(dummy1_bbox, zoom))
    # target window out of CRS bounds
    band = read_raster_window(
        dummy1_3857_tif, tile_pyramid.tile(12, 0, 0))
    assert isinstance(band, ma.MaskedArray)
    assert band.mask.all()
    # not intersecting tile
    tiles.append(tile_pyramid.tile(zoom, 1, 1))   # out of CRS bounds
    tiles.append(tile_pyramid.tile(zoom, 16, 1))  # out of file bbox
    for tile in tiles:
        for band in read_raster_window(dummy1_3857_tif, tile):
            assert isinstance(band, ma.MaskedArray)
            assert band.shape == tile.shape
        bands = read_raster_window(dummy1_3857_tif, tile, [1])
        assert isinstance(bands, ma.MaskedArray)
        assert bands.shape == tile.shape
    # errors
    with pytest.raises(IOError):
        read_raster_window("nonexisting_path", tile)


def test_read_raster_window_resampling(cleantopo_br_tif):
    """Assert various resampling options work."""
    tp = BufferedTilePyramid("geodetic")
    with rasterio.open(cleantopo_br_tif, "r") as src:
        tiles = tp.tiles_from_bounds(src.bounds, 4)
    for tile in tiles:
        outputs = [
            read_raster_window(
                cleantopo_br_tif, tile, resampling=resampling)
            for resampling in [
                "nearest", "bilinear", "cubic", "cubic_spline", "lanczos",
                "average", "mode"
            ]
        ]
        # resampling test:
        assert any([
            not np.array_equal(w, v) for v, w in zip(outputs[:-1], outputs[1:])
        ])


def test_read_raster_window_partly_overlapping(cleantopo_br_tif):
    """Read array with read_raster_window where window is bigger than file."""
    tile = BufferedTilePyramid("geodetic").tile(4, 15, 31)
    data = read_raster_window(cleantopo_br_tif, tile)
    assert isinstance(data, ma.MaskedArray)
    assert data.mask.any()


def test_read_raster_window_mask(s2_band):
    """No resampling artefacts on mask edges."""
    tile = BufferedTilePyramid("geodetic").tile(zoom=13, row=1918, col=8905)
    data = read_raster_window(
        s2_band, tile, resampling="cubic", src_nodata=0, dst_nodata=0)
    assert data.any()
    assert not np.where(data == 1, True, False).any()


def test_write_raster_window():
    """Basic output format writing."""
    path = tempfile.NamedTemporaryFile(delete=False).name
    # standard tile
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    data = ma.masked_array(np.ones((2, ) + tile.shape))
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
            write_raster_window(
                in_tile=tile, in_data=data, out_profile=out_profile,
                out_path=path
            )
            with rasterio.open(path, 'r') as src:
                assert src.read().any()
                assert src.meta["driver"] == out_profile["driver"]
                assert src.transform == tile.affine
                if out_profile["compress"]:
                    assert src.compression == Compression(
                        out_profile["compress"].upper())
        finally:
            shutil.rmtree(path, ignore_errors=True)
    # with metatiling
    tile = BufferedTilePyramid("geodetic", metatiling=4).tile(5, 1, 1)
    data = ma.masked_array(np.ones((2, ) + tile.shape))
    out_tile = BufferedTilePyramid("geodetic").tile(5, 5, 5)
    out_profile = dict(
            driver="GTiff", count=2, dtype="uint8", compress="lzw", nodata=0,
            height=out_tile.height, width=out_tile.width,
            affine=out_tile.affine)
    try:
        write_raster_window(
            in_tile=tile, in_data=data, out_profile=out_profile,
            out_tile=out_tile, out_path=path
        )
        with rasterio.open(path, 'r') as src:
            assert src.shape == out_tile.shape
            assert src.read().any()
            assert src.meta["driver"] == out_profile["driver"]
            assert src.transform == out_profile["transform"]
    finally:
        shutil.rmtree(path, ignore_errors=True)


def test_write_raster_window_memory():
    """Basic output format writing."""
    path = "memoryfile"
    # standard tile
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    data = ma.masked_array(np.ones((2, ) + tile.shape))
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
        memfile = write_raster_window(
            in_tile=tile, in_data=data, out_profile=out_profile, out_path=path)
        # with rasterio.open(memfile, 'r') as src:
        with memfile.open() as src:
            assert src.read().any()
            assert src.meta["driver"] == out_profile["driver"]
            assert src.transform == tile.affine
            if out_profile["compress"]:
                assert src.compression == Compression(
                    out_profile["compress"].upper())
        memfile.close()


def test_raster_window_memoryfile():
    """Use context manager for rasterio MemoryFile."""
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    data = ma.masked_array(np.ones((2, ) + tile.shape))
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
        with RasterWindowMemoryFile(
            in_tile=tile, in_data=data, out_profile=out_profile
        ) as memfile:
            with memfile.open() as src:
                assert src.read().any()
                assert src.meta["driver"] == out_profile["driver"]
                assert src.transform == tile.affine
                if out_profile["compress"]:
                    assert src.compression == Compression(
                        out_profile["compress"].upper())


def test_write_raster_window_errors():
    """Basic output format writing."""
    tile = BufferedTilePyramid("geodetic").tile(5, 5, 5)
    data = ma.masked_array(np.ndarray((1, 1)))
    profile = {}
    path = ""
    # in_tile
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile="invalid tile", in_data=data, out_profile=profile,
            out_tile=tile, out_path=path
        )
    # out_tile
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile=tile, in_data=data, out_profile=profile,
            out_tile="invalid tile", out_path=path
        )
    # in_data
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile=tile, in_data="invalid data", out_profile=profile,
            out_tile=tile, out_path=path
        )
    # out_profile
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile=tile, in_data=data, out_profile="invalid profile",
            out_tile=tile, out_path=path
        )
    # out_path
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile=tile, in_data=data, out_profile=profile,
            out_tile=tile, out_path=999
        )


def test_extract_from_array():
    """Extract subdata from array."""
    in_tile = BufferedTilePyramid("geodetic", metatiling=4).tile(5, 5, 5)
    shape = (in_tile.shape[0]//2, in_tile.shape[1])
    data = ma.masked_array(
        np.concatenate([np.ones(shape), np.ones(shape)*2])
    )
    # intersecting at top
    out_tile = BufferedTilePyramid("geodetic").tile(5, 20, 20)
    out_array = extract_from_array(
        in_raster=data, in_affine=in_tile.affine, out_tile=out_tile
    )
    assert isinstance(out_array, np.ndarray)
    assert np.all(np.where(out_array == 1, True, False))
    # intersecting at bottom
    out_tile = BufferedTilePyramid("geodetic").tile(5, 22, 20)
    out_array = extract_from_array(
        in_raster=data, in_affine=in_tile.affine, out_tile=out_tile
    )
    assert isinstance(out_array, np.ndarray)
    assert np.all(np.where(out_array == 2, True, False))
    # not intersecting
    out_tile = BufferedTilePyramid("geodetic").tile(5, 15, 20)
    with pytest.raises(ValueError):
        out_array = extract_from_array(
            in_raster=data, in_affine=in_tile.affine, out_tile=out_tile
        )


def test_resample_from_array():
    """Resample array using rasterio reproject()."""
    in_tile = BufferedTilePyramid("geodetic").tile(5, 5, 5)
    in_data = np.ones(in_tile.shape)
    # tile from next toom level
    out_tile = BufferedTilePyramid("geodetic").tile(6, 10, 10)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile)
    assert isinstance(out_array, ma.masked_array)
    assert np.all(np.where(out_array == 1, True, False))
    # not intersecting tile
    out_tile = BufferedTilePyramid("geodetic").tile(7, 0, 0)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile)
    assert isinstance(out_array, ma.masked_array)
    assert out_array.mask.all()
    # data as tuple
    in_data = (np.ones(in_tile.shape[1:]), )
    out_tile = BufferedTilePyramid("geodetic").tile(6, 10, 10)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile)
    # errors
    with pytest.raises(TypeError):
        in_data = "invalid_type"
        resample_from_array(in_data, in_tile.affine, out_tile)
    with pytest.raises(TypeError):
        in_data = np.ones(in_tile.shape[0])
        resample_from_array(in_data, in_tile.affine, out_tile)


def test_create_mosaic_errors():
    """Check error handling of create_mosaic()."""
    tp_geo = BufferedTilePyramid("geodetic")
    tp_mer = BufferedTilePyramid("mercator")
    geo_tile = tp_geo.tile(1, 0, 0)
    geo_tile_data = np.ndarray(geo_tile.shape)
    mer_tile = tp_mer.tile(1, 1, 0)
    mer_tile_data = np.ndarray(mer_tile.shape)
    # tiles error
    with pytest.raises(TypeError):
        create_mosaic("invalid tiles")
    with pytest.raises(TypeError):
        create_mosaic(["invalid tiles"])
    # CRS error
    with pytest.raises(ValueError):
        create_mosaic([
            (geo_tile, geo_tile_data), (mer_tile, mer_tile_data)
        ])
    # zoom error
    with pytest.raises(ValueError):
        diff_zoom = tp_geo.tile(2, 1, 0)
        diff_zoom_data = np.ndarray(diff_zoom.shape)
        create_mosaic([
            (geo_tile, geo_tile_data), (diff_zoom, diff_zoom_data)
        ])
    # tile data error
    with pytest.raises(TypeError):
        # for one tile
        create_mosaic([(geo_tile, None)])
    with pytest.raises(TypeError):
        # for multiple tiles
        create_mosaic([(geo_tile, None), (geo_tile, None)])
    # tile data type error
    with pytest.raises(TypeError):
        diff_type = tp_geo.tile(1, 1, 0)
        diff_type_data = np.ndarray(diff_zoom.shape).astype("int")
        create_mosaic([
            (geo_tile, geo_tile_data), (diff_type, diff_type_data)
        ])


def test_create_mosaic():
    """Create mosaic from tiles."""
    tp = BufferedTilePyramid("geodetic")
    # quick return mosaic if there is just one tile
    tile = tp.tile(3, 3, 3)
    data = np.ones(tile.shape)
    mosaic = create_mosaic([(tile, data)])
    assert isinstance(mosaic, ReferencedRaster)
    assert np.array_equal(data, mosaic.data)
    assert tile.affine == mosaic.affine
    # multiple tiles
    for pixelbuffer in [0, 10]:
        tp = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
        tiles = [
            (tp.tile(5, row, col), np.ones(tp.tile(5, row, col).shape))
            for row, col in product(range(4), range(4))
        ]
        # 4x4 top left tiles from zoom 5 equal top left tile from zoom 3
        # also use tile generator
        mosaic = create_mosaic((t for t in tiles))
        assert isinstance(mosaic, ReferencedRaster)
        assert np.all(np.where(mosaic.data == 1, True, False))
        mosaic_bbox = box(
            mosaic.affine[2],
            mosaic.affine[5] + mosaic.data.shape[1] * mosaic.affine[4],
            mosaic.affine[2] + mosaic.data.shape[2] * mosaic.affine[0],
            mosaic.affine[5]
        )
        control_bbox = box(*unary_union([t.bbox for t, _ in tiles]).bounds)
        assert mosaic_bbox.equals(control_bbox)


def test_create_mosaic_antimeridian():
    """Create mosaic using tiles on opposing antimeridian sides."""
    zoom = 5
    row = 0
    pixelbuffer = 5
    tp = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    west = tp.tile(zoom, row, 0)
    east = tp.tile(zoom, row, tp.matrix_width(zoom)-1)
    mosaic = create_mosaic([
        (west, np.ones(west.shape).astype("uint8")),
        (east, np.ones(east.shape).astype("uint8") * 2)
    ])
    assert isinstance(mosaic, ReferencedRaster)
    # Huge array gets initialized because the two tiles are on opposing sides
    # of the projection area. The below test should pass if the tiles are
    # stitched together next to each other.
    assert mosaic.data.shape == (1, west.height, west.width*2-2*pixelbuffer)
    assert mosaic.data[0][0][0] == 2
    assert mosaic.data[0][0][-1] == 1


def test_prepare_array_iterables():
    """Convert iterable data into a proper array."""
    # input is iterable
    # iterable contains arrays
    data = [np.zeros((1, 1))]
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # iterable contains masked arrays
    data = [ma.empty((1, 1))]
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # iterable contains masked arrays with full mask
    data = [ma.masked_array(data=np.ones((1, 1)), mask=np.ones((1, 1)))]
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)


def test_prepare_array_maskedarrays():
    """Convert masked array data into a proper array."""
    # input is ma.masked_array
    data = ma.empty((1, 1, 1))
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # input is ma.masked_array with full mask
    data = ma.masked_array(data=np.ones((1, 1, 1)), mask=np.ones((1, 1, 1)))
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)


def test_prepare_array_ndarrays():
    """Convert ndarray data into a proper array."""
    # input is np.ndarray
    data = np.zeros((1, 1, 1))
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # input is 2D np.ndarray
    data = np.zeros((1, 1))
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)


def test_prepare_array_errors():
    """Convert ndarray data into a proper array."""
    # input is iterable
    data = [None]
    try:
        prepare_array(data)
        raise Exception()
    except ValueError:
        pass
    # input is not array
    data = 5
    try:
        prepare_array(data)
        raise Exception()
    except ValueError:
        pass


def test_read_vector_window(geojson, landpoly_3857):
    """Read vector data from read_vector_window."""
    zoom = 4
    config = MapcheteConfig(geojson.path)
    vectorfile = config.params_at_zoom(zoom)["input"]["file1"]
    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = tile_pyramid.tiles_from_geom(vectorfile.bbox(), zoom)
    feature_count = 0
    for tile in tiles:
        for feature in read_vector_window(vectorfile.path, tile):
            assert "properties" in feature
            assert shape(feature["geometry"]).is_valid
            feature_count += 1
    assert feature_count
    # into different CRS
    raw_config = geojson.dict
    raw_config["input"].update(file1=landpoly_3857)
    config = MapcheteConfig(raw_config)
    vectorfile = config.params_at_zoom(zoom)["input"]["file1"]
    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = tile_pyramid.tiles_from_geom(vectorfile.bbox(), zoom)
    feature_count = 0
    for tile in tiles:
        for feature in read_vector_window(vectorfile.path, tile):
            assert "properties" in feature
            assert shape(feature["geometry"]).is_valid
            feature_count += 1
    assert feature_count


def test_reproject_geometry(landpoly):
    """Reproject geometry."""
    with fiona.open(landpoly, "r") as src:
        for feature in src:

            # WGS84 to Spherical Mercator
            out_geom = reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs),
                CRS().from_epsg(3857))
            assert out_geom.is_valid

            # WGS84 to LAEA
            out_geom = reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs),
                CRS().from_epsg(3035))
            assert out_geom.is_valid

            # WGS84 to WGS84
            out_geom = reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs),
                CRS().from_epsg(4326))
            assert out_geom.is_valid

    # WGS84 bounds to Spherical Mercator
    big_box = box(-180, -90, 180, 90)
    reproject_geometry(
        big_box, CRS().from_epsg(4326), CRS().from_epsg(3857))

    # WGS84 bounds to Spherical Mercator raising clip error
    with pytest.raises(RuntimeError):
        reproject_geometry(
            big_box, CRS().from_epsg(4326), CRS().from_epsg(3857),
            error_on_clip=True)

    # empty geometry
    assert reproject_geometry(
        Polygon(), CRS().from_epsg(4326), CRS().from_epsg(3857)).is_empty
    assert reproject_geometry(
        Polygon(), CRS().from_epsg(4326), CRS().from_epsg(4326)).is_empty

    # CRS parameter
    big_box = box(-180, -90, 180, 90)
    assert reproject_geometry(
        big_box, 4326, 3857) == reproject_geometry(
        big_box, "4326", "3857")
    with pytest.raises(TypeError):
        reproject_geometry(big_box, 1.0, 1.0)


def test_segmentize_geometry():
    """Segmentize function."""
    # Polygon
    polygon = box(-18, -9, 18, 9)
    out = segmentize_geometry(polygon, 1)
    assert out.is_valid
    # wrong type
    with pytest.raises(TypeError):
        segmentize_geometry(polygon.centroid, 1)


def test_clean_geometry_type():
    """Filter and break up geometries."""
    polygon = box(-18, -9, 18, 9)
    # invalid type
    with pytest.raises(TypeError):
        clean_geometry_type(polygon, "invalid_type")

    # don't return geometry
    assert clean_geometry_type(polygon, "LineString") is None

    # return geometry as is
    assert clean_geometry_type(polygon, "Polygon").geom_type == "Polygon"
    assert clean_geometry_type(polygon, "MultiPolygon").geom_type == "Polygon"

    # don't allow multipart geometries
    assert clean_geometry_type(
        MultiPolygon([polygon]), "Polygon", allow_multipart=False) is None

# TODO write_vector_window()
# TODO extract_from_tile()
