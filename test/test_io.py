"""Test Mapchete io module."""

import pytest
import shutil
import rasterio
import tempfile
import numpy as np
import numpy.ma as ma
import fiona
from fiona.errors import DriverError
import os
from rasterio.crs import CRS
from rasterio.enums import Compression
from shapely.errors import TopologicalError
from shapely.geometry import shape, box, Polygon, MultiPolygon, LineString
from shapely.ops import unary_union
from tilematrix import Bounds
from itertools import product

import mapchete
from mapchete.config import MapcheteConfig
from mapchete.errors import GeometryTypeError, MapcheteIOError
from mapchete.io import (
    get_best_zoom_level,
    path_exists,
    absolute_path,
    read_json,
    tile_to_zoom_level,
    tiles_exist,
)
from mapchete.io.raster import (
    read_raster_window,
    write_raster_window,
    extract_from_array,
    resample_from_array,
    create_mosaic,
    ReferencedRaster,
    prepare_array,
    RasterWindowMemoryFile,
    read_raster_no_crs,
)
from mapchete.io.vector import (
    read_vector_window,
    reproject_geometry,
    clean_geometry_type,
    segmentize_geometry,
    write_vector_window,
    _repair,
)
from mapchete.tile import BufferedTilePyramid


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


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
    band = read_raster_window(dummy1_3857_tif, tile_pyramid.tile(12, 0, 0))
    assert isinstance(band, ma.MaskedArray)
    assert band.mask.all()
    # not intersecting tile
    tiles.append(tile_pyramid.tile(zoom, 1, 1))  # out of CRS bounds
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
            read_raster_window(cleantopo_br_tif, tile, resampling=resampling)
            for resampling in [
                "nearest",
                "bilinear",
                "cubic",
                "cubic_spline",
                "lanczos",
                "average",
                "mode",
            ]
        ]
        # resampling test:
        assert any(
            [not np.array_equal(w, v) for v, w in zip(outputs[:-1], outputs[1:])]
        )


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
        s2_band, tile, resampling="cubic", src_nodata=0, dst_nodata=0
    )
    assert data.any()
    assert not np.where(data == 1, True, False).any()


def test_read_raster_window_input_list(cleantopo_br):
    process_zoom = 5
    conf = dict(**cleantopo_br.dict)
    conf["output"].update(metatiling=1)
    with mapchete.open(conf) as mp:
        mp.batch_process(process_zoom)
        tiles = [
            (tile, mp.config.output.get_path(tile))
            for tile in mp.config.output_pyramid.tiles_from_bounds(
                mp.config.bounds, process_zoom
            )
            if path_exists(mp.config.output.get_path(tile))
        ]
        upper_tile = next(mp.get_process_tiles(process_zoom - 1))
        assert len(tiles) > 1
        resampled = resample_from_array(
            in_raster=create_mosaic(
                [(tile, read_raster_window(path, tile)) for tile, path in tiles]
            ),
            out_tile=upper_tile,
        )
    resampled2 = read_raster_window(
        [p for _, p in tiles], upper_tile, src_nodata=0, dst_nodata=0
    )
    assert resampled.dtype == resampled2.dtype
    assert resampled.shape == resampled2.shape
    assert np.array_equal(resampled.mask, resampled2.mask)
    # TODO slight rounding errors occur
    assert np.allclose(resampled, resampled2, rtol=0.01)


def test_read_raster_window_retry(invalid_tif):
    tile = BufferedTilePyramid("geodetic").tile(zoom=13, row=1918, col=8905)
    with pytest.raises(MapcheteIOError):
        read_raster_window(invalid_tif, tile)
    with pytest.raises(FileNotFoundError):
        read_raster_window("not_existing.tif", tile)


def test_read_raster_no_crs_errors():
    with tempfile.NamedTemporaryFile() as tmpfile:
        with pytest.raises(MapcheteIOError):
            read_raster_no_crs(tmpfile.name)


def test_write_raster_window():
    """Basic output format writing."""
    path = tempfile.NamedTemporaryFile(delete=False).name
    # standard tile
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    data = ma.masked_array(np.ones((2,) + tile.shape))
    for out_profile in [
        dict(
            driver="GTiff",
            count=2,
            dtype="uint8",
            compress="lzw",
            nodata=0,
            height=tile.height,
            width=tile.width,
            affine=tile.affine,
        ),
        dict(
            driver="GTiff",
            count=2,
            dtype="uint8",
            compress="deflate",
            nodata=0,
            height=tile.height,
            width=tile.width,
            affine=tile.affine,
        ),
        dict(
            driver="PNG",
            count=2,
            dtype="uint8",
            nodata=0,
            height=tile.height,
            width=tile.width,
            compress=None,
            affine=tile.affine,
        ),
    ]:
        try:
            write_raster_window(
                in_tile=tile, in_data=data, out_profile=out_profile, out_path=path
            )
            with rasterio.open(path, "r") as src:
                assert src.read().any()
                assert src.meta["driver"] == out_profile["driver"]
                assert src.transform == tile.affine
                if out_profile["compress"]:
                    assert src.compression == Compression(
                        out_profile["compress"].upper()
                    )
        finally:
            shutil.rmtree(path, ignore_errors=True)
    # with metatiling
    tile = BufferedTilePyramid("geodetic", metatiling=4).tile(5, 1, 1)
    data = ma.masked_array(np.ones((2,) + tile.shape))
    out_tile = BufferedTilePyramid("geodetic").tile(5, 5, 5)
    out_profile = dict(
        driver="GTiff",
        count=2,
        dtype="uint8",
        compress="lzw",
        nodata=0,
        height=out_tile.height,
        width=out_tile.width,
        affine=out_tile.affine,
    )
    try:
        write_raster_window(
            in_tile=tile,
            in_data=data,
            out_profile=out_profile,
            out_tile=out_tile,
            out_path=path,
        )
        with rasterio.open(path, "r") as src:
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
    data = ma.masked_array(np.ones((2,) + tile.shape))
    for out_profile in [
        dict(
            driver="GTiff",
            count=2,
            dtype="uint8",
            compress="lzw",
            nodata=0,
            height=tile.height,
            width=tile.width,
            affine=tile.affine,
        ),
        dict(
            driver="GTiff",
            count=2,
            dtype="uint8",
            compress="deflate",
            nodata=0,
            height=tile.height,
            width=tile.width,
            affine=tile.affine,
        ),
        dict(
            driver="PNG",
            count=2,
            dtype="uint8",
            nodata=0,
            height=tile.height,
            width=tile.width,
            compress=None,
            affine=tile.affine,
        ),
    ]:
        with pytest.raises(DeprecationWarning):
            write_raster_window(
                in_tile=tile, in_data=data, out_profile=out_profile, out_path=path
            )


def test_raster_window_memoryfile():
    """Use context manager for rasterio MemoryFile."""
    tp = BufferedTilePyramid("geodetic")
    tile = tp.tile(5, 5, 5)
    data = ma.masked_array(np.ones((2,) + tile.shape))
    for out_profile in [
        dict(
            driver="GTiff",
            count=2,
            dtype="uint8",
            compress="lzw",
            nodata=0,
            height=tile.height,
            width=tile.width,
            affine=tile.affine,
        ),
        dict(
            driver="GTiff",
            count=2,
            dtype="uint8",
            compress="deflate",
            nodata=0,
            height=tile.height,
            width=tile.width,
            affine=tile.affine,
        ),
        dict(
            driver="PNG",
            count=2,
            dtype="uint8",
            nodata=0,
            height=tile.height,
            width=tile.width,
            compress=None,
            affine=tile.affine,
        ),
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
                        out_profile["compress"].upper()
                    )


def test_write_raster_window_errors():
    """Basic output format writing."""
    tile = BufferedTilePyramid("geodetic").tile(5, 5, 5)
    data = ma.masked_array(np.ndarray((1, 1)))
    profile = {}
    path = ""
    # in_tile
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile="invalid tile",
            in_data=data,
            out_profile=profile,
            out_tile=tile,
            out_path=path,
        )
    # out_tile
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile=tile,
            in_data=data,
            out_profile=profile,
            out_tile="invalid tile",
            out_path=path,
        )
    # in_data
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile=tile,
            in_data="invalid data",
            out_profile=profile,
            out_tile=tile,
            out_path=path,
        )
    # out_profile
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile=tile,
            in_data=data,
            out_profile="invalid profile",
            out_tile=tile,
            out_path=path,
        )
    # out_path
    with pytest.raises(TypeError):
        write_raster_window(
            in_tile=tile, in_data=data, out_profile=profile, out_tile=tile, out_path=999
        )
    # cannot write
    with pytest.raises(ValueError):
        write_raster_window(
            in_tile=tile,
            in_data=data,
            out_profile=profile,
            out_tile=tile,
            out_path="/invalid_path",
        )


def test_extract_from_array():
    """Extract subdata from array."""
    in_tile = BufferedTilePyramid("geodetic", metatiling=4).tile(5, 5, 5)
    shape = (in_tile.shape[0] // 2, in_tile.shape[1])
    data = ma.masked_array(np.concatenate([np.ones(shape), np.ones(shape) * 2]))
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
    in_data = (np.ones(in_tile.shape[1:]),)
    out_tile = BufferedTilePyramid("geodetic").tile(6, 10, 10)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile)
    # deprecated
    resample_from_array(in_data, in_tile.affine, out_tile, nodata=-9999)
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
        create_mosaic([(geo_tile, geo_tile_data), (mer_tile, mer_tile_data)])
    # zoom error
    with pytest.raises(ValueError):
        diff_zoom = tp_geo.tile(2, 1, 0)
        diff_zoom_data = np.ndarray(diff_zoom.shape)
        create_mosaic([(geo_tile, geo_tile_data), (diff_zoom, diff_zoom_data)])
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
        create_mosaic([(geo_tile, geo_tile_data), (diff_type, diff_type_data)])
    # no tiles
    with pytest.raises(ValueError):
        create_mosaic(tiles=[])


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
    zoom = 5
    # multiple tiles on top left corner of tile matrix
    for pixelbuffer in [0, 10]:
        tp = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
        tiles = [
            (tp.tile(zoom, row, col), np.ones(tp.tile(zoom, row, col).shape))
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
            mosaic.affine[5],
        )
        control_bbox = box(*unary_union([t.bbox for t, _ in tiles]).bounds)
        assert mosaic_bbox.equals(control_bbox)
    # multiple tiles on bottom right corner of tile matrix
    for pixelbuffer in [0, 10]:
        tp = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
        tiles = [
            (tp.tile(zoom, row, col), np.ones(tp.tile(zoom, row, col).shape))
            for row, col in product(
                range(tp.matrix_height(zoom) - 4, tp.matrix_height(zoom)),
                range(tp.matrix_width(zoom) - 4, tp.matrix_width(zoom)),
            )
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
            mosaic.affine[5],
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
    east = tp.tile(zoom, row, tp.matrix_width(zoom) - 1)
    mosaic = create_mosaic(
        [
            (west, np.ones(west.shape).astype("uint8")),
            (east, np.ones(east.shape).astype("uint8") * 2),
        ]
    )
    assert isinstance(mosaic, ReferencedRaster)

    # Huge array gets initialized because the two tiles are on opposing sides of the
    # projection area. The below test should pass if the tiles are stitched together next
    # to each other.
    assert mosaic.data.shape == (1, west.height, west.width * 2 - 2 * pixelbuffer)
    assert mosaic.data[0][0][0] == 2
    assert mosaic.data[0][0][-1] == 1

    # If tiles from opposing sides from Antimeridian are mosaicked it will happen that the
    # output mosaic exceeds the CRS bounds (obviously). In such a case the mosaicking
    # function shall make sure that the larger part of the output mosaic shall be inside
    # the CRS bounds.

    # (1) mosaic crosses Antimeridian in the West, larger part is on Western hemisphere:
    tiles_ids = [
        # Western hemisphere tiles
        (zoom, row, 0),
        (zoom, row, 1),
        # Eastern hemisphere tile
        (zoom, row, tp.matrix_width(zoom) - 1),
    ]
    tiles = [
        (tp.tile(*tile_id), np.ones(tp.tile(*tile_id).shape)) for tile_id in tiles_ids
    ]
    mosaic = create_mosaic(tiles)
    control_bounds = Bounds(
        # Eastern tile has to be shifted
        -(360 - tp.tile(*tiles_ids[2]).left),
        tp.tile(*tiles_ids[2]).bottom,
        tp.tile(*tiles_ids[1]).right,
        tp.tile(*tiles_ids[1]).top,
    )
    assert mosaic.bounds == control_bounds

    # (2) mosaic crosses Antimeridian in the West, larger part is on Eastern hemisphere:
    tiles_ids = [
        # Western hemisphere tile
        (zoom, row, 0),
        # Eastern hemisphere tiles
        (zoom, row, tp.matrix_width(zoom) - 1),
        (zoom, row, tp.matrix_width(zoom) - 2),
    ]
    tiles = [
        (tp.tile(*tile_id), np.ones(tp.tile(*tile_id).shape)) for tile_id in tiles_ids
    ]
    mosaic = create_mosaic(tiles)
    control_bounds = Bounds(
        tp.tile(*tiles_ids[2]).left,
        tp.tile(*tiles_ids[2]).bottom,
        # Western tile has to be shifted
        360 + tp.tile(*tiles_ids[0]).right,
        tp.tile(*tiles_ids[0]).top,
    )
    assert mosaic.bounds == control_bounds


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


def test_read_vector_window_errors(invalid_geojson):
    with pytest.raises(FileNotFoundError):
        read_vector_window(
            "invalid_path", BufferedTilePyramid("geodetic").tile(0, 0, 0)
        )
    with pytest.raises(MapcheteIOError):
        read_vector_window(
            invalid_geojson, BufferedTilePyramid("geodetic").tile(0, 0, 0)
        )


def test_reproject_geometry(landpoly):
    """Reproject geometry."""
    with fiona.open(landpoly, "r") as src:
        for feature in src:

            # WGS84 to Spherical Mercator
            out_geom = reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs), CRS().from_epsg(3857)
            )
            assert out_geom.is_valid

            # WGS84 to LAEA
            out_geom = reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs), CRS().from_epsg(3035)
            )
            assert out_geom.is_valid

            # WGS84 to WGS84
            out_geom = reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs), CRS().from_epsg(4326)
            )
            assert out_geom.is_valid

    # WGS84 bounds to Spherical Mercator
    big_box = box(-180, -90, 180, 90)
    reproject_geometry(big_box, CRS().from_epsg(4326), CRS().from_epsg(3857))

    # WGS84 bounds to Spherical Mercator raising clip error
    with pytest.raises(RuntimeError):
        reproject_geometry(
            big_box, CRS().from_epsg(4326), CRS().from_epsg(3857), error_on_clip=True
        )
    outside_box = box(-180, 87, 180, 90)
    assert reproject_geometry(
        outside_box,
        CRS().from_epsg(4326),
        CRS().from_epsg(3857),
    ).is_valid

    # empty geometry
    assert reproject_geometry(
        Polygon(), CRS().from_epsg(4326), CRS().from_epsg(3857)
    ).is_empty
    assert reproject_geometry(
        Polygon(), CRS().from_epsg(4326), CRS().from_epsg(4326)
    ).is_empty

    # CRS parameter
    big_box = box(-180, -90, 180, 90)
    assert reproject_geometry(big_box, 4326, 3857) == reproject_geometry(
        big_box, "4326", "3857"
    )
    with pytest.raises(TypeError):
        reproject_geometry(big_box, 1.0, 1.0)


def test_repair_geometry():
    # invalid LineString
    l = LineString([(0, 0), (0, 0), (0, 0)])
    with pytest.raises(TopologicalError):
        _repair(l)


def test_write_vector_window_errors(landpoly):
    with fiona.open(landpoly) as src:
        feature = next(iter(src))
    with pytest.raises((DriverError, ValueError, TypeError)):
        write_vector_window(
            in_data=["invalid", feature],
            out_tile=BufferedTilePyramid("geodetic").tile(0, 0, 0),
            out_path="/invalid_path",
            out_schema=dict(geometry="Polygon", properties=dict()),
        )


def test_segmentize_geometry():
    """Segmentize function."""
    # Polygon
    polygon = box(-18, -9, 18, 9)
    out = segmentize_geometry(polygon, 1)
    assert out.is_valid
    # wrong type
    with pytest.raises(TypeError):
        segmentize_geometry(polygon.centroid, 1)


def test_clean_geometry_type(geometrycollection):
    """Filter and break up geometries."""
    polygon = box(-18, -9, 18, 9)
    # invalid type
    with pytest.raises(TypeError):
        clean_geometry_type(polygon, "invalid_type")

    # don't return geometry
    with pytest.raises(GeometryTypeError):
        clean_geometry_type(polygon, "LineString", raise_exception=True)

    # return geometry as is
    assert clean_geometry_type(polygon, "Polygon").geom_type == "Polygon"
    assert clean_geometry_type(polygon, "MultiPolygon").geom_type == "Polygon"

    # don't allow multipart geometries
    with pytest.raises(GeometryTypeError):
        clean_geometry_type(
            MultiPolygon([polygon]),
            "Polygon",
            allow_multipart=False,
            raise_exception=True,
        )

    # multipolygons from geometrycollection
    result = clean_geometry_type(
        geometrycollection, "Polygon", allow_multipart=True, raise_exception=False
    )
    assert result.geom_type == "MultiPolygon"
    assert not result.is_empty

    # polygons from geometrycollection
    result = clean_geometry_type(
        geometrycollection, "Polygon", allow_multipart=False, raise_exception=False
    )
    assert result.geom_type == "GeometryCollection"
    assert result.is_empty


@pytest.mark.remote
def test_s3_path_exists(s2_band_remote):
    assert path_exists(s2_band_remote)


@pytest.mark.remote
def test_s3_read_raster_window(s2_band_remote):
    tile = BufferedTilePyramid("geodetic").tile(10, 276, 1071)
    assert read_raster_window(s2_band_remote, tile).any()


@pytest.mark.remote
def test_remote_path_exists(http_raster):
    assert path_exists(http_raster)
    assert not path_exists("http://ungarj.github.io/invalid_file.tif")


def test_absolute_path():
    assert absolute_path(path="file.tif", base_dir="/mnt/data") == "/mnt/data/file.tif"
    assert (
        absolute_path(path="/mnt/data/file.tif", base_dir="/mnt/other_data")
        == "/mnt/data/file.tif"
    )
    with pytest.raises(TypeError):
        absolute_path(path="file.tif", base_dir=None)
    with pytest.raises(TypeError):
        absolute_path(path="file.tif", base_dir="no/abs/dir")
    assert (
        absolute_path(path="https://file.tif", base_dir="/mnt/data")
        == "https://file.tif"
    )


@pytest.mark.remote
def test_read_remote_json(s3_metadata_json, http_metadata_json):
    assert isinstance(read_json(s3_metadata_json), dict)
    assert isinstance(read_json(http_metadata_json), dict)
    with pytest.raises(FileNotFoundError):
        read_json("s3://mapchete-test/invalid_metadata.json")
    with pytest.raises(FileNotFoundError):
        read_json(
            "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/invalid_metadata.json"
        )


def test_tile_to_zoom_level():
    tp_merc = BufferedTilePyramid("mercator")
    tp_geod = BufferedTilePyramid("geodetic")
    zoom = 9
    col = 0

    # mercator from geodetic
    # at Northern boundary
    assert tile_to_zoom_level(tp_merc.tile(zoom, 0, col), tp_geod) == 9
    assert (
        tile_to_zoom_level(tp_merc.tile(zoom, 0, col), tp_geod, matching_method="min")
        == 12
    )
    # at Equator
    assert (
        tile_to_zoom_level(
            tp_merc.tile(zoom, tp_merc.matrix_height(zoom) // 2, col), tp_geod
        )
        == 9
    )
    assert (
        tile_to_zoom_level(
            tp_merc.tile(zoom, tp_merc.matrix_height(zoom) // 2, col),
            tp_geod,
            matching_method="min",
        )
        == 9
    )
    # at Southern boundary
    assert (
        tile_to_zoom_level(
            tp_merc.tile(zoom, tp_merc.matrix_height(zoom) - 1, col), tp_geod
        )
        == 9
    )
    assert (
        tile_to_zoom_level(
            tp_merc.tile(zoom, tp_merc.matrix_height(zoom) - 1, col),
            tp_geod,
            matching_method="min",
        )
        == 12
    )
    assert (
        tile_to_zoom_level(
            BufferedTilePyramid("mercator", metatiling=2, pixelbuffer=20).tile(4, 0, 7),
            BufferedTilePyramid("geodetic", metatiling=8, pixelbuffer=20),
            matching_method="gdal",
        )
        == 4
    )

    # geodetic from mercator
    # at Northern boundary
    assert tile_to_zoom_level(tp_geod.tile(zoom, 0, col), tp_merc) == 2
    with pytest.raises(TopologicalError):
        tile_to_zoom_level(tp_geod.tile(zoom, 0, col), tp_merc, matching_method="min")
    # at Equator
    assert (
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) // 2, col), tp_merc
        )
        == 10
    )
    assert (
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) // 2, col),
            tp_merc,
            matching_method="min",
        )
        == 10
    )
    # at Southern boundary
    assert (
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) - 1, col), tp_merc
        )
        == 2
    )
    with pytest.raises(TopologicalError):
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) - 1, col),
            tp_merc,
            matching_method="min",
        )

    # check wrong method
    with pytest.raises(ValueError):
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) - 1, col),
            tp_merc,
            matching_method="invalid_method",
        )


def test_tiles_exist_local(example_mapchete):
    bounds = (2.0, 0.0, 4.0, 2.0)
    zoom = 10
    with mapchete.open(
        dict(
            example_mapchete.dict,
            pyramid=dict(example_mapchete.dict["pyramid"], metatiling=4),
            output=dict(example_mapchete.dict["output"], metatiling=1),
        ),
        bounds=bounds,
    ) as mp:
        # generate tile directory
        mp.batch_process(zoom=zoom)
        process_tiles = list(mp.config.process_pyramid.tiles_from_bounds(bounds, zoom))
        output_tiles = list(mp.config.output_pyramid.tiles_from_bounds(bounds, zoom))

        # see which files were written and create set for output_tiles and process_tiles
        out_path = os.path.join(
            SCRIPTDIR, example_mapchete.dict["output"]["path"], str(zoom)
        )
        written_output_tiles = set()
        for root, dirs, files in os.walk(out_path):
            for file in files:
                zoom, row = map(int, root.split("/")[-2:])
                col = int(file.split(".")[0])
                written_output_tiles.add(mp.config.output_pyramid.tile(zoom, row, col))
        written_process_tiles = set(
            [mp.config.process_pyramid.intersecting(t)[0] for t in written_output_tiles]
        )

        # process tiles
        existing = set()
        not_existing = set()
        for tile, exists in tiles_exist(
            config=mp.config, process_tiles=process_tiles, multi=4
        ):
            if exists:
                existing.add(tile)
            else:
                not_existing.add(tile)
        assert existing == written_process_tiles
        assert not_existing
        assert set(process_tiles) == existing.union(not_existing)

        # output tiles
        existing = set()
        not_existing = set()
        for tile, exists in tiles_exist(
            config=mp.config, output_tiles=output_tiles, multi=1
        ):
            if exists:
                existing.add(tile)
            else:
                not_existing.add(tile)
        assert existing == written_output_tiles
        assert not_existing
        assert set(output_tiles) == existing.union(not_existing)


def test_tiles_exist_s3(gtiff_s3, mp_s3_tmpdir):
    bounds = (0, 0, 10, 10)
    # bounds = (3, 1, 4, 2)
    zoom = 5
    with mapchete.open(
        dict(
            gtiff_s3.dict,
            pyramid=dict(gtiff_s3.dict["pyramid"], metatiling=8),
            output=dict(gtiff_s3.dict["output"], metatiling=1),
        ),
        bounds=bounds,
        mode="overwrite",
    ) as mp:
        # generate tile directory
        mp.batch_process(zoom=zoom)
        process_tiles = list(mp.config.process_pyramid.tiles_from_bounds(bounds, zoom))
        output_tiles = list(mp.config.output_pyramid.tiles_from_bounds(bounds, zoom))

        # manually check which tiles exist
        written_output_tiles = set()
        for t in output_tiles:
            if mp.config.output_reader.tiles_exist(output_tile=t):
                written_output_tiles.add(t)
        written_process_tiles = set(
            [mp.config.process_pyramid.intersecting(t)[0] for t in written_output_tiles]
        )

        # process tiles
        existing = set()
        not_existing = set()
        for tile, exists in tiles_exist(
            config=mp.config, process_tiles=process_tiles, multi=4
        ):
            if exists:
                existing.add(tile)
            else:
                not_existing.add(tile)
        assert existing == written_process_tiles
        assert set(process_tiles) == existing.union(not_existing)

        # output tiles
        existing = set()
        not_existing = set()
        for tile, exists in tiles_exist(
            config=mp.config, output_tiles=output_tiles, multi=1
        ):
            if exists:
                existing.add(tile)
            else:
                not_existing.add(tile)
        assert existing == written_output_tiles
        assert set(output_tiles) == existing.union(not_existing)
