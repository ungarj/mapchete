from concurrent.futures import ProcessPoolExecutor, wait
import shutil
import tempfile

import numpy as np
import numpy.ma as ma
import pytest
from pytest_lazyfixture import lazy_fixture
from rasterio.enums import Compression
from shapely import MultiPoint, box, convex_hull

import mapchete
from mapchete.errors import MapcheteIOError
from mapchete.io.raster.array import resample_from_array
from mapchete.io.raster.mosaic import create_mosaic
from mapchete.io.raster.open import rasterio_open
from mapchete.io.raster.read import (
    RasterWindowMemoryFile,
    read_raster_no_crs,
    read_raster_window,
)
from mapchete.io.raster.referenced_raster import ReferencedRaster, read_raster
from mapchete.io.raster.write import write_raster_window
from mapchete.io.vector import reproject_geometry
from mapchete.path import path_exists
from mapchete.tile import BufferedTilePyramid


def test_read_raster_window_nofile(raster_4band_tile):
    with pytest.raises(IOError):
        read_raster_window("nonexisting_path", raster_4band_tile)


def test_read_raster_window_resampling(cleantopo_br_tif):
    """Assert various resampling options work."""
    tp = BufferedTilePyramid("geodetic")
    with rasterio_open(cleantopo_br_tif, "r") as src:
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
        list(mp.execute(zoom=process_zoom))
        tiles = [
            (tile, mp.config.output.get_path(tile))  # type: ignore
            for tile in mp.config.output_pyramid.tiles_from_bounds(
                mp.config.bounds, process_zoom
            )
            if path_exists(mp.config.output.get_path(tile))  # type: ignore
        ]
        upper_tile = next(mp.get_process_tiles(process_zoom - 1))
        assert len(tiles) > 1
        resampled = resample_from_array(
            array_or_raster=create_mosaic(
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


def test_read_raster_window_filenotfound():
    tile = BufferedTilePyramid("geodetic").tile(zoom=13, row=1918, col=8905)
    with pytest.raises(FileNotFoundError):
        read_raster_window("not_existing.tif", tile)


@pytest.mark.integration
def test_read_raster_window_s3_filenotfound(mp_s3_tmpdir):
    tile = BufferedTilePyramid("geodetic").tile(zoom=13, row=1918, col=8905)
    with pytest.raises(FileNotFoundError):
        read_raster_window(mp_s3_tmpdir / "not_existing.tif", tile)


@pytest.mark.integration
def test_read_raster_window_s3_filenotfound_gdalreaddir(mp_s3_tmpdir):
    tile = BufferedTilePyramid("geodetic").tile(zoom=13, row=1918, col=8905)
    with pytest.raises(FileNotFoundError):
        read_raster_window(
            mp_s3_tmpdir / "not_existing.tif",
            tile,
            gdal_opts=dict(GDAL_DISABLE_READDIR_ON_OPEN=False),
        )


@pytest.mark.integration
@pytest.mark.skip(
    reason="this test should pass with a newer GDAL release: https://github.com/OSGeo/gdal/issues/1900"
)
def test_read_raster_window_s3_invalid_file():
    tile = BufferedTilePyramid("geodetic").tile(zoom=13, row=1918, col=8905)
    with pytest.raises(MapcheteIOError):
        read_raster_window(
            "s3://mapchete-test/landpoly.geojson",
            tile,
            gdal_opts=dict(GDAL_DISABLE_READDIR_ON_OPEN=False),
        )


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
                in_grid=tile, in_data=data, out_profile=out_profile, out_path=path
            )
            with rasterio_open(path, "r") as src:
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
            in_grid=tile,
            in_data=data,
            out_profile=out_profile,
            out_grid=out_tile,
            out_path=path,
        )
        with rasterio_open(path, "r") as src:
            assert src.shape == out_tile.shape
            assert src.read().any()
            assert src.meta["driver"] == out_profile["driver"]
            assert src.transform == out_profile["transform"]
    finally:
        shutil.rmtree(path, ignore_errors=True)


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


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band"),
    ],
)
def test_read_raster_no_crs(path):
    arr = read_raster_no_crs(path)
    assert isinstance(arr, ma.MaskedArray)
    assert not arr.mask.all()


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("raster_4band_aws_s3"),
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_secure_http"),
    ],
)
def test_read_raster_no_crs_remote(path):
    test_read_raster_no_crs(path)


@pytest.mark.aws_s3
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_aws_s3"),
    ],
)
def test_read_raster_no_crs_aws_s3(path):
    test_read_raster_no_crs(path)


@pytest.mark.parametrize(
    "path",
    [lazy_fixture("raster_4band"), lazy_fixture("gcps_tif")],
)
@pytest.mark.parametrize("grid", ["geodetic", "mercator"])
@pytest.mark.parametrize("pixelbuffer", [0, 10, 500])
@pytest.mark.parametrize("zoom", [8, 5])
def test_read_raster_window(path, grid, pixelbuffer, zoom):
    """Read array with read_raster_window."""
    tile_pyramid = BufferedTilePyramid(grid, pixelbuffer=pixelbuffer)
    with rasterio_open(path) as src:
        if src.transform.is_identity and src.gcps:
            bbox = convex_hull(
                MultiPoint([(point.x, point.y) for point in src.gcps[0]])
            ).envelope
        else:
            bbox = reproject_geometry(box(*src.bounds), src.crs, tile_pyramid.crs)
        bands = src.count

    tiles = list(tile_pyramid.tiles_from_geom(bbox, zoom))

    # add edge tile
    tiles.append(tile_pyramid.tile(zoom, 0, 0))

    for tile in tiles:
        width, height = tile.shape

        # iterate through bands of output array
        for band in read_raster_window(path, tile):
            assert isinstance(band, ma.MaskedArray)
            assert band.shape == (width, height)
            if tile.row != 0 and tile.col != 0:
                assert not band.mask.all()

        # iterate through indexes
        for index in range(1, bands + 1):
            band = read_raster_window(path, tile, index)
            assert isinstance(band, ma.MaskedArray)
            assert band.shape == (width, height)
            if tile.row != 0 and tile.col != 0:
                assert not band.mask.all()

        # make sure None index is the same as list of indexes
        for index in [None, list(range(1, bands + 1))]:
            band = read_raster_window(path, tile, index)
            assert isinstance(band, ma.MaskedArray)
            assert band.ndim == 3
            assert band.shape == (bands, width, height)
            if tile.row != 0 and tile.col != 0:
                assert not band.mask.all()


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_secure_http"),
    ],
)
@pytest.mark.parametrize("grid", ["geodetic", "mercator"])
@pytest.mark.parametrize("pixelbuffer", [0, 10, 500])
@pytest.mark.parametrize("zoom", [8, 5])
def test_read_raster_window_remote(path, grid, pixelbuffer, zoom):
    test_read_raster_window(path, grid, pixelbuffer, zoom)


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_aws_s3"),
    ],
)
@pytest.mark.parametrize("grid", ["geodetic", "mercator"])
@pytest.mark.parametrize("pixelbuffer", [0, 10, 500])
@pytest.mark.parametrize("zoom", [8, 5])
def test_read_raster_window_aws_s3(path, grid, pixelbuffer, zoom):
    test_read_raster_window(path, grid, pixelbuffer, zoom)


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band"),
        lazy_fixture("stacta"),
    ],
)
def test_read_raster(path):
    rr = read_raster(path)
    assert isinstance(rr, ReferencedRaster)
    assert not rr.masked_array().mask.all()


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_secure_http"),
    ],
)
def test_read_remote_raster(path):
    rr = read_raster(path)
    assert isinstance(rr, ReferencedRaster)
    assert not rr.masked_array().mask.all()


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_secure_http"),
    ],
)
def test_read_remote_raster_subprocess(path):
    with ProcessPoolExecutor(max_workers=1) as executor:
        future = executor.submit(read_raster_no_crs, path)
        wait([future])
    assert not future.result().mask.all()


@pytest.mark.parametrize("masked", [True, False])
@pytest.mark.parametrize("grid", [lazy_fixture("s2_band_tile")])
def test_read_raster_args(s2_band, masked, grid):
    rr = read_raster(s2_band, grid=grid, masked=masked)
    if masked:
        assert isinstance(rr.array, ma.MaskedArray)
    else:
        assert not isinstance(rr.array, ma.MaskedArray)
        assert isinstance(rr.array, np.ndarray)
    if grid:
        assert rr.array.shape[1:] == grid.shape


@pytest.mark.aws_s3
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_aws_s3"),
        lazy_fixture("aws_s3_stacta"),
    ],
)
def test_read_raster_remote(path):
    test_read_raster(path)


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("http_stacta"),
        lazy_fixture("secure_http_stacta"),
    ],
)
def test_read_raster_integration(path):
    test_read_raster(path)


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band"),
        lazy_fixture("stacta"),
    ],
)
def test_read_raster_tile(path):
    tp = BufferedTilePyramid("geodetic")
    tile = next(tp.tiles_from_bounds(read_raster(path).bounds, zoom=13))
    rr = read_raster(path, grid=tile)
    assert isinstance(rr, ReferencedRaster)
    assert not rr.masked_array().mask.all()


@pytest.mark.aws_s3
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_aws_s3"),
        lazy_fixture("aws_s3_stacta"),
    ],
)
def test_read_raster_tile_remote(path):
    test_read_raster_tile(path)


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("raster_4band_s3"),
        lazy_fixture("raster_4band_http"),
        lazy_fixture("raster_4band_secure_http"),
        lazy_fixture("http_stacta"),
        lazy_fixture("secure_http_stacta"),
    ],
)
def test_read_raster_tile_integration(path):
    test_read_raster_tile(path)
