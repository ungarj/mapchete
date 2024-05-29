import os
import warnings
from test.cli.default import run_cli

import pytest
from rio_cogeo.cogeo import cog_validate
from shapely.geometry import shape

import mapchete
from mapchete.io import fiona_open, rasterio_open


def test_geodetic(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid",
            "geodetic",
            "--concurrency",
            "none",
        ]
    )
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_mercator(cleantopo_br_tif, mp_tmpdir):
    """Automatic mercator tile pyramid creation of raster files."""
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid",
            "mercator",
            "--concurrency",
            "none",
        ]
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_custom_grid(s2_band, mp_tmpdir, custom_grid_json):
    """Automatic mercator tile pyramid creation of raster files."""
    run_cli(
        [
            "convert",
            s2_band,
            mp_tmpdir,
            "--output-pyramid",
            custom_grid_json,
            "--concurrency",
            "none",
        ]
    )

    for zoom, row, col in [(0, 5298, 631)]:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_png(cleantopo_br_tif, mp_tmpdir):
    """Automatic PNG tile pyramid creation of raster files."""
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid",
            "mercator",
            "--output-format",
            "PNG",
            "--concurrency",
            "none",
        ]
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = mp_tmpdir / zoom / row / col + ".png"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with rasterio_open(out_file, "r") as src:
                assert src.meta["driver"] == "PNG"
                assert src.meta["dtype"] == "uint8"
                data = src.read(masked=True)
                assert data.mask.any()


def test_bidx(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out_bidx.tif"
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            single_gtiff,
            "--output-pyramid",
            "geodetic",
            "-z",
            "3",
            "--bidx",
            "1",
            "--concurrency",
            "none",
        ]
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_single_gtiff(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out.tif"
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            single_gtiff,
            "--output-pyramid",
            "geodetic",
            "-z",
            "3",
            "--concurrency",
            "none",
        ]
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_single_gtiff_cog(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out_cog.tif"
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            single_gtiff,
            "--output-pyramid",
            "geodetic",
            "-z",
            "3",
            "--cog",
            "--concurrency",
            "none",
        ]
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
    assert cog_validate(single_gtiff, strict=True)


def test_single_gtiff_overviews(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out.tif"
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            single_gtiff,
            "--output-pyramid",
            "geodetic",
            "-z",
            "7",
            "--overviews",
            "--overviews-resampling-method",
            "bilinear",
            "--workers",
            "1",
            "--concurrency",
            "none",
        ]
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert src.overviews(1)


@pytest.mark.integration
def test_remote_single_gtiff(http_raster, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out.tif"
    run_cli(
        [
            "convert",
            http_raster,
            single_gtiff,
            "--output-pyramid",
            "geodetic",
            "-z",
            "1",
            "--concurrency",
            "none",
        ]
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.any()


def test_dtype(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using dtype scale."""
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid",
            "mercator",
            "--output-dtype",
            "uint8",
            "--concurrency",
            "none",
        ]
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()


def test_scale_ratio(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid",
            "mercator",
            "--output-dtype",
            "uint8",
            "--scale-ratio",
            "0.003",
            "--concurrency",
            "none",
        ]
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()
            assert not data.mask.all()


def test_scale_offset(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid",
            "mercator",
            "--output-dtype",
            "uint8",
            "--scale-offset",
            "1",
            "--concurrency",
            "none",
        ]
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()
            assert not data.mask.all()


def test_clip(cleantopo_br_tif, mp_tmpdir, landpoly):
    """Automatic tile pyramid creation cropping data."""
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid",
            "geodetic",
            "--clip-geometry",
            landpoly,
            "-v",
            "--concurrency",
            "none",
        ],
        output_contains="Process area is empty",
    )


@pytest.mark.parametrize(
    "zoom, tiles",
    [("3", [(4, 15, 15), (2, 3, 0)]), ("3,4", [(2, 3, 0)]), ("4,3", [(2, 3, 0)])],
)
def test_zoom(cleantopo_br_tif, mp_tmpdir, zoom, tiles):
    """Automatic tile pyramid creation using a specific zoom."""
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid",
            "mercator",
            "-z",
            "3",
            "--concurrency",
            "none",
        ]
    )
    for zoom, row, col in tiles:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        assert not out_file.exists()


def test_mapchete(cleantopo_br, mp_tmpdir):
    # prepare data
    with mapchete.open(cleantopo_br.path) as mp:
        list(mp.execute(zoom=[1, 4]))
    run_cli(
        [
            "convert",
            cleantopo_br.path,
            mp_tmpdir,
            "--output-pyramid",
            "geodetic",
            "--output-metatiling",
            "1",
            "-d",
            "--concurrency",
            "none",
            "--bounds",
            "168.75",
            "-90.0",
            "180.0",
            "-78.75",
        ]
    )
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_tiledir(cleantopo_br, mp_tmpdir):
    # prepare data
    with mapchete.open(cleantopo_br.path) as mp:
        list(mp.execute(zoom=[1, 4]))
    run_cli(
        [
            "convert",
            os.path.join(
                cleantopo_br.dict["config_dir"], cleantopo_br.dict["output"]["path"]
            ),
            mp_tmpdir,
            "--output-pyramid",
            "geodetic",
            "--output-metatiling",
            "1",
            "--zoom",
            "1,4",
            "-d",
            "--concurrency",
            "none",
            "--bounds",
            "168.75",
            "-90.0",
            "180.0",
            "-78.75",
        ]
    )
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_geojson(landpoly, mp_tmpdir):
    run_cli(
        [
            "convert",
            landpoly,
            mp_tmpdir,
            "--output-pyramid",
            "geodetic",
            "--zoom",
            "4",
            "--bounds",
            "-101.25",
            "78.75",
            "-90.0",
            "90.0",
            "--concurrency",
            "none",
        ]
    )
    zoom, row, col = (4, 0, 7)
    control = 14
    out_file = mp_tmpdir / zoom / row / col + ".geojson"
    with fiona_open(out_file, "r") as src:
        assert len(src) == control
        for f in src:
            assert shape(f["geometry"]).is_valid


def test_vrt(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid",
            "geodetic",
            "--vrt",
            "--zoom",
            "1,4",
            "--concurrency",
            "none",
        ]
    )
    for zoom in [4, 3, 2, 1]:
        out_file = mp_tmpdir / zoom + ".vrt"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "VRT"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_errors(s2_band_jp2, mp_tmpdir, s2_band, cleantopo_br, landpoly):
    # output format required
    run_cli(
        ["convert", s2_band_jp2, mp_tmpdir, "--output-pyramid", "geodetic"],
        expected_exit_code=1,
        output_contains="Output format required.",
        raise_exc=False,
    )

    # output pyramid reqired
    run_cli(
        ["convert", s2_band, mp_tmpdir],
        expected_exit_code=1,
        output_contains="Output pyramid required.",
        raise_exc=False,
    )

    # prepare data for tiledir input
    with mapchete.open(cleantopo_br.path) as mp:
        mp.execute(zoom=[1, 4])
    tiledir_path = cleantopo_br.dict["config_dir"] / cleantopo_br.dict["output"]["path"]

    # zoom level required
    run_cli(
        [
            "convert",
            tiledir_path,
            mp_tmpdir,
            "--output-pyramid",
            "geodetic",
            "--concurrency",
            "none",
        ],
        expected_exit_code=1,
        output_contains="Zoom levels required.",
        raise_exc=False,
    )

    # incompatible formats
    run_cli(
        [
            "convert",
            tiledir_path,
            mp_tmpdir,
            "--output-pyramid",
            "geodetic",
            "--zoom",
            "5",
            "--output-format",
            "GeoJSON",
            "--concurrency",
            "none",
        ],
        expected_exit_code=1,
        output_contains=("is incompatible with input format"),
        raise_exc=False,
    )

    # unsupported output format extension
    run_cli(
        [
            "convert",
            s2_band_jp2,
            "output.jp2",
            "--output-pyramid",
            "geodetic",
            "--zoom",
            "5",
            "--concurrency",
            "none",
        ],
        expected_exit_code=1,
        output_contains=("currently only single file GeoTIFFs are allowed"),
        raise_exc=False,
    )

    # malformed band index
    run_cli(
        ["convert", s2_band_jp2, "output.tif", "--bidx", "invalid"],
        expected_exit_code=2,
        output_contains=("Invalid value for '--bidx'"),
        raise_exc=False,
    )
