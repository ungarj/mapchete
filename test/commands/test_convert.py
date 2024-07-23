import warnings
from test.commands import TaskCounter

import pytest
from rasterio.enums import Resampling
from rio_cogeo import cog_validate
from shapely.geometry import shape

import mapchete
from mapchete.commands import convert, execute
from mapchete.enums import Concurrency
from mapchete.io.raster.open import rasterio_open
from mapchete.io.vector import fiona_open
from mapchete.tile import BufferedTilePyramid


def test_convert_geodetic(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="geodetic")
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_mercator(cleantopo_br_tif, mp_tmpdir):
    """Automatic mercator tile pyramid creation of raster files."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator")
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_custom_grid_json(s2_band, mp_tmpdir, custom_grid_json):
    """Automatic mercator tile pyramid creation of raster files."""
    convert(s2_band, mp_tmpdir, output_pyramid=custom_grid_json)
    for zoom, row, col in [(0, 5298, 631)]:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_custom_grid_dict(s2_band, mp_tmpdir, custom_grid_json):
    """Automatic mercator tile pyramid creation of raster files."""
    convert(s2_band, mp_tmpdir, output_pyramid=custom_grid_json.read_json())
    for zoom, row, col in [(0, 5298, 631)]:
        out_file = mp_tmpdir / zoom / row / col + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_png(cleantopo_br_tif, mp_tmpdir):
    """Automatic PNG tile pyramid creation of raster files."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", output_format="PNG")

    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".png"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with rasterio_open(out_file, "r") as src:
                assert src.meta["driver"] == "PNG"
                assert src.meta["dtype"] == "uint8"
                data = src.read(masked=True)
                assert data.mask.any()


def test_convert_bidx(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out_bidx.tif"
    convert(cleantopo_br_tif, single_gtiff, output_pyramid="geodetic", zoom=3, bidx=1)
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_convert_single_gtiff(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out.tif"
    convert(cleantopo_br_tif, single_gtiff, output_pyramid="geodetic", zoom=3)
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_convert_single_gtiff_cog(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out_cog.tif"
    convert(cleantopo_br_tif, single_gtiff, output_pyramid="geodetic", zoom=5, cog=True)
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert not data.mask.all()
    assert cog_validate(single_gtiff, strict=True)[0]


def test_convert_single_gtiff_cog_dask(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out_cog.tif"
    convert(
        cleantopo_br_tif,
        single_gtiff,
        output_pyramid="geodetic",
        zoom=5,
        cog=True,
        concurrency=Concurrency.dask,
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert not data.mask.all()
    assert cog_validate(single_gtiff, strict=True)[0]


def test_convert_single_gtiff_overviews(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out.tif"
    convert(
        cleantopo_br_tif,
        single_gtiff,
        output_pyramid="geodetic",
        zoom=7,
        overviews=True,
        overviews_resampling_method=Resampling.bilinear,
        concurrency=Concurrency.none,
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert src.overviews(1)


@pytest.mark.integration
def test_convert_remote_single_gtiff(http_raster, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = mp_tmpdir / "single_out.tif"
    convert(
        http_raster,
        single_gtiff,
        output_pyramid="geodetic",
        zoom=1,
        concurrency=Concurrency.none,
    )
    with rasterio_open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.any()


def test_convert_dtype(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using dtype scale."""
    convert(
        cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", output_dtype="uint8"
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_scale_ratio(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    convert(
        cleantopo_br_tif,
        mp_tmpdir,
        output_pyramid="mercator",
        output_dtype="uint8",
        scale_ratio=0.003,
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()
            assert not data.mask.all()


def test_convert_scale_offset(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    convert(
        cleantopo_br_tif,
        mp_tmpdir,
        output_pyramid="mercator",
        output_dtype="uint8",
        scale_offset=1,
    )
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()
            assert not data.mask.all()


def test_convert_clip(cleantopo_br_tif, mp_tmpdir, landpoly):
    """Automatic tile pyramid creation cropping data."""
    task_counter = TaskCounter()
    convert(
        cleantopo_br_tif,
        mp_tmpdir,
        output_pyramid="geodetic",
        clip_geometry=landpoly,
        observers=[task_counter],
    )
    assert task_counter.tasks == 0


def test_convert_zoom(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using a specific zoom."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", zoom=3)
    for zoom, row, col in [(4, 15, 15), (2, 3, 0)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        assert not out_file.exists()


def test_convert_zoom_minmax(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using min max zoom."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", zoom=[3, 4])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        assert not out_file.exists()


def test_convert_zoom_maxmin(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using max min zoom."""
    convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", zoom=[4, 3])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        assert not out_file.exists()


def test_convert_mapchete(cleantopo_br, mp_tmpdir):
    # prepare data
    execute(cleantopo_br.path, zoom=[1, 3])

    convert(
        cleantopo_br.path,
        mp_tmpdir,
        output_pyramid="geodetic",
        output_metatiling=1,
        zoom=[1, 3],
    )
    for zoom, row, col in [(3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_tiledir(cleantopo_br, mp_tmpdir):
    bounds = BufferedTilePyramid("geodetic").tile(4, 15, 31).bounds
    # prepare data
    with mapchete.open(cleantopo_br.dict) as mp:
        list(mp.execute(zoom=[1, 4]))
    convert(
        cleantopo_br.dict["config_dir"] / cleantopo_br.dict["output"]["path"],
        mp_tmpdir,
        output_pyramid="geodetic",
        output_metatiling=1,
        zoom=[1, 4],
        bounds=bounds,
    )
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".tif"
        with rasterio_open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_gcps(gcps_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    out_file = mp_tmpdir / "gcps_out.tif"
    convert(gcps_tif, out_file, output_pyramid="geodetic", zoom=8)
    with rasterio_open(out_file, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()


def test_convert_geojson(landpoly, mp_tmpdir):
    convert(
        landpoly,
        mp_tmpdir,
        output_pyramid="geodetic",
        zoom=4,
        concurrency=Concurrency.none,
        bounds=(-101.25, 67.5, -90.0, 90.0),
    )
    for (zoom, row, col), control in zip([(4, 0, 7), (4, 1, 7)], [14, 42]):
        out_file = mp_tmpdir / str(zoom) / str(row) / str(col) + ".geojson"
        with fiona_open(out_file, "r") as src:
            assert len(src) == control
            for f in src:
                assert shape(f["geometry"]).is_valid


def test_convert_errors(s2_band_jp2, mp_tmpdir, s2_band, cleantopo_br, landpoly):
    # output format required
    with pytest.raises(ValueError):
        convert(s2_band_jp2, mp_tmpdir, output_pyramid="geodetic")

    # output pyramid reqired
    with pytest.raises(ValueError):
        convert(s2_band, mp_tmpdir)

    # prepare data for tiledir input
    with mapchete.open(cleantopo_br.dict) as mp:
        list(mp.execute(zoom=[1, 4]))
    tiledir_path = cleantopo_br.dict["config_dir"] / cleantopo_br.dict["output"]["path"]

    # zoom level required
    with pytest.raises(ValueError):
        convert(tiledir_path, mp_tmpdir, output_pyramid="geodetic")

    # incompatible formats
    with pytest.raises(ValueError):
        convert(
            tiledir_path,
            mp_tmpdir,
            output_pyramid="geodetic",
            zoom=5,
            output_format="GeoJSON",
        )

    # unsupported output format extension
    with pytest.raises(ValueError):
        convert(s2_band_jp2, "output.jp2", output_pyramid="geodetic", zoom=5)

    # malformed band index
    with pytest.raises(ValueError):
        convert(s2_band_jp2, "output.tif", bidx="invalid")  # type: ignore
