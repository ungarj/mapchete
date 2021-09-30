import fiona
import geobuf
import os
import pytest
import rasterio
from rio_cogeo.cogeo import cog_validate
from shapely.geometry import box, shape
from tilematrix import TilePyramid
import warnings

import mapchete
from mapchete.commands import convert, cp, execute, index, rm


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


def test_cp(mp_tmpdir, cleantopo_br, wkt_geom):
    # generate TileDirectory
    with mapchete.open(
        cleantopo_br.path, bounds=[169.19251592399996, -90, 180, -80.18582802550002]
    ) as mp:
        mp.batch_process(zoom=5)
    out_path = os.path.join(TESTDATA_DIR, cleantopo_br.dict["output"]["path"])

    # copy tiles and subset by bounds
    tiles = cp(
        out_path,
        os.path.join(mp_tmpdir, "bounds"),
        zoom=5,
        bounds=[169.19251592399996, -90, 180, -80.18582802550002],
    )
    assert len(tiles)

    # copy all tiles
    tiles = cp(
        out_path,
        os.path.join(mp_tmpdir, "all"),
        zoom=5,
    )
    assert len(tiles)

    # copy tiles and subset by area
    tiles = cp(out_path, os.path.join(mp_tmpdir, "area"), zoom=5, area=wkt_geom)
    assert len(tiles)

    # copy local tiles without using threads
    tiles = cp(out_path, os.path.join(mp_tmpdir, "nothreads"), zoom=5, multi=1)
    assert len(tiles)


def test_cp_http(mp_tmpdir, http_tiledir):
    # copy tiles and subset by bounds
    tiles = cp(
        http_tiledir, os.path.join(mp_tmpdir, "http"), zoom=1, bounds=[3, 1, 4, 2]
    )
    assert len(tiles)


def test_rm(mp_tmpdir, cleantopo_br):
    # generate TileDirectory
    with mapchete.open(
        cleantopo_br.path, bounds=[169.19251592399996, -90, 180, -80.18582802550002]
    ) as mp:
        mp.batch_process(zoom=5)
    out_path = os.path.join(TESTDATA_DIR, cleantopo_br.dict["output"]["path"])

    # remove tiles
    tiles = rm(out_path, zoom=5)
    assert len(tiles) > 0

    # remove tiles but this time they should already have been removed
    tiles = rm(out_path, zoom=5)
    assert len(tiles) == 0


def test_execute(mp_tmpdir, cleantopo_br, cleantopo_br_tif):
    zoom = 5
    config = cleantopo_br.dict
    config["pyramid"].update(metatiling=1)
    tp = TilePyramid("geodetic")
    tiles = list(tp.tiles_from_bounds(rasterio.open(cleantopo_br_tif).bounds, zoom))
    job = execute(config, zoom=zoom)
    for t in job:
        assert t
    assert len(tiles) == len(job)
    with mapchete.open(config) as mp:
        for t in tiles:
            with rasterio.open(mp.config.output.get_path(t)) as src:
                assert not src.read(masked=True).mask.all()


def test_execute_cancel(mp_tmpdir, cleantopo_br, cleantopo_br_tif):
    zoom = 5
    config = cleantopo_br.dict
    config["pyramid"].update(metatiling=1)
    job = execute(config, zoom=zoom, as_iterator=True)
    for i, t in enumerate(job):
        job.cancel()
        break
    assert i == 0
    assert job.status == "cancelled"


def test_execute_tile(mp_tmpdir, cleantopo_br):
    tile = (5, 30, 63)

    config = cleantopo_br.dict
    config["pyramid"].update(metatiling=1)
    job = execute(config, tile=tile)

    assert len(job) == 1

    with mapchete.open(config) as mp:
        with rasterio.open(
            mp.config.output.get_path(mp.config.output_pyramid.tile(*tile))
        ) as src:
            assert not src.read(masked=True).mask.all()


def test_execute_point(mp_tmpdir, example_mapchete, dummy2_tif):
    """Using bounds from WKT."""
    with rasterio.open(dummy2_tif) as src:
        g = box(*src.bounds)
    job = execute(example_mapchete.path, point=[g.centroid.x, g.centroid.y], zoom=10)
    assert len(job) == 1


def test_convert_geodetic(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    job = convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="geodetic")
    assert len(job)
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_mercator(cleantopo_br_tif, mp_tmpdir):
    """Automatic mercator tile pyramid creation of raster files."""
    job = convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator")
    assert len(job)
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_png(cleantopo_br_tif, mp_tmpdir):
    """Automatic PNG tile pyramid creation of raster files."""
    job = convert(
        cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", output_format="PNG"
    )
    assert len(job)
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".png"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with rasterio.open(out_file, "r") as src:
                assert src.meta["driver"] == "PNG"
                assert src.meta["dtype"] == "uint8"
                data = src.read(masked=True)
                assert data.mask.any()


def test_convert_bidx(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out_bidx.tif")
    job = convert(
        cleantopo_br_tif, single_gtiff, output_pyramid="geodetic", zoom=3, bidx=1
    )
    assert len(job)
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_convert_single_gtiff(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out.tif")
    job = convert(cleantopo_br_tif, single_gtiff, output_pyramid="geodetic", zoom=3)
    assert len(job)
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_convert_single_gtiff_cog(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out_cog.tif")
    job = convert(
        cleantopo_br_tif, single_gtiff, output_pyramid="geodetic", zoom=3, cog=True
    )
    assert len(job)
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
    assert cog_validate(single_gtiff, strict=True)


def test_convert_single_gtiff_overviews(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out.tif")
    job = convert(
        cleantopo_br_tif,
        single_gtiff,
        output_pyramid="geodetic",
        zoom=7,
        overviews=True,
        overviews_resampling_method="bilinear",
        concurrency=None,
    )
    assert len(job)
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert src.overviews(1)


def test_convert_remote_single_gtiff(http_raster, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out.tif")
    job = convert(
        http_raster, single_gtiff, output_pyramid="geodetic", zoom=1, concurrency=None
    )
    assert len(job)
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.any()


def test_convert_dtype(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using dtype scale."""
    job = convert(
        cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", output_dtype="uint8"
    )
    assert len(job)
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_scale_ratio(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    job = convert(
        cleantopo_br_tif,
        mp_tmpdir,
        output_pyramid="mercator",
        output_dtype="uint8",
        scale_ratio=0.003,
    )
    print(job)
    assert len(job)
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()
            assert not data.mask.all()


def test_convert_scale_offset(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    job = convert(
        cleantopo_br_tif,
        mp_tmpdir,
        output_pyramid="mercator",
        output_dtype="uint8",
        scale_offset=1,
    )
    assert len(job)
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()
            assert not data.mask.all()


def test_convert_clip(cleantopo_br_tif, mp_tmpdir, landpoly):
    """Automatic tile pyramid creation cropping data."""
    job = convert(
        cleantopo_br_tif, mp_tmpdir, output_pyramid="geodetic", clip_geometry=landpoly
    )
    assert len(job) == 0


def test_convert_zoom(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using a specific zoom."""
    job = convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", zoom=3)
    assert len(job)
    for zoom, row, col in [(4, 15, 15), (2, 3, 0)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        assert not os.path.isfile(out_file)


def test_convert_zoom_minmax(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using min max zoom."""
    job = convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", zoom=[3, 4])
    assert len(job)
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        assert not os.path.isfile(out_file)


def test_convert_zoom_maxmin(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using max min zoom."""
    job = convert(cleantopo_br_tif, mp_tmpdir, output_pyramid="mercator", zoom=[4, 3])
    assert len(job)
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        assert not os.path.isfile(out_file)


def test_convert_mapchete(cleantopo_br, mp_tmpdir):
    # prepare data
    job = execute(cleantopo_br.path, zoom=[1, 4])
    assert len(job)

    job = convert(
        cleantopo_br.path,
        mp_tmpdir,
        output_pyramid="geodetic",
        output_metatiling=1,
        zoom=[1, 4],
    )
    assert len(job)
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_tiledir(cleantopo_br, mp_tmpdir):
    # prepare data
    with mapchete.open(cleantopo_br.path) as mp:
        mp.batch_process(zoom=[1, 4])
    job = convert(
        os.path.join(
            cleantopo_br.dict["config_dir"], cleantopo_br.dict["output"]["path"]
        ),
        mp_tmpdir,
        output_pyramid="geodetic",
        output_metatiling=1,
        zoom=[1, 4],
    )
    assert len(job)
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_geojson(landpoly, mp_tmpdir):
    job = convert(landpoly, mp_tmpdir, output_pyramid="geodetic", zoom=4)
    assert len(job)
    for (zoom, row, col), control in zip([(4, 0, 7), (4, 1, 7)], [9, 32]):
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col) + ".geojson"]
        )
        with fiona.open(out_file, "r") as src:
            assert len(src) == control
            for f in src:
                assert shape(f["geometry"]).is_valid


def test_convert_geobuf(landpoly, mp_tmpdir):
    # convert to geobuf
    geobuf_outdir = os.path.join(mp_tmpdir, "geobuf")
    job = convert(
        landpoly,
        geobuf_outdir,
        output_pyramid="geodetic",
        zoom=4,
        output_format="Geobuf",
    )
    assert len(job)
    for (zoom, row, col), control in zip([(4, 0, 7), (4, 1, 7)], [9, 32]):
        out_file = os.path.join(
            *[geobuf_outdir, str(zoom), str(row), str(col) + ".pbf"]
        )
        with open(out_file, "rb") as src:
            features = geobuf.decode(src.read())["features"]
            assert len(features) == control
            for f in features:
                assert f["geometry"]["type"] == "Polygon"
                assert shape(f["geometry"]).area


def test_convert_errors(s2_band_jp2, mp_tmpdir, s2_band, cleantopo_br, landpoly):
    # output format required
    with pytest.raises(ValueError):
        convert(s2_band_jp2, mp_tmpdir, output_pyramid="geodetic")

    # output pyramid reqired
    with pytest.raises(ValueError):
        convert(s2_band, mp_tmpdir)

    # prepare data for tiledir input
    with mapchete.open(cleantopo_br.path) as mp:
        mp.batch_process(zoom=[1, 4])
    tiledir_path = os.path.join(
        cleantopo_br.dict["config_dir"], cleantopo_br.dict["output"]["path"]
    )

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
        convert(s2_band_jp2, "output.tif", bidx="invalid")


def test_index_geojson(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    job = execute(cleantopo_br.path, zoom=3)
    assert len(job)

    # generate index for zoom 3
    job = index(cleantopo_br.path, zoom=3, geojson=True)
    assert len(job)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 3
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_geojson_fieldname(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    job = execute(cleantopo_br.path, zoom=3)
    assert len(job)

    # index and rename "location" to "new_fieldname"
    job = index(
        cleantopo_br.path,
        zoom=3,
        geojson=True,
        fieldname="new_fieldname",
    )
    assert len(job)
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        for f in src:
            assert "new_fieldname" in f["properties"]
        assert len(list(src)) == 1


def test_index_geojson_basepath(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    job = execute(cleantopo_br.path, zoom=3)
    assert len(job)

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    job = index(cleantopo_br.path, zoom=3, geojson=True, basepath=basepath)
    assert len(job)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        for f in src:
            assert f["properties"]["location"].startswith(basepath)
        assert len(list(src)) == 1


def test_index_geojson_for_gdal(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    job = execute(cleantopo_br.path, zoom=3)
    assert len(job)

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    job = index(
        cleantopo_br.path, zoom=3, geojson=True, basepath=basepath, for_gdal=True
    )
    assert len(job)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        for f in src:
            assert f["properties"]["location"].startswith("/vsicurl/" + basepath)
        assert len(list(src)) == 1


def test_index_geojson_tile(mp_tmpdir, cleantopo_tl):
    # execute process at zoom 3
    job = execute(cleantopo_tl.path, zoom=3)
    assert len(job)

    # generate index
    job = index(cleantopo_tl.path, tile=(3, 0, 0), geojson=True)
    assert len(job)

    with mapchete.open(cleantopo_tl.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 3
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        assert len(list(src)) == 1


def test_index_geojson_wkt_area(mp_tmpdir, cleantopo_tl, wkt_geom_tl):
    # execute process at zoom 3
    job = execute(cleantopo_tl.path, area=wkt_geom_tl)
    assert len(job)

    # generate index for zoom 3
    job = index(cleantopo_tl.path, geojson=True, area=wkt_geom_tl)
    assert len(job)

    with mapchete.open(cleantopo_tl.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 13
        assert "3.geojson" in files


def test_index_gpkg(mp_tmpdir, cleantopo_br):
    # execute process
    job = execute(cleantopo_br.path, zoom=5)
    assert len(job)

    # generate index
    job = index(cleantopo_br.path, zoom=5, gpkg=True)
    assert len(job)
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.gpkg" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.gpkg")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    job = index(cleantopo_br.path, zoom=5, gpkg=True)
    assert len(job)
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.gpkg" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.gpkg")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_shp(mp_tmpdir, cleantopo_br):
    # execute process
    job = execute(cleantopo_br.path, zoom=5)
    assert len(job)

    # generate index
    job = index(cleantopo_br.path, zoom=5, shp=True)
    assert len(job)
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.shp" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.shp")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    job = index(cleantopo_br.path, zoom=5, shp=True)
    assert len(job)
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.shp" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.shp")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_text(cleantopo_br):
    # execute process
    job = execute(cleantopo_br.path, zoom=5)
    assert len(job)

    # generate index
    job = index(cleantopo_br.path, zoom=5, txt=True)
    assert len(job)
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.txt" in files
    with open(os.path.join(mp.config.output.path, "5.txt")) as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")

    # write again and assert there is no new entry because there is already one
    job = index(cleantopo_br.path, zoom=5, txt=True)
    assert len(job)
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.txt" in files
    with open(os.path.join(mp.config.output.path, "5.txt")) as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")


def test_index_errors(mp_tmpdir, cleantopo_br):
    with pytest.raises(ValueError):
        index(cleantopo_br.path, zoom=5)
