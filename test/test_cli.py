"""Test Mapchete main module and processing."""

from click.testing import CliRunner
import fiona
import os
import pytest
from shapely import wkt
import rasterio
from rasterio.io import MemoryFile
from rio_cogeo.cogeo import cog_validate
import yaml

import mapchete
from mapchete.cli.main import main as mapchete_cli
from mapchete.errors import MapcheteProcessOutputError


def run_cli(args, expected_exit_code=0, output_contains=None, raise_exc=True):
    result = CliRunner(env=dict(MAPCHETE_TEST="TRUE")).invoke(mapchete_cli, args)
    if output_contains:
        assert output_contains in result.output
    if raise_exc and result.exception:
        raise result.exception
    assert result.exit_code == expected_exit_code


def test_main(mp_tmpdir):
    # """Main CLI."""
    for command in ["create", "execute", "create"]:
        run_cli(
            [command],
            expected_exit_code=2,
            output_contains="Error: Missing argument",
            raise_exc=False
        )

    run_cli(
        ["formats"],
        expected_exit_code=0
    )

    run_cli(
        ["invalid_command"],
        expected_exit_code=2,
        output_contains="Error: No such command",
        raise_exc=False
    )


def test_missing_input_file(mp_tmpdir):
    """Check if IOError is raised if input-file is invalid."""
    run_cli(
        ["execute", "process.mapchete", "--input-file", "invalid.tif"],
        expected_exit_code=2,
        output_contains="Path 'process.mapchete' does not exist.",
        raise_exc=False
    )


def test_create_and_execute(mp_tmpdir, cleantopo_br_tif):
    """Run mapchete create and execute."""
    temp_mapchete = os.path.join(mp_tmpdir, "temp.mapchete")
    temp_process = os.path.join(mp_tmpdir, "temp.py")
    out_format = "GTiff"
    # create from template
    run_cli(
        [
            "create", temp_mapchete, temp_process, out_format,
            "--pyramid-type", "geodetic"
        ],
        expected_exit_code=0
    )
    # edit configuration
    with open(temp_mapchete, "r") as config_file:
        config = yaml.load(config_file)
        config["output"].update(bands=1, dtype="uint8", path=mp_tmpdir)
    with open(temp_mapchete, "w") as config_file:
        config_file.write(yaml.dump(config, default_flow_style=False))
    # run process for single tile, this creates an empty output error
    with pytest.raises(MapcheteProcessOutputError):
        run_cli(
            [
                "execute", temp_mapchete, "--tile", "6", "62", "124",
                "--input-file", cleantopo_br_tif, "-d"
            ],
            expected_exit_code=-1
        )


def test_create_existing(mp_tmpdir):
    """Run mapchete create and execute."""
    temp_mapchete = os.path.join(mp_tmpdir, "temp.mapchete")
    temp_process = os.path.join(mp_tmpdir, "temp.py")
    out_format = "GTiff"
    # create files from template
    args = [
        "create", temp_mapchete, temp_process, out_format, "--pyramid-type", "geodetic"
    ]
    run_cli(args)
    # try to create again
    with pytest.raises((IOError, OSError)):  # for python 2 and 3
        run_cli(args, expected_exit_code=-1)


def test_execute_multiprocessing(mp_tmpdir, cleantopo_br, cleantopo_br_tif):
    """Run mapchete execute with multiple workers."""
    temp_mapchete = os.path.join(mp_tmpdir, "temp.mapchete")
    temp_process = os.path.join(mp_tmpdir, "temp.py")
    out_format = "GTiff"
    # create from template
    run_cli([
        "create",
        temp_mapchete,
        temp_process,
        out_format,
        "--pyramid-type", "geodetic"
    ])
    # edit configuration
    with open(temp_mapchete, "r") as config_file:
        config = yaml.load(config_file)
        config["output"].update(bands=1, dtype="uint8", path=mp_tmpdir)
    with open(temp_mapchete, "w") as config_file:
        config_file.write(yaml.dump(config, default_flow_style=False))
    # run process with multiprocessing
    with pytest.raises(MapcheteProcessOutputError):
        run_cli([
            "execute",
            temp_mapchete,
            "--zoom", "5",
            "--input-file", cleantopo_br_tif,
            "-m", "2",
            "-d"
        ])
    # run example process with multiprocessing
    run_cli([
        "execute",
        cleantopo_br.path,
        "--zoom", "5",
        "-m", "2",
        "-d"
    ])


def test_execute_debug(mp_tmpdir, example_mapchete):
    """Using debug output."""
    run_cli(
        [
            "execute", example_mapchete.path, "-t", "10", "500", "1040",
            "--debug"
        ]
    )


def test_execute_vrt(mp_tmpdir, cleantopo_br):
    """Using debug output."""
    run_cli(["execute", cleantopo_br.path, "-z", "5", "--vrt"])
    with mapchete.open(cleantopo_br.dict) as mp:
        vrt_path = os.path.join(mp.config.output.path, "5.vrt")
        with rasterio.open(vrt_path) as src:
            assert src.read().any()

    # run again, this time with custom output directory
    run_cli(
        ["execute", cleantopo_br.path, "-z", "5", "--vrt", "--idx-out-dir", mp_tmpdir]
    )
    with mapchete.open(cleantopo_br.dict) as mp:
        vrt_path = os.path.join(mp_tmpdir, "5.vrt")
        with rasterio.open(vrt_path) as src:
            assert src.read().any()

    # run with single tile
    run_cli(
        ["execute", cleantopo_br.path, "-t", "5", "3", "7", "--vrt"]
    )

    # no new entries
    run_cli(
        ["execute", cleantopo_br.path, "-t", "5", "0", "0", "--vrt"]
    )


def test_execute_verbose(mp_tmpdir, example_mapchete):
    """Using verbose output."""
    run_cli(["execute", example_mapchete.path, "-t", "10", "500", "1040", "--verbose"])


def test_execute_logfile(mp_tmpdir, example_mapchete):
    """Using logfile."""
    logfile = os.path.join(mp_tmpdir, "temp.log")
    run_cli([
        "execute", example_mapchete.path, "-t", "10", "500", "1040", "--logfile", logfile
    ])
    assert os.path.isfile(logfile)
    with open(logfile) as log:
        assert "DEBUG" in log.read()


def test_execute_wkt_bounds(mp_tmpdir, example_mapchete, wkt_geom):
    """Using bounds from WKT."""
    run_cli(["execute", example_mapchete.path, "--wkt-geometry", wkt_geom])


def test_execute_point(mp_tmpdir, example_mapchete, wkt_geom):
    """Using bounds from WKT."""
    g = wkt.loads(wkt_geom)
    run_cli([
        "execute", example_mapchete.path,
        "--point", str(g.centroid.x), str(g.centroid.y)
    ])


def test_formats(capfd):
    """Output of mapchete formats command."""
    run_cli(["formats"])
    err = capfd.readouterr()[1]
    assert not err
    run_cli(["formats", "-i"])
    err = capfd.readouterr()[1]
    assert not err
    run_cli(["formats", "-o"])
    err = capfd.readouterr()[1]
    assert not err


def test_convert_geodetic(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    run_cli(["convert", cleantopo_br_tif, mp_tmpdir, "--output-pyramid", "geodetic"])
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_mercator(cleantopo_br_tif, mp_tmpdir):
    """Automatic mercator tile pyramid creation of raster files."""
    run_cli(["convert", cleantopo_br_tif, mp_tmpdir, "--output-pyramid", "mercator"])
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_png(cleantopo_br_tif, mp_tmpdir):
    """Automatic PNG tile pyramid creation of raster files."""
    run_cli([
        "convert",
        cleantopo_br_tif,
        mp_tmpdir,
        "--output-pyramid", "mercator",
        "--output-format", "PNG"
    ])
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col) + ".png"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "PNG"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_bidx(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out_bidx.tif")
    run_cli([
        "convert",
        cleantopo_br_tif,
        single_gtiff,
        "--output-pyramid", "geodetic",
        "-z", "3",
        "--bidx", "1"
    ])
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_convert_single_gtiff(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out.tif")
    run_cli([
        "convert",
        cleantopo_br_tif,
        single_gtiff,
        "--output-pyramid", "geodetic",
        "-z", "3"
    ])
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert not src.overviews(1)


def test_convert_single_gtiff_cog(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out_cog.tif")
    run_cli([
        "convert",
        cleantopo_br_tif,
        single_gtiff,
        "--output-pyramid", "geodetic",
        "-z", "3",
        "--cog"
    ])
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
    assert cog_validate(single_gtiff, strict=True)


def test_convert_single_gtiff_overviews(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out.tif")
    run_cli([
        "convert",
        cleantopo_br_tif,
        single_gtiff,
        "--output-pyramid", "geodetic",
        "-z", "3",
        "--overviews",
        "--overviews-resampling-method", "bilinear"
    ])
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.mask.any()
        assert src.overviews(1)


def test_convert_remote_single_gtiff(http_raster, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    single_gtiff = os.path.join(mp_tmpdir, "single_out.tif")
    run_cli([
        "convert",
        http_raster,
        single_gtiff,
        "--output-pyramid", "geodetic",
        "-z", "1"
    ])
    with rasterio.open(single_gtiff, "r") as src:
        assert src.meta["driver"] == "GTiff"
        assert src.meta["dtype"] == "uint16"
        data = src.read(masked=True)
        assert data.any()


def test_convert_dtype(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using dtype scale."""
    run_cli([
        "convert",
        cleantopo_br_tif,
        mp_tmpdir,
        "--output-pyramid", "mercator",
        "--output-dtype", "uint8"
    ])
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_scale_ratio(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    run_cli([
        "convert",
        cleantopo_br_tif,
        mp_tmpdir,
        "--output-pyramid", "mercator",
        "--output-dtype", "uint8",
        "--scale-ratio", "0.003"
    ])
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
    run_cli([
        "convert",
        cleantopo_br_tif,
        mp_tmpdir,
        "--output-pyramid", "mercator",
        "--output-dtype", "uint8",
        "--scale-offset", "1"
    ])
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
    run_cli(
        [
            "convert",
            cleantopo_br_tif,
            mp_tmpdir,
            "--output-pyramid", "geodetic",
            "--clip-geometry", landpoly,
            "-d"
        ],
        output_contains="Process area is empty"
    )


def test_convert_zoom(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using a specific zoom."""
    run_cli([
        "convert",
        cleantopo_br_tif,
        mp_tmpdir,
        "--output-pyramid", "mercator",
        "-z", "3"
    ])
    for zoom, row, col in [(4, 15, 15), (2, 3, 0)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        assert not os.path.isfile(out_file)


def test_convert_zoom_minmax(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using min max zoom."""
    run_cli([
        "convert",
        cleantopo_br_tif,
        mp_tmpdir,
        "--output-pyramid", "mercator",
        "-z", "3,4"
    ])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        assert not os.path.isfile(out_file)


def test_convert_zoom_maxmin(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using max min zoom."""
    run_cli([
        "convert",
        cleantopo_br_tif,
        mp_tmpdir,
        "--output-pyramid", "mercator",
        "-z", "4,3"
    ])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        assert not os.path.isfile(out_file)


def test_convert_mapchete(cleantopo_br, mp_tmpdir):
    # prepare data
    with mapchete.open(cleantopo_br.path) as mp:
        mp.batch_process(zoom=[1, 4])
    run_cli([
        "convert",
        cleantopo_br.path,
        mp_tmpdir,
        "--output-pyramid", "geodetic",
        "--output-metatiling", "1",
        "-d",
    ])
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
    run_cli([
        "convert",
        os.path.join(
            cleantopo_br.dict["config_dir"], cleantopo_br.dict["output"]["path"]
        ),
        mp_tmpdir,
        "--output-pyramid", "geodetic",
        "--output-metatiling", "1",
        "--zoom", "1,4",
        "-d",
    ])
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col) + ".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_convert_errors(s2_band_jp2, mp_tmpdir, s2_band, cleantopo_br, landpoly):
    # output format required
    run_cli(
        ["convert", s2_band_jp2, mp_tmpdir, "--output-pyramid", "geodetic"],
        expected_exit_code=2,
        output_contains="Output format required.",
        raise_exc=False
    )

    # output pyramid reqired
    run_cli(
        ["convert", s2_band, mp_tmpdir],
        expected_exit_code=2,
        output_contains="Output pyramid required.",
        raise_exc=False
    )

    # prepare data for tiledir input
    with mapchete.open(cleantopo_br.path) as mp:
        mp.batch_process(zoom=[1, 4])
    tiledir_path = os.path.join(
        cleantopo_br.dict["config_dir"], cleantopo_br.dict["output"]["path"]
    )

    # zoom level required
    run_cli(
        [
            "convert",
            tiledir_path,
            mp_tmpdir,
            "--output-pyramid", "geodetic",
        ],
        expected_exit_code=2,
        output_contains="Zoom levels required.",
        raise_exc=False
    )

    # incompatible formats
    run_cli(
        [
            "convert",
            tiledir_path,
            mp_tmpdir,
            "--output-pyramid", "geodetic",
            "--zoom", "5",
            "--output-format", "GeoJSON"
        ],
        expected_exit_code=2,
        output_contains=(
            "Output format type (vector) is incompatible with input format (raster)."
        ),
        raise_exc=False
    )

    # unsupported format
    run_cli(
        [
            "convert",
            landpoly,
            mp_tmpdir,
            "--output-pyramid", "geodetic",
            "--zoom", "5",
            "--output-format", "GeoJSON"
        ],
        expected_exit_code=2,
        output_contains=("driver vector_file is not supported"),
        raise_exc=False
    )

    # unsupported output format extension
    run_cli(
        [
            "convert",
            s2_band_jp2,
            "output.jp2",
            "--output-pyramid", "geodetic",
            "--zoom", "5",
        ],
        expected_exit_code=2,
        output_contains=("Could not determine output from extension"),
        raise_exc=False
    )

    # malformed band index
    run_cli(
        [
            "convert",
            s2_band_jp2,
            "output.tif",
            "--bidx", "invalid"
        ],
        expected_exit_code=2,
        output_contains=("Invalid value for '--bidx'"),
        raise_exc=False
    )


def test_serve_cli_params(cleantopo_br, mp_tmpdir):
    """Test whether different CLI params pass."""
    # assert too few arguments error
    with pytest.raises(SystemExit):
        run_cli(["serve"])

    for args in [
        ["serve", cleantopo_br.path],
        ["serve", cleantopo_br.path, "--port", "5001"],
        ["serve", cleantopo_br.path, "--internal-cache", "512"],
        ["serve", cleantopo_br.path, "--zoom", "5"],
        ["serve", cleantopo_br.path, "--bounds", "-1", "-1", "1", "1"],
        ["serve", cleantopo_br.path, "--overwrite"],
        ["serve", cleantopo_br.path, "--readonly"],
        ["serve", cleantopo_br.path, "--memory"],
        ["serve", cleantopo_br.path, "--input-file", cleantopo_br.path],
    ]:
        run_cli(args)


def test_serve(client, mp_tmpdir):
    """Mapchete serve with default settings."""
    tile_base_url = "/wmts_simple/1.0.0/dem_to_hillshade/default/WGS84/"
    for url in ["/"]:
        response = client.get(url)
        assert response.status_code == 200
    for url in [
        tile_base_url + "5/30/62.png",
        tile_base_url + "5/30/63.png",
        tile_base_url + "5/31/62.png",
        tile_base_url + "5/31/63.png",
    ]:
        response = client.get(url)
        assert response.status_code == 200
        img = response.data
        with MemoryFile(img) as memfile:
            with memfile.open() as dataset:
                data = dataset.read()
                # get alpha band and assert some pixels are masked
                assert data[3].any()
    # test outside zoom range
    response = client.get(tile_base_url + "6/31/63.png")
    assert response.status_code == 200
    img = response.data
    with MemoryFile(img) as memfile:
        with memfile.open() as dataset:
            data = dataset.read()
            assert not data.all()
    # test invalid url
    response = client.get(tile_base_url + "invalid_url")
    assert response.status_code == 404


def test_index_geojson(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    run_cli(["execute", cleantopo_br.path, "-z", "3", "--debug"])

    # generate index for zoom 3
    run_cli(["index", cleantopo_br.path, "-z", "3", "--geojson", "--debug"])
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
    run_cli(["execute", cleantopo_br.path, "-z", "3", "--debug"])

    # index and rename "location" to "new_fieldname"
    run_cli([
        "index", cleantopo_br.path, "-z", "3", "--geojson", "--debug",
        "--fieldname", "new_fieldname"
    ])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        for f in src:
            assert "new_fieldname" in f["properties"]
        assert len(list(src)) == 1


def test_index_geojson_basepath(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    run_cli(["execute", cleantopo_br.path, "-z", "3", "--debug"])

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    run_cli([
        "index", cleantopo_br.path, "-z", "3", "--geojson", "--debug",
        "--basepath", basepath
    ])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        for f in src:
            assert f["properties"]["location"].startswith(basepath)
        assert len(list(src)) == 1


def test_index_geojson_for_gdal(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    run_cli(["execute", cleantopo_br.path, "-z", "3", "--debug"])

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    run_cli([
        "index", cleantopo_br.path, "-z", "3", "--geojson", "--debug",
        "--basepath", basepath, "--for-gdal"
    ])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        for f in src:
            assert f["properties"]["location"].startswith(
                "/vsicurl/" + basepath)
        assert len(list(src)) == 1


def test_index_geojson_tile(mp_tmpdir, cleantopo_tl):
    # execute process for single tile
    run_cli(["execute", cleantopo_tl.path, "-t", "3", "0", "0", "--debug"])
    # generate index
    run_cli(["index", cleantopo_tl.path, "-t", "3", "0", "0", "--geojson", "--debug"])
    with mapchete.open(cleantopo_tl.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 3
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        assert len(list(src)) == 1


def test_index_geojson_wkt_geom(mp_tmpdir, cleantopo_br, wkt_geom):
    # execute process at zoom 3
    run_cli(["execute", cleantopo_br.path, "--debug", "--wkt-geometry", wkt_geom])

    # generate index for zoom 3
    run_cli([
        "index", cleantopo_br.path, "--geojson", "--debug", "--wkt-geometry", wkt_geom
    ])

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 7
        assert "3.geojson" in files


def test_index_gpkg(mp_tmpdir, cleantopo_br):
    # execute process
    run_cli(["execute", cleantopo_br.path, "-z", "5", "--debug"])

    # generate index
    run_cli(["index", cleantopo_br.path, "-z", "5", "--gpkg", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.gpkg" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.gpkg")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    run_cli(["index", cleantopo_br.path, "-z", "5", "--gpkg", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.gpkg" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.gpkg")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_shp(mp_tmpdir, cleantopo_br):
    # execute process
    run_cli(["execute", cleantopo_br.path, "-z", "5", "--debug"])

    # generate index
    run_cli(["index", cleantopo_br.path, "-z", "5", "--shp", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.shp" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.shp")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    run_cli(["index", cleantopo_br.path, "-z", "5", "--shp", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.shp" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.shp")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_text(cleantopo_br):
    # execute process
    run_cli(["execute", cleantopo_br.path, "-z", "5", "--debug"])

    # generate index
    run_cli(["index", cleantopo_br.path, "-z", "5", "--txt", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.txt" in files
    with open(os.path.join(mp.config.output.path, "5.txt")) as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")

    # write again and assert there is no new entry because there is already one
    run_cli(["index", cleantopo_br.path, "-z", "5", "--txt", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.txt" in files
    with open(os.path.join(mp.config.output.path, "5.txt")) as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")


def test_index_errors(mp_tmpdir, cleantopo_br):
    with pytest.raises(SystemExit):
        run_cli(["index", cleantopo_br.path, "-z", "5", "--debug"])


def test_processes():
    run_cli(["processes"])
    run_cli(["processes", "-n", "mapchete.processes.examples.example_process"])


def test_callback_errors(cleantopo_tl):
    run_cli(
        ["execute", cleantopo_tl.path, "--zoom", "4,5,7"],
        expected_exit_code=2,
        raise_exc=False,
        output_contains="zooms can be maximum two items"
    )
    run_cli(
        ["execute", cleantopo_tl.path, "--zoom", "invalid"],
        expected_exit_code=2,
        raise_exc=False,
        output_contains="zoom levels must be integer values"
    )
