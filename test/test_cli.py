"""Test Mapchete main module and processing."""

from click.testing import CliRunner
import fiona
import numpy as np
import os
import pytest
from shapely import wkt
import rasterio
from rasterio.io import MemoryFile
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
    """Check if IOError is raised if input_file is invalid."""
    run_cli(
        ["execute", "process.mapchete", "--input_file", "invalid.tif"],
        expected_exit_code=2,
        output_contains='Path "process.mapchete" does not exist.',
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
            'create', temp_mapchete, temp_process, out_format,
            "--pyramid_type", "geodetic"
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
                'execute', temp_mapchete, '--tile', '6', '62', '124',
                '--input_file', cleantopo_br_tif, '-d'
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
        'create', temp_mapchete, temp_process, out_format, "--pyramid_type", "geodetic"
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
        'create', temp_mapchete, temp_process, out_format, "--pyramid_type", "geodetic"
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
            'execute', temp_mapchete, '--zoom', '5', '--input_file', cleantopo_br_tif,
            '-m', '2', '-d'
        ])
    # run example process with multiprocessing
    run_cli(['execute', cleantopo_br.path, '--zoom', '5', '-m', '2', '-d'])


def test_execute_debug(mp_tmpdir, example_mapchete):
    """Using debug output."""
    run_cli(
        [
            'execute', example_mapchete.path, "-t", "10", "500", "1040",
            "--debug"
        ]
    )


def test_execute_vrt(mp_tmpdir, cleantopo_br):
    """Using debug output."""
    run_cli(['execute', cleantopo_br.path, "-z", "5", "--vrt"])
    with mapchete.open(cleantopo_br.dict) as mp:
        vrt_path = os.path.join(mp.config.output.path, "5.vrt")
        with rasterio.open(vrt_path) as src:
            assert src.read().any()

    # run again, this time with custom output directory
    run_cli(
        ['execute', cleantopo_br.path, "-z", "5", "--vrt", "--idx_out_dir", mp_tmpdir]
    )
    with mapchete.open(cleantopo_br.dict) as mp:
        vrt_path = os.path.join(mp_tmpdir, "5.vrt")
        with rasterio.open(vrt_path) as src:
            assert src.read().any()

    # run with single tile
    run_cli(
        ['execute', cleantopo_br.path, "-t", "5", "3", "7", "--vrt"]
    )

    # no new entries
    run_cli(
        ['execute', cleantopo_br.path, "-t", "5", "0", "0", "--vrt"]
    )


def test_execute_verbose(mp_tmpdir, example_mapchete):
    """Using verbose output."""
    run_cli(['execute', example_mapchete.path, "-t", "10", "500", "1040", "--verbose"])


def test_execute_logfile(mp_tmpdir, example_mapchete):
    """Using logfile."""
    logfile = os.path.join(mp_tmpdir, "temp.log")
    run_cli([
        'execute', example_mapchete.path, "-t", "10", "500", "1040", "--logfile", logfile
    ])
    assert os.path.isfile(logfile)
    with open(logfile) as log:
        assert "DEBUG" in log.read()


def test_execute_wkt_bounds(mp_tmpdir, example_mapchete, wkt_geom):
    """Using bounds from WKT."""
    run_cli(['execute', example_mapchete.path, "--wkt_geometry", wkt_geom])


def test_execute_point(mp_tmpdir, example_mapchete, wkt_geom):
    """Using bounds from WKT."""
    g = wkt.loads(wkt_geom)
    run_cli([
        'execute', example_mapchete.path,
        "--point", str(g.centroid.x), str(g.centroid.y)
    ])


def test_formats(capfd):
    """Output of mapchete formats command."""
    run_cli(['formats'])
    err = capfd.readouterr()[1]
    assert not err
    run_cli(['formats', '-i'])
    err = capfd.readouterr()[1]
    assert not err
    run_cli(['formats', '-o'])
    err = capfd.readouterr()[1]
    assert not err


def test_pyramid_geodetic(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    run_cli(['pyramid', cleantopo_br_tif, mp_tmpdir, "-pt", "geodetic"])
    for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_pyramid_mercator(cleantopo_br_tif, mp_tmpdir):
    """Automatic mercator tile pyramid creation of raster files."""
    run_cli(['pyramid', cleantopo_br_tif, mp_tmpdir, '-d'])
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(*[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_pyramid_png(cleantopo_br_tif, mp_tmpdir):
    """Automatic PNG tile pyramid creation of raster files."""
    run_cli(['pyramid', cleantopo_br_tif, mp_tmpdir, "-of", "PNG"])
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".png"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "PNG"
            assert src.meta["dtype"] == "uint8"
            data = src.read(masked=True)
            assert data.mask.any()


def test_pyramid_minmax(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using minmax scale."""
    run_cli(['pyramid', cleantopo_br_tif, mp_tmpdir, "-s", "minmax_scale"])
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_pyramid_dtype(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using dtype scale."""
    run_cli(['pyramid', cleantopo_br_tif, mp_tmpdir, "-s", "dtype_scale"])
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_pyramid_crop(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation cropping data."""
    run_cli(['pyramid', cleantopo_br_tif, mp_tmpdir, "-s", "crop"])
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()
            assert np.all(np.where(data <= 255, True, False))


def test_pyramid_zoom(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using a specific zoom."""
    run_cli(['pyramid', cleantopo_br_tif, mp_tmpdir, "-z", "3"])
    for zoom, row, col in [(4, 15, 15), (2, 3, 0)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        assert not os.path.isfile(out_file)


def test_pyramid_zoom_minmax(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using min max zoom."""
    run_cli(['pyramid', cleantopo_br_tif, mp_tmpdir, "-z", "3,4"])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        assert not os.path.isfile(out_file)


def test_pyramid_zoom_maxmin(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using max min zoom."""
    run_cli(['pyramid', cleantopo_br_tif, mp_tmpdir, "-z", "4,3"])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        assert not os.path.isfile(out_file)


# TODO pyramid specific bounds
# TODO pyramid overwrite


def test_serve_cli_params(cleantopo_br, mp_tmpdir):
    """Test whether different CLI params pass."""
    # assert too few arguments error
    with pytest.raises(SystemExit):
        run_cli(['serve'])

    for args in [
        ['serve', cleantopo_br.path],
        ['serve', cleantopo_br.path, "--port", "5001"],
        ['serve', cleantopo_br.path, "--internal_cache", "512"],
        ['serve', cleantopo_br.path, "--zoom", "5"],
        ['serve', cleantopo_br.path, "--bounds", "-1", "-1", "1", "1"],
        ['serve', cleantopo_br.path, "--overwrite"],
        ['serve', cleantopo_br.path, "--readonly"],
        ['serve', cleantopo_br.path, "--memory"],
        ['serve', cleantopo_br.path, "--input_file", cleantopo_br.path],
    ]:
        run_cli(args)


def test_serve(client, mp_tmpdir):
    """Mapchete serve with default settings."""
    tile_base_url = '/wmts_simple/1.0.0/dem_to_hillshade/default/WGS84/'
    for url in ["/"]:
        response = client.get(url)
        assert response.status_code == 200
    for url in [
        tile_base_url+"5/30/62.png",
        tile_base_url+"5/30/63.png",
        tile_base_url+"5/31/62.png",
        tile_base_url+"5/31/63.png",
    ]:
        response = client.get(url)
        assert response.status_code == 200
        img = response.response.file
        with MemoryFile(img) as memfile:
            with memfile.open() as dataset:
                data = dataset.read()
                # get alpha band and assert not all are masked
                assert not data[3].all()
    # test outside zoom range
    response = client.get(tile_base_url+"6/31/63.png")
    assert response.status_code == 200
    img = response.response.file
    with MemoryFile(img) as memfile:
        with memfile.open() as dataset:
            data = dataset.read()
            assert not data.any()
    # test invalid url
    response = client.get(tile_base_url+"invalid_url")
    assert response.status_code == 404


def test_index_geojson(mp_tmpdir, cleantopo_br):
    # execute process at zoom 3
    run_cli(['execute', cleantopo_br.path, '-z', '3', '--debug'])

    # generate index for zoom 3
    run_cli(['index', cleantopo_br.path,  '-z', '3', '--geojson', '--debug'])
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
    run_cli(['execute', cleantopo_br.path, '-z', '3', '--debug'])

    # index and rename "location" to "new_fieldname"
    run_cli([
        'index', cleantopo_br.path,  '-z', '3', '--geojson', '--debug',
        '--fieldname', 'new_fieldname'
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
    run_cli(['execute', cleantopo_br.path, '-z', '3', '--debug'])

    basepath = 'http://localhost'
    # index and rename "location" to "new_fieldname"
    run_cli([
        'index', cleantopo_br.path,  '-z', '3', '--geojson', '--debug',
        '--basepath', basepath
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
    run_cli(['execute', cleantopo_br.path, '-z', '3', '--debug'])

    basepath = 'http://localhost'
    # index and rename "location" to "new_fieldname"
    run_cli([
        'index', cleantopo_br.path,  '-z', '3', '--geojson', '--debug',
        '--basepath', basepath, '--for_gdal'
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
    run_cli(['execute', cleantopo_tl.path, '-t', '3', '0', '0', '--debug'])
    # generate index
    run_cli(['index', cleantopo_tl.path, '-t', '3', '0', '0', '--geojson', '--debug'])
    with mapchete.open(cleantopo_tl.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 3
        assert "3.geojson" in files
    with fiona.open(os.path.join(mp.config.output.path, "3.geojson")) as src:
        assert len(list(src)) == 1


def test_index_geojson_wkt_geom(mp_tmpdir, cleantopo_br, wkt_geom):
    # execute process at zoom 3
    run_cli(['execute', cleantopo_br.path, '--debug', "--wkt_geometry", wkt_geom])

    # generate index for zoom 3
    run_cli([
        'index', cleantopo_br.path, '--geojson', '--debug', "--wkt_geometry", wkt_geom
    ])

    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert len(files) == 7
        assert "3.geojson" in files


def test_index_gpkg(mp_tmpdir, cleantopo_br):
    # execute process
    run_cli(['execute', cleantopo_br.path, '-z', '5', '--debug'])

    # generate index
    run_cli(['index', cleantopo_br.path,  '-z', '5', '--gpkg', '--debug'])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.gpkg" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.gpkg")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    run_cli(['index', cleantopo_br.path,  '-z', '5', '--gpkg', '--debug'])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.gpkg" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.gpkg")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_shp(mp_tmpdir, cleantopo_br):
    # execute process
    run_cli(['execute', cleantopo_br.path, '-z', '5', '--debug'])

    # generate index
    run_cli(['index', cleantopo_br.path,  '-z', '5', '--shp', '--debug'])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.shp" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.shp")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    run_cli(['index', cleantopo_br.path,  '-z', '5', '--shp', '--debug'])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.shp" in files
    with fiona.open(os.path.join(mp.config.output.path, "5.shp")) as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_text(cleantopo_br):
    # execute process
    run_cli(['execute', cleantopo_br.path, '-z', '5', '--debug'])

    # generate index
    run_cli(['index', cleantopo_br.path,  '-z', '5', '--txt', '--debug'])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = os.listdir(mp.config.output.path)
        assert "5.txt" in files
    with open(os.path.join(mp.config.output.path, "5.txt")) as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")

    # write again and assert there is no new entry because there is already one
    run_cli(['index', cleantopo_br.path,  '-z', '5', '--txt', '--debug'])
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
        run_cli(['index', cleantopo_br.path,  '-z', '5', '--debug'])


def test_processes():
    run_cli(['processes'])
