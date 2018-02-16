#!/usr/bin/env python
"""Test Mapchete main module and processing."""

import pytest
import os
import subprocess
import yaml
import rasterio
from rasterio.io import MemoryFile
import numpy as np

from mapchete.cli.main import MapcheteCLI
from mapchete.errors import MapcheteProcessOutputError


def _getstatusoutput(command):
    sp = subprocess.Popen(
        command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    sp.wait()
    return sp.returncode, sp.stdout.read()


def test_main():
    """Main CLI."""
    for command in [
            "mapchete create", "mapchete execute", "mapchete serve"
    ]:
        status, output = _getstatusoutput(command)
        assert status == 2
        assert any([
            err in output
            for err in [
                "the following arguments are required",  # python 3
                "too few arguments"  # python 2
            ]
        ])

    status = _getstatusoutput("mapchete formats")[0]
    assert status == 0

    status, output = _getstatusoutput("mapchete wrong_command")
    assert status == 2
    assert "unrecognized command" in output


def test_missing_input_file():
    """Check if IOError is raised if input_file is invalid."""
    status, output = _getstatusoutput(
        "mapchete execute process.mapchete --input_file invalid.tif"
    )
    assert status == 1
    assert "IOError: input_file invalid.tif not found"


def test_create_and_execute(mp_tmpdir, cleantopo_br_tif):
    """Run mapchete create and execute."""
    temp_mapchete = os.path.join(mp_tmpdir, "temp.mapchete")
    temp_process = os.path.join(mp_tmpdir, "temp.py")
    out_format = "GTiff"
    # create from template
    args = [
        None, 'create', temp_mapchete, temp_process, out_format,
        "--pyramid_type", "geodetic"]
    MapcheteCLI(args)
    # edit configuration
    with open(temp_mapchete, "r") as config_file:
        config = yaml.load(config_file)
        config["output"].update(bands=1, dtype="uint8", path=".")
    with open(temp_mapchete, "w") as config_file:
        config_file.write(yaml.dump(config, default_flow_style=False))
    # run process for single tile
    args = [
        None, 'execute', temp_mapchete, '--tile', '6', '62', '124',
        '--input_file', cleantopo_br_tif]
    with pytest.raises(MapcheteProcessOutputError):
        MapcheteCLI(args)


def test_create_existing(mp_tmpdir):
    """Run mapchete create and execute."""
    temp_mapchete = os.path.join(mp_tmpdir, "temp.mapchete")
    temp_process = os.path.join(mp_tmpdir, "temp.py")
    out_format = "GTiff"
    # create files from template
    args = [
        None, 'create', temp_mapchete, temp_process, out_format,
        "--pyramid_type", "geodetic"]
    MapcheteCLI(args)
    # try to create again
    with pytest.raises(IOError):
        MapcheteCLI(args)


def test_execute_multiprocessing(mp_tmpdir, cleantopo_br, cleantopo_br_tif):
    """Run mapchete execute with multiple workers."""
    temp_mapchete = os.path.join(mp_tmpdir, "temp.mapchete")
    temp_process = os.path.join(mp_tmpdir, "temp.py")
    out_format = "GTiff"
    # create from template
    args = [
        None, 'create', temp_mapchete, temp_process, out_format,
        "--pyramid_type", "geodetic"]
    MapcheteCLI(args)
    # edit configuration
    with open(temp_mapchete, "r") as config_file:
        config = yaml.load(config_file)
        config["output"].update(bands=1, dtype="uint8", path=".")
    with open(temp_mapchete, "w") as config_file:
        config_file.write(yaml.dump(config, default_flow_style=False))
    # run process with multiprocessing
    args = [
        None, 'execute', temp_mapchete, '--zoom', '5',
        '--input_file', cleantopo_br_tif]
    with pytest.raises(MapcheteProcessOutputError):
        MapcheteCLI(args)
    # run example process with multiprocessing
    args = [None, 'execute', cleantopo_br.path, '--zoom', '5']
    MapcheteCLI(args)


def test_execute_debug(mp_tmpdir, example_mapchete):
    """Using debug output."""
    args = [
        None, 'execute', example_mapchete.path, "-t", "10", "500", "1040",
        "--debug"]
    MapcheteCLI(args)


def test_execute_verbose(mp_tmpdir, example_mapchete):
    """Using verbose output."""
    args = [
        None, 'execute', example_mapchete.path, "-t", "10", "500", "1040",
        "--verbose"]
    MapcheteCLI(args)


def test_execute_logfile(mp_tmpdir, example_mapchete):
    """Using logfile."""
    logfile = os.path.join(mp_tmpdir, "temp.log")
    args = [
        None, 'execute', example_mapchete.path, "-t", "10", "500", "1040",
        "--logfile", logfile]
    MapcheteCLI(args)
    assert os.path.isfile(logfile)
    with open(logfile) as log:
        assert "DEBUG" in log.read()


def test_formats(capfd):
    """Output of mapchete formats command."""
    MapcheteCLI([None, 'formats'])
    err = capfd.readouterr()[1]
    assert not err
    MapcheteCLI([None, 'formats', '-i'])
    err = capfd.readouterr()[1]
    assert not err
    MapcheteCLI([None, 'formats', '-o'])
    err = capfd.readouterr()[1]
    assert not err


def test_pyramid_geodetic(cleantopo_br_tif, mp_tmpdir):
    """Automatic geodetic tile pyramid creation of raster files."""
    MapcheteCLI([
        None, 'pyramid', cleantopo_br_tif, mp_tmpdir, "-pt", "geodetic"])
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
    MapcheteCLI([None, 'pyramid', cleantopo_br_tif, mp_tmpdir])
    for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        with rasterio.open(out_file, "r") as src:
            assert src.meta["driver"] == "GTiff"
            assert src.meta["dtype"] == "uint16"
            data = src.read(masked=True)
            assert data.mask.any()


def test_pyramid_png(cleantopo_br_tif, mp_tmpdir):
    """Automatic PNG tile pyramid creation of raster files."""
    MapcheteCLI([None, 'pyramid', cleantopo_br_tif, mp_tmpdir, "-of", "PNG"])
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
    MapcheteCLI([
        None, 'pyramid', cleantopo_br_tif, mp_tmpdir, "-s", "minmax_scale"])
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
    MapcheteCLI([
        None, 'pyramid', cleantopo_br_tif, mp_tmpdir, "-s", "dtype_scale"])
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
    MapcheteCLI([
        None, 'pyramid', cleantopo_br_tif, mp_tmpdir, "-s", "crop"])
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
    MapcheteCLI([
        None, 'pyramid', cleantopo_br_tif, mp_tmpdir, "-z", "3"])
    for zoom, row, col in [(4, 15, 15), (2, 3, 0)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        assert not os.path.isfile(out_file)


def test_pyramid_zoom_minmax(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using min max zoom."""
    MapcheteCLI([
        None, 'pyramid', cleantopo_br_tif, mp_tmpdir, "-z", "3", "4"])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        assert not os.path.isfile(out_file)


def test_pyramid_zoom_maxmin(cleantopo_br_tif, mp_tmpdir):
    """Automatic tile pyramid creation using max min zoom."""
    MapcheteCLI([
        None, 'pyramid', cleantopo_br_tif, mp_tmpdir, "-z", "4", "3"])
    for zoom, row, col in [(2, 3, 0)]:
        out_file = os.path.join(
            *[mp_tmpdir, str(zoom), str(row), str(col)+".tif"])
        assert not os.path.isfile(out_file)


# TODO pyramid specific bounds
# TODO pyramid overwrite


def test_serve_cli_params(cleantopo_br):
    """Test whether different CLI params pass."""
    # assert too few arguments error
    with pytest.raises(SystemExit):
        MapcheteCLI([None, 'serve'], _test_serve=True)

    for args in [
        [None, 'serve', cleantopo_br.path],
        [None, 'serve', cleantopo_br.path, "--port", "5001"],
        [None, 'serve', cleantopo_br.path, "--internal_cache", "512"],
        [None, 'serve', cleantopo_br.path, "--zoom", "5"],
        [None, 'serve', cleantopo_br.path, "--bounds", "-1", "-1", "1", "1"],
        [None, 'serve', cleantopo_br.path, "--overwrite"],
        [None, 'serve', cleantopo_br.path, "--readonly"],
        [None, 'serve', cleantopo_br.path, "--memory"],
        [
            None, 'serve', cleantopo_br.path, "--input_file",
            cleantopo_br.path],
    ]:
        MapcheteCLI(args, _test_serve=True)


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
