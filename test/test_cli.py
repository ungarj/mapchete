#!/usr/bin/env python
"""Test Mapchete main module and processing."""

import os
import shutil
import commands
import yaml
import rasterio
import numpy as np
from PIL import Image

from mapchete.cli.main import MapcheteCLI
from mapchete.errors import MapcheteProcessOutputError

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")


def test_main():
    """Main CLI."""
    for command in [
            "mapchete", "mapchete create", "mapchete execute", "mapchete serve"
    ]:
        status = commands.getstatusoutput(command)[0]
        assert status == 512

    status = commands.getstatusoutput("mapchete formats")[0]
    assert status == 0

    status = commands.getstatusoutput("mapchete wrong_command")[0]
    assert status == 256


def test_create_and_execute():
    """Run mapchete create and execute."""
    temp_mapchete = "temp.mapchete"
    temp_process = "temp.py"
    out_format = "GTiff"
    try:
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
        input_file = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.tif")
        args = [
            None, 'execute', temp_mapchete, '--tile', '6', '62', '124',
            '--input_file', input_file]
        try:
            MapcheteCLI(args)
        except MapcheteProcessOutputError:
            pass
    finally:
        delete_files = [temp_mapchete, temp_process, "temp.pyc", "temp.log"]
        for delete_file in delete_files:
            try:
                os.remove(delete_file)
            except OSError:
                pass
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_create_existing():
    """Run mapchete create and execute."""
    temp_mapchete = "temp.mapchete"
    temp_process = "temp.py"
    out_format = "GTiff"
    # create files from template
    args = [
        None, 'create', temp_mapchete, temp_process, out_format,
        "--pyramid_type", "geodetic"]
    MapcheteCLI(args)
    # try to create again
    try:
        MapcheteCLI(args)
    except IOError:
        pass
    finally:
        delete_files = [temp_mapchete, temp_process]
        for delete_file in delete_files:
            try:
                os.remove(delete_file)
            except OSError:
                pass
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_execute_multiprocessing():
    """Run mapchete execute with multiple workers."""
    temp_mapchete = "temp.mapchete"
    temp_process = "temp.py"
    out_format = "GTiff"
    try:
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
        input_file = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.tif")
        args = [
            None, 'execute', temp_mapchete, '--zoom', '6',
            '--input_file', input_file]
        try:
            MapcheteCLI(args)
        except MapcheteProcessOutputError:
            pass
        # run example process with multiprocessing
        args = [
            None, 'execute', os.path.join(
                SCRIPTDIR, "testdata/cleantopo_br.mapchete"),
            '--zoom', '8'
        ]
        MapcheteCLI(args)
    finally:
        delete_files = [temp_mapchete, temp_process, "temp.pyc", "temp.log"]
        for delete_file in delete_files:
            try:
                os.remove(delete_file)
            except OSError:
                pass
        shutil.rmtree(OUT_DIR, ignore_errors=True)


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


def test_pyramid_geodetic():
    """Automatic geodetic tile pyramid creation of raster files."""
    test_raster = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, OUT_DIR, "-pt", "geodetic"])
        for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
            out_file = os.path.join(
                *[OUT_DIR, str(zoom), str(row), str(col)+".tif"])
            with rasterio.open(out_file, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_pyramid_mercator():
    """Automatic mercator tile pyramid creation of raster files."""
    test_raster = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([None, 'pyramid', test_raster, OUT_DIR])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            out_file = os.path.join(
                *[OUT_DIR, str(zoom), str(row), str(col)+".tif"])
            with rasterio.open(out_file, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_pyramid_png():
    """Automatic PNG tile pyramid creation of raster files."""
    test_raster = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([None, 'pyramid', test_raster, OUT_DIR, "-of", "PNG"])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            out_file = os.path.join(
                *[OUT_DIR, str(zoom), str(row), str(col)+".png"])
            with rasterio.open(out_file, "r") as src:
                assert src.meta["driver"] == "PNG"
                assert src.meta["dtype"] == "uint8"
                data = src.read(masked=True)
                assert data.mask.any()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_pyramid_minmax():
    """Automatic tile pyramid creation using minmax scale."""
    test_raster = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, OUT_DIR, "-s", "minmax_scale"])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            out_file = os.path.join(
                *[OUT_DIR, str(zoom), str(row), str(col)+".tif"])
            with rasterio.open(out_file, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_pyramid_dtype():
    """Automatic tile pyramid creation using dtype scale."""
    test_raster = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, OUT_DIR, "-s", "dtype_scale"])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            out_file = os.path.join(
                *[OUT_DIR, str(zoom), str(row), str(col)+".tif"])
            with rasterio.open(out_file, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_pyramid_crop():
    """Automatic tile pyramid creation cropping data."""
    test_raster = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, OUT_DIR, "-s", "crop"])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            out_file = os.path.join(
                *[OUT_DIR, str(zoom), str(row), str(col)+".tif"])
            with rasterio.open(out_file, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
                assert np.all(np.where(data <= 255, True, False))
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_pyramid_zoom():
    """Automatic tile pyramid creation using a specific zoom."""
    test_raster = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, OUT_DIR, "-z", "3"])
        for zoom, row, col in [(4, 15, 15), (2, 3, 0)]:
            out_file = os.path.join(
                *[OUT_DIR, str(zoom), str(row), str(col)+".tif"])
            assert not os.path.isfile(out_file)
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)

    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, OUT_DIR, "-z", "3", "4"])
        for zoom, row, col in [(2, 3, 0)]:
            out_file = os.path.join(
                *[OUT_DIR, str(zoom), str(row), str(col)+".tif"])
            assert not os.path.isfile(out_file)
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)

    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, OUT_DIR, "-z", "4", "3"])
        for zoom, row, col in [(2, 3, 0)]:
            out_file = os.path.join(
                *[OUT_DIR, str(zoom), str(row), str(col)+".tif"])
            assert not os.path.isfile(out_file)
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


# TODO pyramid specific bounds
# TODO pyramid overwrite


def test_serve_cli_params():
    """Test whether different CLI params pass."""
    # assert too few arguments error
    try:
        MapcheteCLI([None, 'serve'], _test_serve=True)
    except SystemExit as exit_code:
        assert exit_code.message == 2

    example_process = os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
    for args in [
            [None, 'serve', example_process],
            [None, 'serve', example_process, "--port", "5001"],
            [None, 'serve', example_process, "--internal_cache", "512"],
            [None, 'serve', example_process, "--zoom", "5"],
            [None, 'serve', example_process, "--bounds", "-1", "-1", "1", "1"],
            [None, 'serve', example_process, "--overwrite"],
            [None, 'serve', example_process, "--readonly"],
            [None, 'serve', example_process, "--memory"],
            [None, 'serve', example_process, "--input_file", example_process],
    ]:
        MapcheteCLI(args, _test_serve=True)


def test_serve(client):
    """Mapchete serve with default settings."""
    tile_base_url = '/wmts_simple/1.0.0/mapchete/default/WGS84/'
    try:
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
            img.seek(0)
            data = np.array(Image.open(img)).transpose(2, 0, 1)
            # get alpha band and assert not all are masked
            assert not data[3].all()
        # test outside zoom range
        response = client.get(tile_base_url+"6/31/63.png")
        assert response.status_code == 200
        img = response.response.file
        img.seek(0)
        data = np.array(Image.open(img)).transpose(2, 0, 1)
        # all three bands have to be 0
        assert not data[0].any()
        assert not data[1].any()
        assert not data[2].any()
        # alpha band has to be filled with 0
        assert not data[3].any()
        # test invalid url
        response = client.get(tile_base_url+"invalid_url")
        assert response.status_code == 404
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)
