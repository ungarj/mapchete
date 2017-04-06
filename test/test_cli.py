#!/usr/bin/env python
"""Test Mapchete main module and processing."""

import os
import yaml
import shutil
import commands
import rasterio
import numpy as np


from mapchete.cli.main import MapcheteCLI

scriptdir = os.path.dirname(os.path.realpath(__file__))
out_dir = os.path.join(scriptdir, "testdata/tmp")


def test_main():
    """Main CLI."""
    for command in [
        "mapchete", "mapchete create", "mapchete execute",
        "mapchete serve"
    ]:
        status, output = commands.getstatusoutput(command)
        assert status == 512

    status, output = commands.getstatusoutput("mapchete formats")
    assert status == 0

    status, output = commands.getstatusoutput("mapchete wrong_command")
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
        input_file = os.path.join(scriptdir, "testdata/cleantopo_br.tif")
        args = [
            None, 'execute', temp_mapchete, '--tile', '6', '62', '124',
            '--input_file', input_file]
        try:
            MapcheteCLI(args)
        except RuntimeError:
            pass
    except Exception:
        raise
    finally:
        delete_files = [temp_mapchete, temp_process, "temp.pyc", "temp.log"]
        for delete_file in delete_files:
            try:
                os.remove(delete_file)
            except OSError:
                pass
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


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
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


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
        input_file = os.path.join(scriptdir, "testdata/cleantopo_br.tif")
        args = [
            None, 'execute', temp_mapchete, '--zoom', '6',
            '--input_file', input_file]
        try:
            MapcheteCLI(args)
        except RuntimeError:
            pass
        # run example process with multiprocessing
        args = [
            None, 'execute', os.path.join(
                scriptdir, "testdata/cleantopo_br.mapchete"),
            '--zoom', '8'
        ]
        MapcheteCLI(args)
    except Exception:
        raise
    finally:
        delete_files = [temp_mapchete, temp_process, "temp.pyc", "temp.log"]
        for delete_file in delete_files:
            try:
                os.remove(delete_file)
            except OSError:
                pass
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


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
    test_raster = os.path.join(scriptdir, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, out_dir, "-pt", "geodetic"])
        for zoom, row, col in [(4, 15, 31), (3, 7, 15), (2, 3, 7), (1, 1, 3)]:
            f = os.path.join(
                os.path.join(
                    os.path.join(
                        out_dir, str(zoom)), str(row)), str(col)+".tif")
            with rasterio.open(f, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


def test_pyramid_mercator():
    """Automatic mercator tile pyramid creation of raster files."""
    test_raster = os.path.join(scriptdir, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([None, 'pyramid', test_raster, out_dir])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            f = os.path.join(
                os.path.join(
                    os.path.join(
                        out_dir, str(zoom)), str(row)), str(col)+".tif")
            with rasterio.open(f, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


def test_pyramid_png():
    """Automatic PNG tile pyramid creation of raster files."""
    test_raster = os.path.join(scriptdir, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([None, 'pyramid', test_raster, out_dir, "-of", "PNG"])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            f = os.path.join(
                os.path.join(
                    os.path.join(
                        out_dir, str(zoom)), str(row)), str(col)+".png")
            with rasterio.open(f, "r") as src:
                assert src.meta["driver"] == "PNG"
                assert src.meta["dtype"] == "uint8"
                data = src.read(masked=True)
                assert data.mask.any()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


def test_pyramid_minmax():
    """Automatic tile pyramid creation using minmax scale."""
    test_raster = os.path.join(scriptdir, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, out_dir, "-s", "minmax_scale"])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            f = os.path.join(
                os.path.join(
                    os.path.join(
                        out_dir, str(zoom)), str(row)), str(col)+".tif")
            with rasterio.open(f, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


def test_pyramid_dtype():
    """Automatic tile pyramid creation using dtype scale."""
    test_raster = os.path.join(scriptdir, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, out_dir, "-s", "dtype_scale"])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            f = os.path.join(
                os.path.join(
                    os.path.join(
                        out_dir, str(zoom)), str(row)), str(col)+".tif")
            with rasterio.open(f, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


def test_pyramid_crop():
    """Automatic tile pyramid creation cropping data."""
    test_raster = os.path.join(scriptdir, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, out_dir, "-s", "crop"])
        for zoom, row, col in [(4, 15, 15), (3, 7, 7)]:
            f = os.path.join(
                os.path.join(
                    os.path.join(
                        out_dir, str(zoom)), str(row)), str(col)+".tif")
            with rasterio.open(f, "r") as src:
                assert src.meta["driver"] == "GTiff"
                assert src.meta["dtype"] == "uint16"
                data = src.read(masked=True)
                assert data.mask.any()
                assert np.all(np.where(data <= 255, True, False))
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


def test_pyramid_zoom():
    """Automatic tile pyramid creation using a specific zoom."""
    test_raster = os.path.join(scriptdir, "testdata/cleantopo_br.tif")
    try:
        MapcheteCLI([
            None, 'pyramid', test_raster, out_dir, "-z", "3"])
        for zoom, row, col in [(4, 15, 15), (2, 3, 0)]:
            f = os.path.join(
                os.path.join(
                    os.path.join(
                        out_dir, str(zoom)), str(row)), str(col)+".tif")
            assert not os.path.isfile(f)
    except Exception:
        raise
    finally:
        try:
            shutil.rmtree(out_dir)
        except OSError:
            pass


# TODO pyramid specific bounds
# TODO pyramid overwrite
# TODO mapchete serve
