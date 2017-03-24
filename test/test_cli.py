#!/usr/bin/env python
"""Test Mapchete main module and processing."""

import os
import yaml
import shutil

from mapchete.cli.main import MapcheteCLI

scriptdir = os.path.dirname(os.path.realpath(__file__))
out_dir = os.path.join(scriptdir, "testdata/tmp")


def test_execute():
    """Run mapchete execute."""
    temp_mapchete = "temp.mapchete"
    temp_process = "temp.py"
    out_format = "GTiff"
    out_dir = os.path.join(scriptdir, "testdata/tmp")
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
    except:
        raise
    finally:
        delete_files = [temp_mapchete, temp_process, "temp.pyc", "temp.log"]
        for delete_file in delete_files:
            try:
                os.remove(delete_file)
            except:
                pass
        try:
            shutil.rmtree(out_dir)
        except:
            pass


def test_execute_multiprocessing():
    """Run mapchete execute with multiple workers."""
    temp_mapchete = "temp.mapchete"
    temp_process = "temp.py"
    out_format = "GTiff"
    out_dir = os.path.join(scriptdir, "testdata/tmp")
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
    except:
        raise
    finally:
        delete_files = [temp_mapchete, temp_process, "temp.pyc", "temp.log"]
        for delete_file in delete_files:
            try:
                os.remove(delete_file)
            except:
                pass
        try:
            shutil.rmtree(out_dir)
        except:
            pass


# TODO mapchete serve
# TODO mapchete pyramid
