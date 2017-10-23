#!/usr/bin/env python
"""Test custom Mapchete errors."""


import pytest
import os
import yaml
import shutil
from copy import copy

import mapchete
from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTilePyramid
from mapchete import errors


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
OUT_DIR = os.path.join(SCRIPTDIR, "testdata/tmp")


def test_mapchete_init():
    """Raise TypeError if not MapcheteConfig object is passed."""
    with pytest.raises(TypeError):
        mapchete.Mapchete("wrong_type")


def test_config_modes():
    """Assert process mode is handled correctly."""
    # invalid mode
    with pytest.raises(errors.MapcheteConfigError):
        MapcheteConfig(
            os.path.join(SCRIPTDIR, "example.mapchete"), mode="invalid")


def test_execute():
    """Mapchete execute() errors."""
    # in readonly mode
    with mapchete.open(
        os.path.join(SCRIPTDIR, "example.mapchete"), mode="readonly"
    ) as mp:
        with pytest.raises(ValueError):
            mp.execute(mp.get_process_tiles())
    # wrong tile type
    with mapchete.open(os.path.join(SCRIPTDIR, "example.mapchete")) as mp:
        with pytest.raises(TypeError):
            mp.execute("invalid")


def test_read():
    """Mapchete read() errors."""
    # in memory mode
    with mapchete.open(
        os.path.join(SCRIPTDIR, "example.mapchete"), mode="memory"
    ) as mp:
        with pytest.raises(ValueError):
            mp.read(mp.get_process_tiles())
    # wrong tile type
    with mapchete.open(os.path.join(SCRIPTDIR, "example.mapchete")) as mp:
        with pytest.raises(TypeError):
            mp.read("invalid")


def test_get_raw_output():
    """Mapchete get_raw_output() errors."""
    with mapchete.open(os.path.join(SCRIPTDIR, "example.mapchete")) as mp:
        # wrong tile type
        with pytest.raises(TypeError):
            mp.get_raw_output("invalid")
        # not matching CRSes
        tile = BufferedTilePyramid("mercator").tile(7, 1, 1)
        with pytest.raises(NotImplementedError):
            mp.get_raw_output(tile)


def test_process_tile_write():
    """Raise DeprecationWarning on MapcheteProcess.write()."""
    config = MapcheteConfig(os.path.join(SCRIPTDIR, "example.mapchete"))
    tile = BufferedTilePyramid("mercator").tile(7, 1, 1)
    process_tile = mapchete.MapcheteProcess(tile, config)
    with pytest.raises(DeprecationWarning):
        process_tile.write("data")


def test_process_tile_open():
    """Raise ValueError on MapcheteProcess.open()."""
    config = MapcheteConfig(os.path.join(SCRIPTDIR, "example.mapchete"))
    tile = BufferedTilePyramid("mercator").tile(7, 1, 1)
    process_tile = mapchete.MapcheteProcess(tile, config)
    with pytest.raises(TypeError):
        process_tile.open("nonexisting_id")


def test_metatiles():
    """Assert metatile sizes are checked."""
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(
                config_dir=SCRIPTDIR, metatiling=1)
            config["output"].update(metatiling=2)
            MapcheteConfig(config)


def test_no_cli_input_file():
    """Assert input file from command line is checked."""
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(
                config_dir=SCRIPTDIR, input="from_command_line")
            MapcheteConfig(config)


def test_wrong_bounds():
    """Wrong bounds number raises error."""
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(
                config_dir=SCRIPTDIR)
            MapcheteConfig(config, bounds=[2, 3])


def test_empty_input_files():
    """Assert empty input files raises error."""
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["input"]
            MapcheteConfig(config)


def test_mandatory_params():
    """Check availability of mandatory parameters."""
    for param in ["process_file", "input", "output"]:
        with pytest.raises(errors.MapcheteConfigError):
            with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
                config = copy(yaml.load(mc))
                del config[param]
                config.update(config_dir=SCRIPTDIR)
                MapcheteConfig(config)
    # invalid path
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR, process_file="invalid/path.py")
            MapcheteConfig(config).process_file

    # no config dir given
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            MapcheteConfig(config).process_file


def test_invalid_output_params():
    """Check on invalid configuration."""
    # missing or invalid params
    for param in ["format", "type"]:
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            with pytest.raises(errors.MapcheteConfigError):
                # invalid
                config["output"][param] = "invalid"
                MapcheteConfig(config)
            with pytest.raises(errors.MapcheteConfigError):
                # missing
                del config["output"][param]
                MapcheteConfig(config)


def test_invalid_zoom_levels():
    """Check on invalid zoom configuration."""
    # no zoom levels given
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["process_minzoom"]
            del config["process_maxzoom"]
            MapcheteConfig(config)
    # invalid single zoom level
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["process_minzoom"]
            del config["process_maxzoom"]
            MapcheteConfig(config, zoom=-5)
    # invalid zoom level in pair
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["process_minzoom"]
            del config["process_maxzoom"]
            MapcheteConfig(config, zoom=[-5, 0])
    # invalid number of zoom levels
    with pytest.raises(errors.MapcheteConfigError):
        with open(os.path.join(SCRIPTDIR, "example.mapchete")) as mc:
            config = copy(yaml.load(mc))
            config.update(config_dir=SCRIPTDIR)
            del config["process_minzoom"]
            del config["process_maxzoom"]
            MapcheteConfig(config, zoom=[0, 5, 7])


def test_import_error():
    """Assert import error is raised."""
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mc:
            config = yaml.load(mc)
        config.update(
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_file=os.path.join(SCRIPTDIR, "testdata/import_error.py"))
        mp = mapchete.open(config)
        with pytest.raises(errors.MapcheteProcessImportError):
            mp.execute((5, 0, 0))
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_malformed_process_file():
    """Assert import error is raised."""
    config = yaml.load(open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
    ).read())
    config.update(
        config_dir=os.path.join(SCRIPTDIR, "testdata"),
        process_file=os.path.join(SCRIPTDIR, "testdata/malformed.py")
    )
    with mapchete.open(config) as mp:
        with pytest.raises(errors.MapcheteProcessImportError):
            mp.execute((5, 0, 0))


def test_execute_params():
    """Assert import error is raised."""
    config = yaml.load(open(
        os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
    ).read())
    config.update(
        config_dir=os.path.join(SCRIPTDIR, "testdata"),
        process_file=os.path.join(SCRIPTDIR, "testdata/execute_params_error.py")
    )
    with mapchete.open(config) as mp:
        with pytest.raises(errors.MapcheteProcessImportError):
            mp.execute((5, 0, 0))


def test_syntax_error():
    """Assert syntax error is raised."""
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mc:
            config = yaml.load(mc)
        config.update(
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_file=os.path.join(SCRIPTDIR, "testdata/syntax_error.py"))
        with pytest.raises(errors.MapcheteProcessSyntaxError):
            mapchete.open(config)
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_process_exception():
    """Assert process exception is raised."""
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mc:
            config = yaml.load(mc)
        config.update(
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_file=os.path.join(SCRIPTDIR, "testdata/process_error.py"))
        mp = mapchete.open(config)
        with pytest.raises(errors.MapcheteProcessException):
            mp.execute((5, 0, 0))
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_output_error():
    """Assert output error is raised."""
    try:
        with open(
            os.path.join(SCRIPTDIR, "testdata/cleantopo_br.mapchete")
        ) as mc:
            config = yaml.load(mc)
        config.update(
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_file=os.path.join(SCRIPTDIR, "testdata/output_error.py"))
        mp = mapchete.open(config)
        with pytest.raises(errors.MapcheteProcessOutputError):
            mp.execute((5, 0, 0))
    finally:
        shutil.rmtree(OUT_DIR, ignore_errors=True)
