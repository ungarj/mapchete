#!/usr/bin/env python
"""Test custom Mapchete errors."""


import pytest
import os
import yaml
import shutil
from copy import deepcopy

import mapchete
from mapchete.config import MapcheteConfig
from mapchete.tile import BufferedTilePyramid
from mapchete import errors


def test_mapchete_init():
    """Raise TypeError if not MapcheteConfig object is passed."""
    with pytest.raises(TypeError):
        mapchete.Mapchete("wrong_type")


def test_config_modes(example_mapchete):
    """Assert process mode is handled correctly."""
    # invalid mode
    with pytest.raises(errors.MapcheteConfigError):
        MapcheteConfig(example_mapchete.path, mode="invalid")


def test_execute(example_mapchete):
    """Mapchete execute() errors."""
    # in readonly mode
    with mapchete.open(example_mapchete.path, mode="readonly") as mp:
        with pytest.raises(ValueError):
            mp.execute(mp.get_process_tiles())
    # wrong tile type
    with mapchete.open(example_mapchete.path) as mp:
        with pytest.raises(TypeError):
            mp.execute("invalid")


def test_read(example_mapchete):
    """Mapchete read() errors."""
    # in memory mode
    with mapchete.open(example_mapchete.path, mode="memory") as mp:
        with pytest.raises(ValueError):
            mp.read(mp.get_process_tiles())
    # wrong tile type
    with mapchete.open(example_mapchete.path) as mp:
        with pytest.raises(TypeError):
            mp.read("invalid")


def test_write(cleantopo_tl):
    """Test write function when passing an invalid process_tile."""
    with mapchete.open(cleantopo_tl.path) as mp:
        # process and save
        with pytest.raises(ValueError):
            mp.write("invalid tile", None)


def test_get_raw_output(example_mapchete):
    """Mapchete get_raw_output() errors."""
    with mapchete.open(example_mapchete.path) as mp:
        # wrong tile type
        with pytest.raises(TypeError):
            mp.get_raw_output("invalid")
        # not matching CRSes
        tile = BufferedTilePyramid("mercator").tile(7, 1, 1)
        with pytest.raises(NotImplementedError):
            mp.get_raw_output(tile)


def test_process_tile_write(example_mapchete):
    """Raise DeprecationWarning on MapcheteProcess.write()."""
    config = MapcheteConfig(example_mapchete.path)
    tile = BufferedTilePyramid("mercator").tile(7, 1, 1)
    process_tile = mapchete.MapcheteProcess(tile, config)
    with pytest.raises(DeprecationWarning):
        process_tile.write("data")


def test_process_tile_open(example_mapchete):
    """Raise ValueError on MapcheteProcess.open()."""
    config = MapcheteConfig(example_mapchete.path)
    tile = BufferedTilePyramid("mercator").tile(7, 1, 1)
    process_tile = mapchete.MapcheteProcess(tile, config)
    with pytest.raises(TypeError):
        process_tile.open("nonexisting_id")


def test_metatiles(example_mapchete):
    """Assert metatile sizes are checked."""
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(metatiling=1)
        config["output"].update(metatiling=2)
        MapcheteConfig(config)


def test_no_cli_input_file(example_mapchete):
    """Assert input file from command line is checked."""
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(input="from_command_line")
        MapcheteConfig(config)


def test_wrong_bounds(example_mapchete):
    """Wrong bounds number raises error."""
    with pytest.raises(errors.MapcheteConfigError):
        MapcheteConfig(example_mapchete.dict, bounds=[2, 3])


def test_empty_input_files(example_mapchete):
    """Assert empty input files raises error."""
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        del config["input"]
        MapcheteConfig(config)


def test_mandatory_params(example_mapchete):
    """Check availability of mandatory parameters."""
    for param in ["process_file", "input", "output"]:
        with pytest.raises(errors.MapcheteConfigError):
            config = deepcopy(example_mapchete.dict)
            del config[param]
            MapcheteConfig(config)
    # invalid path
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(process_file="invalid/path.py")
        MapcheteConfig(config).process_file

    # no config dir given
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.pop("config_dir")
        MapcheteConfig(config).process_file


def test_invalid_output_params(example_mapchete):
    """Check on invalid configuration."""
    # missing or invalid params
    for param in ["format", "type"]:
        config = deepcopy(example_mapchete.dict)
        with pytest.raises(errors.MapcheteConfigError):
            # invalid
            config["output"][param] = "invalid"
            MapcheteConfig(config)
        with pytest.raises(errors.MapcheteConfigError):
            # missing
            config["output"].pop(param)
            MapcheteConfig(config)


def test_invalid_zoom_levels(example_mapchete):
    """Check on invalid zoom configuration."""
    # no zoom levels given
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.pop("process_minzoom", "process_maxzoom")
        MapcheteConfig(config)
    # invalid single zoom level
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.pop("process_minzoom", "process_maxzoom")
        MapcheteConfig(config, zoom=-5)
    # invalid zoom level in pair
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.pop("process_minzoom", "process_maxzoom")
        MapcheteConfig(config, zoom=[-5, 0])
    # invalid number of zoom levels
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.pop("process_minzoom", "process_maxzoom")
        MapcheteConfig(config, zoom=[0, 5, 7])


def test_import_error(mp_tmpdir, cleantopo_br, import_error_py):
    """Assert import error is raised."""
    config = cleantopo_br.dict
    config.update(process_file=import_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(errors.MapcheteProcessImportError):
            mp.execute((5, 0, 0))


def test_malformed_process_file(cleantopo_br, malformed_py):
    """Assert import error is raised."""
    config = cleantopo_br.dict
    config.update(process_file=malformed_py)
    with mapchete.open(config) as mp:
        with pytest.raises(errors.MapcheteProcessImportError):
            mp.execute((5, 0, 0))


def test_execute_params(cleantopo_br, execute_params_error_py):
    """Assert import error is raised."""
    config = cleantopo_br.dict
    config.update(process_file=execute_params_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(errors.MapcheteProcessImportError):
            mp.execute((5, 0, 0))


def test_syntax_error(mp_tmpdir, cleantopo_br, syntax_error_py):
    """Assert syntax error is raised."""
    config = cleantopo_br.dict
    config.update(process_file=syntax_error_py)
    with pytest.raises(errors.MapcheteProcessSyntaxError):
        mapchete.open(config)


def test_process_exception(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process_file=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(errors.MapcheteProcessException):
            mp.execute((5, 0, 0))


def test_output_error(mp_tmpdir, cleantopo_br, output_error_py):
    """Assert output error is raised."""
    config = cleantopo_br.dict
    config.update(process_file=output_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(errors.MapcheteProcessOutputError):
            mp.execute((5, 0, 0))
