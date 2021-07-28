#!/usr/bin/env python
"""Test custom Mapchete errors."""


import pytest
from copy import deepcopy

import mapchete
from mapchete.config import MapcheteConfig, validate_values
from mapchete._processing import Executor
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
            mp.execute(next(mp.get_process_tiles()))
    # wrong tile type
    with mapchete.open(example_mapchete.path) as mp:
        with pytest.raises(TypeError):
            mp.execute("invalid")


def test_read(example_mapchete):
    """Mapchete read() errors."""
    # in memory mode
    with mapchete.open(example_mapchete.path, mode="memory") as mp:
        with pytest.raises(ValueError):
            mp.read(next(mp.get_process_tiles()))
    # wrong tile type
    with mapchete.open(example_mapchete.path) as mp:
        with pytest.raises(TypeError):
            mp.read("invalid")


def test_write(cleantopo_tl):
    """Test write function when passing an invalid process_tile."""
    with mapchete.open(cleantopo_tl.path) as mp:
        # process and save
        with pytest.raises(TypeError):
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
    user_process = mapchete.MapcheteProcess(
        tile=tile,
        params=config.params_at_zoom(tile.zoom),
        input=config.get_inputs_for_tile(tile),
    )
    with pytest.raises(DeprecationWarning):
        user_process.write("data")


def test_process_tile_open(example_mapchete):
    """Raise ValueError on MapcheteProcess.open()."""
    config = MapcheteConfig(example_mapchete.path)
    tile = BufferedTilePyramid("mercator").tile(7, 1, 1)
    user_process = mapchete.MapcheteProcess(
        tile=tile,
        params=config.params_at_zoom(tile.zoom),
        input=config.get_inputs_for_tile(tile),
    )
    with pytest.raises(ValueError):
        user_process.open("nonexisting_id")


def test_process_tile_read(example_mapchete):
    """Raise ValueError on MapcheteProcess.open()."""
    config = MapcheteConfig(example_mapchete.path)
    tile = BufferedTilePyramid("mercator").tile(7, 1, 1)
    user_process = mapchete.MapcheteProcess(
        tile=tile,
        params=config.params_at_zoom(tile.zoom),
        input=config.get_inputs_for_tile(tile),
    )
    with pytest.raises(DeprecationWarning):
        user_process.read()


def test_metatiles(example_mapchete):
    """Assert metatile sizes are checked."""
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config["pyramid"].update(metatiling=1)
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
    with pytest.raises(errors.MapcheteConfigError):
        MapcheteConfig(dict(example_mapchete.dict, bounds=[2, 3]))


def test_empty_input_files(example_mapchete):
    """Assert empty input files raises error."""
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        del config["input"]
        MapcheteConfig(config)


def test_mandatory_params(example_mapchete):
    """Check availability of mandatory parameters."""
    for param in ["process", "input", "output"]:
        with pytest.raises(errors.MapcheteConfigError):
            config = deepcopy(example_mapchete.dict)
            del config[param]
            MapcheteConfig(config)
    # invalid path
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(process="invalid/path.py")
        MapcheteConfig(config).process

    # no config dir given
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.pop("config_dir")
        MapcheteConfig(config).process


def test_invalid_output_params(example_mapchete):
    """Check on invalid configuration."""
    # missing or invalid params
    for param in ["format"]:
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
    # process zooms
    # no zoom levels given
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.pop("zoom_levels")
        with pytest.deprecated_call():
            MapcheteConfig(config)
    # invalid single zoom level
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(zoom_levels=-5)
        MapcheteConfig(config)
    # invalid zoom level in pair
    with pytest.raises(errors.MapcheteConfigError):
        config = deepcopy(example_mapchete.dict)
        config.update(zoom_levels=[-5, 0])
        MapcheteConfig(config)
    # min or max missing
    config = deepcopy(example_mapchete.dict)
    config.update(zoom_levels=dict(min=0))
    with pytest.raises(errors.MapcheteConfigError):
        MapcheteConfig(config)
    config.update(zoom_levels=dict(max=5))
    with pytest.raises(errors.MapcheteConfigError):
        MapcheteConfig(config)
    # min bigger than max
    config = deepcopy(example_mapchete.dict)
    config.update(zoom_levels=dict(min=5, max=0))

    # init zooms
    # invalid single zoom level
    with pytest.raises(errors.MapcheteConfigError):
        MapcheteConfig(config, zoom=-5)
    # invalid zoom level in pair
    with pytest.raises(errors.MapcheteConfigError):
        MapcheteConfig(config, zoom=[-5, 0])
    # not a subset
    with pytest.raises(errors.MapcheteConfigError):
        MapcheteConfig(config, zoom=[0, 20])


def test_zoom_dependend_functions(cleantopo_br):
    with mapchete.open(cleantopo_br.dict) as mp:
        with pytest.raises(ValueError):
            mp.config.params_at_zoom(20)
        with pytest.raises(ValueError):
            mp.config.area_at_zoom(20)


def test_validate_values():
    with pytest.raises(TypeError):
        validate_values(None, None)


def test_input_error(cleantopo_br_tiledir):
    config = deepcopy(cleantopo_br_tiledir.dict)
    config["input"].update(file1=dict(format="TileDirectory"))
    with pytest.raises(errors.MapcheteDriverError):
        MapcheteConfig(config)


def test_import_error(mp_tmpdir, cleantopo_br, import_error_py):
    """Assert import error is raised."""
    config = cleantopo_br.dict
    config.update(process=import_error_py)
    with pytest.raises(errors.MapcheteProcessImportError):
        mapchete.open(config)


def test_malformed_process(cleantopo_br, malformed_py):
    """Assert import error is raised."""
    config = cleantopo_br.dict
    config.update(process=malformed_py)
    with pytest.raises(errors.MapcheteProcessImportError):
        mapchete.open(config)


def test_process_import_error(mp_tmpdir, cleantopo_br, import_error_py):
    """Assert import error is raised."""
    config = cleantopo_br.dict
    config.update(process="not.existing.process.module")
    with pytest.raises(errors.MapcheteProcessImportError):
        mapchete.open(config)


def test_syntax_error(mp_tmpdir, cleantopo_br, syntax_error_py):
    """Assert syntax error is raised."""
    config = cleantopo_br.dict
    config.update(process=syntax_error_py)
    with pytest.raises(errors.MapcheteProcessSyntaxError):
        mapchete.open(config)


def test_process_exception(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(errors.MapcheteProcessException):
            mp.execute((5, 0, 0))


def test_output_error(mp_tmpdir, cleantopo_br, output_error_py):
    """Assert output error is raised."""
    config = cleantopo_br.dict
    config.update(process=output_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(errors.MapcheteProcessOutputError):
            mp.execute((5, 0, 0))


def _raise_error(i):
    """Helper function for test_finished_task()"""
    1 / 0


def test_finished_task():
    """Encapsulating exceptions test."""
    task = next(Executor().as_completed(func=_raise_error, iterable=[0]))
    assert task.exception()
    with pytest.raises(ZeroDivisionError):
        task.result()
    assert "FakeFuture" in str(task)


def test_strip_zoom_error(files_zooms):
    with pytest.raises(errors.MapcheteConfigError):
        config = files_zooms.dict
        config["input"]["equals"]["zoom=invalid"] = "dummy1.tif"
        mapchete.open(config)
