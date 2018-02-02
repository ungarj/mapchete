#!/usr/bin/env python
"""Test Mapchete config module."""

import pytest
import os
from shapely.geometry import Polygon
from shapely.wkt import loads
from copy import deepcopy

import mapchete
from mapchete.config import MapcheteConfig
from mapchete.errors import MapcheteDriverError, MapcheteConfigError


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))


def test_config_errors(example_mapchete):
    """Test various configuration parsing errors."""
    config_orig = example_mapchete.dict
    # wrong config type
    with pytest.raises(MapcheteConfigError):
        mapchete.open("not_a_config")
    # missing process_file
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config.pop("process_file")
        MapcheteConfig(config)
    # using input and input_files
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config.update(input=None, input_files=None)
        mapchete.open(config)
    # output configuration not compatible with driver
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config["output"].pop("bands")
        MapcheteConfig(config)
    # no baselevel params
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config.update(baselevels={})
        with mapchete.open(config) as mp:
            mp.config.baselevels
    # wrong baselevel min or max
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config.update(baselevels={"min": "invalid"})
        with mapchete.open(config) as mp:
            mp.config.baselevels
    # wrong pixelbuffer type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config["pyramid"].update(pixelbuffer="wrong_type")
        mapchete.open(config)
    # wrong metatiling type
    with pytest.raises(MapcheteConfigError):
        config = deepcopy(config_orig)
        config["pyramid"].update(metatiling="wrong_type")
        mapchete.open(config)


def test_config_zoom7(example_mapchete, dummy2_tif):
    """Example configuration at zoom 5."""
    config = MapcheteConfig(example_mapchete.path)
    zoom7 = config.params_at_zoom(7)
    input_files = zoom7["input"]
    assert input_files["file1"] is None
    assert input_files["file2"].path == dummy2_tif
    assert zoom7["some_integer_parameter"] == 12
    assert zoom7["some_float_parameter"] == 5.3
    assert zoom7["some_string_parameter"] == "string1"
    assert zoom7["some_bool_parameter"] is True


def test_config_zoom11(example_mapchete, dummy2_tif, dummy1_tif):
    """Example configuration at zoom 11."""
    config = MapcheteConfig(example_mapchete.path)
    zoom11 = config.params_at_zoom(11)
    input_files = zoom11["input"]
    assert input_files["file1"].path == dummy1_tif
    assert input_files["file2"].path == dummy2_tif
    assert zoom11["some_integer_parameter"] == 12
    assert zoom11["some_float_parameter"] == 5.3
    assert zoom11["some_string_parameter"] == "string2"
    assert zoom11["some_bool_parameter"] is True


def test_read_zoom_level(zoom_mapchete):
    """Read zoom level from config file."""
    config = MapcheteConfig(zoom_mapchete.path)
    assert 5 in config.zoom_levels


def test_minmax_zooms(minmax_zoom):
    """Read min/max zoom levels from config file."""
    config = MapcheteConfig(minmax_zoom.path)
    for zoom in [7, 8, 9, 10]:
        assert zoom in config.zoom_levels


def test_override_zoom_levels(minmax_zoom):
    """Override zoom levels when constructing configuration."""
    config = MapcheteConfig(minmax_zoom.path, zoom=[7, 8])
    for zoom in [7, 8]:
        assert zoom in config.zoom_levels


def test_read_bounds(zoom_mapchete):
    """Read bounds from config file."""
    config = MapcheteConfig(zoom_mapchete.path)
    test_polygon = Polygon([
        [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]])
    assert config.area_at_zoom(5).equals(test_polygon)


def test_override_bounds(zoom_mapchete):
    """Override bounds when construcing configuration."""
    config = MapcheteConfig(zoom_mapchete.path, bounds=[3, 2, 3.5, 1.5])
    test_polygon = Polygon([
        [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]])
    assert config.area_at_zoom(5).equals(test_polygon)


def test_bounds_from_input_files(files_bounds):
    """Read bounds from input files."""
    config = MapcheteConfig(files_bounds.path)
    test_polygon = Polygon(
        [[3, 2], [4, 2], [4, 1], [3, 1], [2, 1], [2, 4], [3, 4], [3, 2]])
    assert config.area_at_zoom(10).equals(test_polygon)


def test_read_mapchete_input(mapchete_input):
    """Read Mapchete files as input files."""
    config = MapcheteConfig(mapchete_input.path)
    area = config.area_at_zoom(5)
    testpolygon = "POLYGON ((4 1, 3 1, 2 1, 2 4, 3 4, 3 2, 4 2, 4 1))"
    assert area.equals(loads(testpolygon))


def test_read_baselevels(baselevels):
    """Read baselevels."""
    config = MapcheteConfig(baselevels.path)
    assert isinstance(config.baselevels, dict)
    assert set(config.baselevels["zooms"]) == set([5, 6])
    assert config.baselevels["lower"] == "bilinear"
    assert config.baselevels["higher"] == "nearest"

    # without min
    config = deepcopy(baselevels.dict)
    del config["baselevels"]["min"]
    assert min(MapcheteConfig(config).baselevels["zooms"]) == 3

    # without max and resampling
    config = deepcopy(baselevels.dict)
    del config["baselevels"]["max"]
    del config["baselevels"]["lower"]
    assert max(MapcheteConfig(config).baselevels["zooms"]) == 7
    assert MapcheteConfig(config).baselevels["lower"] == "nearest"


def test_empty_input(file_groups):
    """Verify configuration gets parsed without input files."""
    config = file_groups.dict
    config.update(input=None)
    assert mapchete.open(config)


def test_read_input_groups(file_groups):
    """Read input data groups."""
    config = MapcheteConfig(file_groups.path)
    input_files = config.params_at_zoom(0)["input"]
    assert "file1" in input_files["group1"]
    assert "file2" in input_files["group1"]
    assert "file1" in input_files["group2"]
    assert "file2" in input_files["group2"]
    assert "nested_group" in input_files
    assert "group1" in input_files["nested_group"]
    assert "file1" in input_files["nested_group"]["group1"]
    assert "file2" in input_files["nested_group"]["group1"]
    assert "file1" in input_files["nested_group"]["group2"]
    assert "file2" in input_files["nested_group"]["group2"]


def test_input_zooms(files_zooms):
    """Read correct input file per zoom."""
    config = MapcheteConfig(files_zooms.path)
    # zoom 7
    input_files = config.params_at_zoom(7)["input"]
    assert os.path.basename(
        input_files["greater_smaller"].path) == "dummy1.tif"
    assert os.path.basename(input_files["equals"].path) == "dummy1.tif"
    # zoom 8
    input_files = config.params_at_zoom(8)["input"]
    assert os.path.basename(
        input_files["greater_smaller"].path) == "dummy1.tif"
    assert os.path.basename(input_files["equals"].path) == "dummy2.tif"
    # zoom 9
    input_files = config.params_at_zoom(9)["input"]
    assert os.path.basename(
        input_files["greater_smaller"].path) == "dummy2.tif"
    assert os.path.basename(input_files["equals"].path) == "cleantopo_br.tif"
    # zoom 10
    input_files = config.params_at_zoom(10)["input"]
    assert os.path.basename(
        input_files["greater_smaller"].path) == "dummy2.tif"
    assert os.path.basename(input_files["equals"].path) == "cleantopo_tl.tif"


def test_abstract_input(abstract_input):
    """Read abstract input definitions."""
    with pytest.raises(MapcheteDriverError):
        MapcheteConfig(abstract_input.path)


def test_init_zoom(cleantopo_br):
    with mapchete.open(cleantopo_br.dict, zoom=[3, 5]) as mp:
        assert mp.config.init_zoom_levels == range(3, 6)
