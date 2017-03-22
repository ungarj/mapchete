#!/usr/bin/env python
"""Test Mapchete config module."""

import os
from shapely.geometry import Polygon
from shapely.wkt import loads

from mapchete.config import MapcheteConfig

scriptdir = os.path.dirname(os.path.realpath(__file__))


def test_config_zoom5():
    """Example configuration at zoom 5."""
    config = MapcheteConfig(os.path.join(scriptdir, "example.mapchete"))
    dummy2_abspath = os.path.join(scriptdir, "testdata/dummy2.tif")
    zoom5 = config.at_zoom(5)
    input_files = zoom5["input_files"]
    assert input_files["file1"] is None
    assert input_files["file2"].path == dummy2_abspath
    assert zoom5["some_integer_parameter"] == 12
    assert zoom5["some_float_parameter"] == 5.3
    assert zoom5["some_string_parameter"] == "string1"
    assert zoom5["some_bool_parameter"] is True


def test_config_zoom11():
    """Example configuration at zoom 11."""
    config = MapcheteConfig(os.path.join(scriptdir, "example.mapchete"))
    dummy1_abspath = os.path.join(scriptdir, "testdata/dummy1.tif")
    dummy2_abspath = os.path.join(scriptdir, "testdata/dummy2.tif")
    zoom11 = config.at_zoom(11)
    input_files = zoom11["input_files"]
    assert input_files["file1"].path == dummy1_abspath
    assert input_files["file2"].path == dummy2_abspath
    assert zoom11["some_integer_parameter"] == 12
    assert zoom11["some_float_parameter"] == 5.3
    assert zoom11["some_string_parameter"] == "string2"
    assert zoom11["some_bool_parameter"] is True


def test_read_zoom_level():
    """Read zoom level from config file."""
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/zoom.mapchete"))
    assert 5 in config.zoom_levels


def test_minmax_zooms():
    """Read min/max zoom levels from config file."""
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/minmax_zoom.mapchete"))
    for zoom in [7, 8, 9, 10]:
        assert zoom in config.zoom_levels


def test_override_zoom_levels():
    """Override zoom levels when constructing configuration."""
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/minmax_zoom.mapchete"), zoom=[1, 4])
    for zoom in [1, 2, 3, 4]:
        assert zoom in config.zoom_levels


def test_read_bounds():
    """Read bounds from config file."""
    config = MapcheteConfig(os.path.join(scriptdir, "testdata/zoom.mapchete"))
    test_polygon = Polygon([
        [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]
    ])
    assert config.process_area(5).equals(test_polygon)


def test_override_bounds():
    """Override bounds when construcing configuration."""
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/zoom.mapchete"),
        bounds=[3, 2, 3.5, 1.5])
    test_polygon = Polygon([
        [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]])
    assert config.process_area(5).equals(test_polygon)


def test_bounds_from_input_files():
    """Read bounds from input files."""
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/files_bounds.mapchete"))
    test_polygon = Polygon(
        [[3, 2], [4, 2], [4, 1], [3, 1], [2, 1], [2, 4], [3, 4], [3, 2]])
    assert config.process_area(10).equals(test_polygon)


def test_read_mapchete_input():
    """Read Mapchete files as input files."""
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/mapchete_input.mapchete"))
    area = config.process_area(5)
    testpolygon = "POLYGON ((3 2, 3.5 2, 3.5 1.5, 3 1.5, 3 1, 2 1, 2 4, 3 4, 3 2))"
    assert area.equals(loads(testpolygon))


def test_read_baselevels():
    """Read baselevels."""
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/baselevels.mapchete"))
    assert isinstance(config.baselevels, dict)
    assert set(config.baselevels["zooms"]) == set([12, 13, 14])
    assert config.baselevels["lower"] == "bilinear"
    assert config.baselevels["higher"] == "nearest"


def test_read_input_groups():
    """Read input data groups."""
    config = MapcheteConfig(
        os.path.join(scriptdir, "testdata/file_groups.mapchete"))
    input_files = config.at_zoom(0)["input_files"]
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
