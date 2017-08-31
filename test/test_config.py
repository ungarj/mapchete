#!/usr/bin/env python
"""Test Mapchete config module."""

import os
import yaml
from shapely.geometry import Polygon
from shapely.wkt import loads

import mapchete
from mapchete.config import MapcheteConfig
from mapchete.errors import MapcheteDriverError


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))


def test_config_zoom5():
    """Example configuration at zoom 5."""
    config = MapcheteConfig(os.path.join(SCRIPTDIR, "example.mapchete"))
    dummy2_abspath = os.path.join(SCRIPTDIR, "testdata/dummy2.tif")
    zoom5 = config.at_zoom(5)
    input_files = zoom5["input"]
    assert input_files["file1"] is None
    assert input_files["file2"].path == dummy2_abspath
    assert zoom5["some_integer_parameter"] == 12
    assert zoom5["some_float_parameter"] == 5.3
    assert zoom5["some_string_parameter"] == "string1"
    assert zoom5["some_bool_parameter"] is True


def test_config_zoom11():
    """Example configuration at zoom 11."""
    config = MapcheteConfig(os.path.join(SCRIPTDIR, "example.mapchete"))
    dummy1_abspath = os.path.join(SCRIPTDIR, "testdata/dummy1.tif")
    dummy2_abspath = os.path.join(SCRIPTDIR, "testdata/dummy2.tif")
    zoom11 = config.at_zoom(11)
    input_files = zoom11["input"]
    assert input_files["file1"].path == dummy1_abspath
    assert input_files["file2"].path == dummy2_abspath
    assert zoom11["some_integer_parameter"] == 12
    assert zoom11["some_float_parameter"] == 5.3
    assert zoom11["some_string_parameter"] == "string2"
    assert zoom11["some_bool_parameter"] is True


def test_read_zoom_level():
    """Read zoom level from config file."""
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/zoom.mapchete"))
    assert 5 in config.zoom_levels


def test_minmax_zooms():
    """Read min/max zoom levels from config file."""
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/minmax_zoom.mapchete"))
    for zoom in [7, 8, 9, 10]:
        assert zoom in config.zoom_levels


def test_override_zoom_levels():
    """Override zoom levels when constructing configuration."""
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/minmax_zoom.mapchete"), zoom=[1, 4])
    for zoom in [1, 2, 3, 4]:
        assert zoom in config.zoom_levels


def test_read_bounds():
    """Read bounds from config file."""
    config = MapcheteConfig(os.path.join(SCRIPTDIR, "testdata/zoom.mapchete"))
    test_polygon = Polygon([
        [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]
    ])
    assert config.process_area(5).equals(test_polygon)


def test_override_bounds():
    """Override bounds when construcing configuration."""
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/zoom.mapchete"),
        bounds=[3, 2, 3.5, 1.5])
    test_polygon = Polygon([
        [3, 1.5], [3, 2], [3.5, 2], [3.5, 1.5], [3, 1.5]])
    assert config.process_area(5).equals(test_polygon)


def test_input():
    """Parse configuration using "input" instead of "input"."""
    config = yaml.load(
        open(os.path.join(SCRIPTDIR, "example.mapchete"), "r").read()
    )
    # config["input"] = config.pop("input")
    config["config_dir"] = SCRIPTDIR
    assert mapchete.open(config)


def test_bounds_from_input_files():
    """Read bounds from input files."""
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/files_bounds.mapchete"))
    test_polygon = Polygon(
        [[3, 2], [4, 2], [4, 1], [3, 1], [2, 1], [2, 4], [3, 4], [3, 2]])
    assert config.process_area(10).equals(test_polygon)


def test_read_mapchete_input():
    """Read Mapchete files as input files."""
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/mapchete_input.mapchete"))
    area = config.process_area(5)
    testpolygon = "POLYGON ((3 2, 3.5 2, 3.5 1.5, 3 1.5, 3 1, 2 1, 2 4, 3 4, 3 2))"
    assert area.equals(loads(testpolygon))


def test_read_baselevels():
    """Read baselevels."""
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/baselevels.mapchete"))
    assert isinstance(config.baselevels, dict)
    assert set(config.baselevels["zooms"]) == set([5, 6])
    assert config.baselevels["lower"] == "bilinear"
    assert config.baselevels["higher"] == "nearest"

    # without min
    with open(
        os.path.join(SCRIPTDIR, "testdata/baselevels.mapchete")
    ) as mc:
        config = yaml.load(mc)
        config.update(config_dir=os.path.join(SCRIPTDIR, "testdata"))
        del config["baselevels"]["min"]
        assert min(MapcheteConfig(config).baselevels["zooms"]) == 3

    # without max and resampling
    with open(
        os.path.join(SCRIPTDIR, "testdata/baselevels.mapchete")
    ) as mc:
        config = yaml.load(mc)
        config.update(config_dir=os.path.join(SCRIPTDIR, "testdata"))
        del config["baselevels"]["max"]
        del config["baselevels"]["lower"]
        assert max(MapcheteConfig(config).baselevels["zooms"]) == 7
        assert MapcheteConfig(config).baselevels["lower"] == "nearest"


def test_empty_input_files():
    """Verify configuration gets parsed without input files."""
    with open(
        os.path.join(SCRIPTDIR, "testdata/file_groups.mapchete"), "r"
    ) as src:
        config = yaml.load(src.read())
        config.update(
            input_files=None, config_dir=os.path.join(SCRIPTDIR, "testdata")
        )
    assert mapchete.open(config)


def test_read_input_groups():
    """Read input data groups."""
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/file_groups.mapchete"))
    input_files = config.at_zoom(0)["input"]
    print input_files
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


def test_input_files_zooms():
    """Read correct input file per zoom."""
    config = MapcheteConfig(
        os.path.join(SCRIPTDIR, "testdata/files_zooms.mapchete"))
    # zoom 7
    input_files = config.at_zoom(7)["input"]
    assert os.path.basename(
        input_files["greater_smaller"].path) == "dummy1.tif"
    assert os.path.basename(input_files["equals"].path) == "dummy1.tif"
    # zoom 8
    input_files = config.at_zoom(8)["input"]
    assert os.path.basename(
        input_files["greater_smaller"].path) == "dummy1.tif"
    assert os.path.basename(input_files["equals"].path) == "dummy2.tif"
    # zoom 9
    input_files = config.at_zoom(9)["input"]
    assert os.path.basename(
        input_files["greater_smaller"].path) == "dummy2.tif"
    assert os.path.basename(input_files["equals"].path) == "cleantopo_br.tif"
    # zoom 10
    input_files = config.at_zoom(10)["input"]
    assert os.path.basename(
        input_files["greater_smaller"].path) == "dummy2.tif"
    assert os.path.basename(input_files["equals"].path) == "cleantopo_tl.tif"


def test_abstract_input():
    """Read abstract input definitions."""
    try:
        MapcheteConfig(
            os.path.join(SCRIPTDIR, "testdata/abstract_input.mapchete")
        )
        raise Exception
    except MapcheteDriverError:
        pass
