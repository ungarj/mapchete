#!/usr/bin/env python
"""Fixtures such as Flask app for serve."""

from collections import namedtuple
import os
import pytest
import shutil
import yaml

from mapchete.cli.serve import create_app


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPT_DIR, "testdata")
TEMP_DIR = os.path.join(TESTDATA_DIR, "tmp")


ExampleConfig = namedtuple("ExampleConfig", ("path", "dict"))


class Namespace(object):
    """Dummy argparse class."""

    def __init__(self, **kwargs):
        """Initialize using a dictionary."""
        self.__dict__.update(kwargs)


@pytest.fixture
def app():
    """Dummy Flask app."""
    example_process = os.path.join(
        SCRIPTDIR, "testdata/dem_to_hillshade.mapchete")
    args = Namespace(
        port=5000, mapchete_file=example_process, zoom=None, bounds=None,
        input_file=None, memory=None, readonly=False, overwrite=True,
        debug=True
    )
    return create_app(args)


# temporary directory for I/O tests
@pytest.fixture
def mp_tmpdir():
    """Setup and teardown temporary directory."""
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    os.makedirs(TEMP_DIR)
    yield TEMP_DIR
    shutil.rmtree(TEMP_DIR, ignore_errors=True)


# example files
@pytest.fixture
def http_raster():
    """Fixture for HTTP raster."""
    return "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/1/0/0.tif"


@pytest.fixture
def landpoly():
    """Fixture for landpoly.geojson"""
    return os.path.join(TESTDATA_DIR, "landpoly.geojson")


@pytest.fixture
def landpoly_3857():
    """Fixture for landpoly_3857.geojson"""
    return os.path.join(TESTDATA_DIR, "landpoly_3857.geojson")


@pytest.fixture
def cleantopo_br_tif():
    """Fixture for cleantopo_br.tif"""
    return os.path.join(TESTDATA_DIR, "cleantopo_br.tif")


@pytest.fixture
def dummy1_3857_tif():
    """Fixture for dummy1_3857.tif"""
    return os.path.join(TESTDATA_DIR, "dummy1_3857.tif")


@pytest.fixture
def dummy1_tif():
    """Fixture for dummy1.tif"""
    return os.path.join(TESTDATA_DIR, "dummy1.tif")


@pytest.fixture
def dummy2_tif():
    """Fixture for dummy2.tif"""
    return os.path.join(TESTDATA_DIR, "dummy2.tif")


@pytest.fixture
def import_error_py():
    """Fixture for import_error.py"""
    return os.path.join(TESTDATA_DIR, "import_error.py")


@pytest.fixture
def malformed_py():
    """Fixture for malformed.py"""
    return os.path.join(TESTDATA_DIR, "malformed.py")


@pytest.fixture
def syntax_error_py():
    """Fixture for syntax_error.py"""
    return os.path.join(TESTDATA_DIR, "syntax_error.py")


@pytest.fixture
def execute_params_error_py():
    """Fixture for execute_params_error.py"""
    return os.path.join(TESTDATA_DIR, "execute_params_error.py")


@pytest.fixture
def process_error_py():
    """Fixture for process_error.py"""
    return os.path.join(TESTDATA_DIR, "process_error.py")


@pytest.fixture
def output_error_py():
    """Fixture for output_error.py"""
    return os.path.join(TESTDATA_DIR, "output_error.py")


@pytest.fixture
def process_as_function_py():
    """Fixture for process_as_function.py"""
    return os.path.join(SCRIPT_DIR, "example_process_as_function.py")


@pytest.fixture
def old_style_process_py():
    """Fixture for old_style_process.py"""
    return os.path.join(TESTDATA_DIR, "old_style_process.py")


# example mapchete configurations
@pytest.fixture
def abstract_input():
    """Fixture for abstract_input.mapchete."""
    path = os.path.join(TESTDATA_DIR, "abstract_input.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def files_zooms():
    """Fixture for files_zooms.mapchete."""
    path = os.path.join(TESTDATA_DIR, "files_zooms.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def file_groups():
    """Fixture for file_groups.mapchete."""
    path = os.path.join(TESTDATA_DIR, "file_groups.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def baselevels():
    """Fixture for baselevels.mapchete."""
    path = os.path.join(TESTDATA_DIR, "baselevels.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def mapchete_input():
    """Fixture for mapchete_input.mapchete."""
    path = os.path.join(TESTDATA_DIR, "mapchete_input.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def files_bounds():
    """Fixture for files_bounds.mapchete."""
    path = os.path.join(TESTDATA_DIR, "files_bounds.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def example_mapchete():
    """Fixture for example.mapchete."""
    path = os.path.join(SCRIPT_DIR, "example.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def zoom_mapchete():
    """Fixture for zoom.mapchete."""
    path = os.path.join(TESTDATA_DIR, "zoom.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def minmax_zoom():
    """Fixture for minmax_zoom.mapchete."""
    path = os.path.join(TESTDATA_DIR, "minmax_zoom.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_tl():
    """Fixture for cleantopo_tl.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_tl.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_br():
    """Fixture for cleantopo_br.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_br.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_remote():
    """Fixture for cleantopo_remote.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_remote.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def cleantopo_br_tiledir():
    """Fixture for cleantopo_br_tiledir.mapchete."""
    path = os.path.join(TESTDATA_DIR, "cleantopo_br_tiledir.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def geojson():
    """Fixture for geojson.mapchete."""
    path = os.path.join(TESTDATA_DIR, "geojson.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


@pytest.fixture
def geojson_tiledir():
    """Fixture for geojson_tiledir.mapchete."""
    path = os.path.join(TESTDATA_DIR, "geojson_tiledir.mapchete")
    return ExampleConfig(path=path, dict=_dict_from_mapchete(path))


# helper functions
def _dict_from_mapchete(path):
    config = yaml.load(open(path).read())
    config.update(config_dir=os.path.dirname(path))
    return config
