#!/usr/bin/env python
"""Fixtures such as Flask app for serve."""

from collections import namedtuple
import os
import pytest
import shutil
import yaml

from mapchete.cli.serve import create_app


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")
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


@pytest.fixture
def mp_tmpdir():
    """Setup and teardown temporary directory."""
    yield TEMP_DIR
    shutil.rmtree(TEMP_DIR, ignore_errors=True)


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


def _dict_from_mapchete(path):
    config = yaml.load(open(path).read())
    config.update(config_dir=os.path.dirname(path))
    return config
