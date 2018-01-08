"""Test Mapchete default formats."""

import pytest
import os
import yaml
from tilematrix import TilePyramid
from rasterio.crs import CRS
from mapchete.formats import available_input_formats
from mapchete.formats.default import tile_directory

import mapchete
from mapchete import errors


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


def test_driver_available():
    assert "TileDirectory" in available_input_formats()


def test_parse_bounds():
    config = yaml.load(open(
        os.path.join(TESTDATA_DIR, "geojson_tiledir.mapchete")
    ).read())
    config.update(config_dir=TESTDATA_DIR)
    config["input"]["file1"].update(path=SCRIPTDIR)
    # fall back to pyramid bounds
    with mapchete.open(config) as mp:
        ip = mp.config.at_zoom(4)["input"]["file1"]
        assert ip.bbox().bounds == (-180, -90, 180, 90)
    # user defined bounds
    user_bounds = (0, 0, 30, 30)
    config["input"]["file1"].update(bounds=user_bounds)
    with mapchete.open(config) as mp:
        ip = mp.config.at_zoom(4)["input"]["file1"]
        assert ip.bbox().bounds == user_bounds
        # reproject
        assert ip.bbox(out_crs="3857")


def test_read_vector_data():
    pass


def test_read_raster_data():
    pass


def test_parse_errors():
    pass
