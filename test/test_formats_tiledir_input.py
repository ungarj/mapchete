"""Test Mapchete default formats."""

from copy import deepcopy
import os
import pytest
import six

from mapchete.formats import available_input_formats
from mapchete.errors import MapcheteDriverError

import mapchete


def test_driver_available():
    """Driver is correctly registered."""
    assert "TileDirectory" in available_input_formats()


def test_parse_bounds(geojson_tiledir):
    """Read and configure bounds."""
    # fall back to pyramid bounds
    with mapchete.open(geojson_tiledir.dict) as mp:
        ip = mp.config.params_at_zoom(4)["input"]["file1"]
        assert ip.bbox().bounds == (-180, -90, 180, 90)
    # user defined bounds
    user_bounds = (0, 0, 30, 30)
    geojson_tiledir.dict["input"]["file1"].update(bounds=user_bounds)
    with mapchete.open(geojson_tiledir.dict) as mp:
        ip = mp.config.params_at_zoom(4)["input"]["file1"]
        assert ip.bbox().bounds == user_bounds
        # reproject
        assert ip.bbox(out_crs="3857")


def test_read_vector_data(mp_tmpdir, geojson, geojson_tiledir):
    """Read vector data."""
    # prepare data
    with mapchete.open(geojson.path) as mp:
        bounds = mp.config.bounds_at_zoom()
        mp.batch_process(zoom=4)
    # read data
    for metatiling in [2, 4, 8]:
        _run_tiledir_process_vector(geojson_tiledir.dict, metatiling, bounds)


def _run_tiledir_process_vector(conf_dict, metatiling, bounds):
    conf = deepcopy(conf_dict)
    conf["pyramid"].update(metatiling=metatiling)
    features = []
    with mapchete.open(conf, mode="overwrite", bounds=bounds) as mp:
        for tile in mp.get_process_tiles(4):
            input_tile = next(six.itervalues(mp.config.input)).open(tile)
            features.extend(input_tile.read())
    assert features


def test_read_raster_data(mp_tmpdir, cleantopo_br, cleantopo_br_tiledir):
    """Read raster data."""
    # prepare data
    with mapchete.open(cleantopo_br.path) as mp:
        bounds = mp.config.bounds_at_zoom()
        mp.batch_process(zoom=4)
    for metatiling in [1, 2, 4, 8]:
        _run_tiledir_process_raster(
            cleantopo_br_tiledir.dict, metatiling, bounds)


def _run_tiledir_process_raster(conf_dict, metatiling, bounds):
    conf = deepcopy(conf_dict)
    conf["pyramid"].update(metatiling=metatiling)
    with mapchete.open(conf, mode="overwrite", bounds=bounds) as mp:
        assert any([
            next(six.itervalues(mp.config.input)).open(tile).read().any()
            for tile in mp.get_process_tiles(4)])


def test_read_remote_raster_data(mp_tmpdir, cleantopo_remote):
    """Read raster data."""
    with mapchete.open(cleantopo_remote.path) as mp:
        assert all([
            next(six.itervalues(mp.config.input)).open(tile).read().any()
            for tile in mp.get_process_tiles(1)])


def test_parse_errors(geojson_tiledir, cleantopo_br_tiledir):
    """Different configuration exceptions."""
    # without path
    vector_type = deepcopy(geojson_tiledir.dict)
    vector_type["input"]["file1"].pop("path")
    with pytest.raises(MapcheteDriverError):
        mapchete.open(vector_type)
    # without type
    vector_type = deepcopy(geojson_tiledir.dict)
    vector_type["input"]["file1"].pop("type")
    with pytest.raises(MapcheteDriverError):
        mapchete.open(vector_type)
    # wrong type
    vector_type = deepcopy(geojson_tiledir.dict)
    vector_type["input"]["file1"]["type"] = "invalid"
    with pytest.raises(MapcheteDriverError):
        mapchete.open(vector_type)
    # without extension
    vector_type = deepcopy(geojson_tiledir.dict)
    vector_type["input"]["file1"].pop("extension")
    with pytest.raises(MapcheteDriverError):
        mapchete.open(vector_type)
    # without invalid extension
    vector_type = deepcopy(geojson_tiledir.dict)
    vector_type["input"]["file1"]["extension"] = "invalid"
    with pytest.raises(MapcheteDriverError):
        mapchete.open(vector_type)

    # raster type specific
    ######################
    # without count
    raster_type = deepcopy(cleantopo_br_tiledir.dict)
    raster_type["input"]["file1"].pop("count")
    with pytest.raises(MapcheteDriverError):
        mapchete.open(raster_type)
    # without dtype
    raster_type = deepcopy(cleantopo_br_tiledir.dict)
    raster_type["input"]["file1"].pop("dtype")
    with pytest.raises(MapcheteDriverError):
        mapchete.open(raster_type)
