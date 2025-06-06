"""Test Mapchete default formats."""

import shutil
from copy import deepcopy

import pytest

import mapchete
from mapchete.errors import MapcheteDriverError
from mapchete.formats import available_input_formats
from mapchete.formats.default.tile_directory import InputData


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


@pytest.mark.parametrize("metatiling", [2, 4, 8])
def test_read_vector_data(geojson, geojson_tiledir, metatiling):
    """Read vector data."""
    tile = geojson.first_process_tile()
    # prepare data
    with mapchete.open(geojson.dict) as mp:
        bounds = mp.config.bounds_at_zoom()
        list(mp.execute(tile=tile))
    # read data
    config = geojson_tiledir.dict.copy()
    config["input"]["file1"]["path"] = mp.config.output.path
    config["pyramid"].update(metatiling=metatiling)
    features = []
    with mapchete.open(config, mode="overwrite", bounds=bounds) as mp:
        input_tile = next(iter(mp.config.input.values())).open(tile)
        features.extend(input_tile.read())
    assert features


def test_read_raster_data(mp_tmpdir, cleantopo_br, cleantopo_br_tiledir):
    """Read raster data."""
    # prepare data
    with mapchete.open(cleantopo_br.dict) as mp:
        bounds = mp.config.bounds_at_zoom()
        list(mp.execute(zoom=4))
    config = cleantopo_br_tiledir.dict.copy()
    config["input"]["file1"]["path"] = mp.config.output.path
    for metatiling in [1, 2, 4, 8]:
        cleantopo_br_tiledir.clear_output()
        _run_tiledir_process_raster(config, metatiling, bounds)


def _run_tiledir_process_raster(conf_dict, metatiling, bounds):
    conf = deepcopy(conf_dict)
    conf["pyramid"].update(metatiling=metatiling)
    with mapchete.open(conf, mode="overwrite", bounds=bounds) as mp:
        assert any(
            [
                next(iter(mp.config.input.values())).open(tile).read().any()
                for tile in mp.get_process_tiles(4)
            ]
        )
        # read empty tile
        assert (
            not next(iter(mp.config.input.values()))
            .open(mp.config.process_pyramid.tile(4, 0, 0))
            .read()
            .any()
        )
        shutil.rmtree(mp.config.output.path, ignore_errors=True)


def test_read_reprojected_raster_data(
    mp_tmpdir, cleantopo_br, cleantopo_br_tiledir_mercator
):
    """Read reprojected raster data."""
    zoom = 4
    # prepare data
    with mapchete.open(cleantopo_br.dict) as mp:
        list(mp.execute(zoom=zoom))

    config = cleantopo_br_tiledir_mercator.dict.copy()
    config["input"]["file1"] = mp.config.output.path
    with mapchete.open(config, mode="overwrite") as mp:
        # read some data
        assert any(
            [
                next(iter(mp.config.input.values())).open(tile).read().any()
                for tile in mp.get_process_tiles(zoom)
            ]
        )
        # read empty tile
        assert (
            not next(iter(mp.config.input.values()))
            .open(mp.config.process_pyramid.tile(zoom, 0, 0))
            .read()
            .any()
        )
        # read from fixed zoom
        assert not any(
            [
                next(iter(mp.config.input.values()))
                .open(tile)
                .read(tile_directory_zoom=5)
                .any()
                for tile in mp.get_process_tiles(zoom)
            ]
        )
        # read using maxzoom
        assert not any(
            [
                next(iter(mp.config.input.values()))
                .open(tile)
                .read(matching_max_zoom=3)
                .any()
                for tile in mp.get_process_tiles(zoom)
            ]
        )
        # use fallback zoom
        assert any(
            [
                next(iter(mp.config.input.values()))
                .open(tile)
                .read(fallback_to_higher_zoom=True)
                .any()
                for tile in mp.get_process_tiles(5)
            ]
        )


def test_read_raster_data_from_lower_zoom(cleantopo_read_lower_zoom):
    """Read raster data."""
    mp = cleantopo_read_lower_zoom.process_mp()
    with mp.open("file1") as src:
        assert src._resampling == "bilinear"
        assert src.read().any()


def test_read_from_dir(mp_tmpdir, cleantopo_br, cleantopo_br_tiledir):
    """Read raster data."""
    # prepare data
    with mapchete.open(cleantopo_br.dict) as mp:
        bounds = mp.config.bounds_at_zoom()
        list(mp.execute(zoom=4))
    config = dict(cleantopo_br_tiledir.dict, input=dict(file1=mp.config.output.path))
    _run_tiledir_process_raster(config, 4, bounds)


def test_read_indexes_shape(cleantopo_br_tiledir):
    mp = cleantopo_br_tiledir.mp()
    tile = cleantopo_br_tiledir.first_process_tile()
    # create local TileDirectory
    list(mp.execute(tile=tile))
    input_data = InputData({"path": mp.config.output_reader.path})
    input_tile = input_data.open(tile)

    # no indexes --> 3D array
    three_d_arr = input_tile.read()
    assert three_d_arr.ndim == 3

    # list index --> 3D array
    three_d_arr = input_tile.read([1])
    assert three_d_arr.ndim == 3

    # int index --> 2D array
    two_d_arr = input_tile.read(1)
    assert two_d_arr.ndim == 2


def test_no_metadata_json(mp_tmpdir, cleantopo_br_tiledir):
    """Read raster data."""
    # prepare data
    with pytest.raises(MapcheteDriverError):
        mapchete.open(
            dict(cleantopo_br_tiledir.dict, input=dict(file1="tmp/cleantopo_br"))
        )


def test_parse_errors(geojson_tiledir, cleantopo_br_tiledir):
    """Different configuration exceptions."""
    # without path
    vector_type = deepcopy(geojson_tiledir.dict)
    vector_type["input"]["file1"].pop("path")
    with pytest.raises(MapcheteDriverError):
        mapchete.open(vector_type)
    # without type
    vector_type = deepcopy(geojson_tiledir.dict)
    vector_type["input"]["file1"].pop("grid")
    with pytest.raises(MapcheteDriverError):
        mapchete.open(vector_type)
    # wrong type
    vector_type = deepcopy(geojson_tiledir.dict)
    vector_type["input"]["file1"]["grid"] = "invalid"
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
