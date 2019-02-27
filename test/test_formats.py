#!/usr/bin/env python
"""Test Mapchete default formats."""

import pytest
from tilematrix import TilePyramid
from rasterio.crs import CRS

import mapchete
from mapchete import MapcheteProcess, errors
from mapchete.formats import (
    available_input_formats, available_output_formats, driver_from_file, base,
    load_output_writer, load_input_reader
)


def test_available_input_formats():
    """Check if default input formats can be listed."""
    assert set(['Mapchete', 'raster_file', 'vector_file']).issubset(
        set(available_input_formats()))


def test_available_output_formats():
    """Check if default output formats can be listed."""
    assert set(['GTiff', 'PNG', 'PNG_hillshade', 'GeoJSON']).issubset(
        set(available_output_formats()))


def test_filename_to_driver():
    """Check converting file names to driver."""
    for filename in [
        'temp.mapchete', 'temp.tif', 'temp.jp2', 'temp.png', 'temp.vrt',
        'temp.geojson', 'temp.shp'
    ]:
        assert driver_from_file(filename)


def test_output_writer_errors():
    """Test errors when loading output writer."""
    with pytest.raises(TypeError):
        load_output_writer("not_a_dictionary")
    with pytest.raises(errors.MapcheteDriverError):
        load_output_writer({"format": "invalid_driver"})


def test_input_reader_errors():
    """Test errors when loading input readers."""
    with pytest.raises(TypeError):
        load_input_reader("not_a_dictionary")
    with pytest.raises(errors.MapcheteDriverError):
        load_input_reader({})
    with pytest.raises(errors.MapcheteDriverError):
        load_input_reader({"abstract": {"format": "invalid_format"}})


def test_driver_from_file_errors():
    """Test errors when determining input driver from filename."""
    with pytest.raises(errors.MapcheteDriverError):
        driver_from_file("invalid_extension.exe")


def test_mapchete_input(mapchete_input):
    """Mapchete process as input for other process."""
    with mapchete.open(mapchete_input.path) as mp:
        config = mp.config.params_at_zoom(5)
        input_data = config["input"]["file2"]
        assert input_data.bbox()
        assert input_data.bbox(CRS.from_epsg(3857))
        mp_input = input_data.open(next(mp.get_process_tiles(5)))
        assert not mp_input.is_empty()


def test_base_format_classes():
    """Base format classes."""
    # InputData
    tp = TilePyramid("geodetic")
    tmp = base.InputData(dict(pyramid=tp, pixelbuffer=0))
    assert tmp.pyramid
    assert tmp.pixelbuffer == 0
    assert tmp.crs
    assert tmp.srid
    with pytest.raises(NotImplementedError):
        tmp.open(None)
    with pytest.raises(NotImplementedError):
        tmp.bbox()
    with pytest.raises(NotImplementedError):
        tmp.exists()

    # InputTile
    tmp = base.InputTile(None)
    with pytest.raises(NotImplementedError):
        tmp.read()
    with pytest.raises(NotImplementedError):
        tmp.is_empty()

    # OutputData
    tmp = base.OutputData(dict(pixelbuffer=0, type="geodetic", metatiling=1))
    assert tmp.pyramid
    assert tmp.pixelbuffer == 0
    assert tmp.crs
    assert tmp.srid
    with pytest.raises(NotImplementedError):
        tmp.read(None)
    with pytest.raises(NotImplementedError):
        tmp.write(None)
    with pytest.raises(NotImplementedError):
        tmp.is_valid_with_config(None)
    with pytest.raises(NotImplementedError):
        tmp.for_web(None)
    with pytest.raises(NotImplementedError):
        tmp.empty(None)
    with pytest.raises(NotImplementedError):
        tmp.open(None, None)


def test_http_rasters(files_bounds, http_raster):
    """Raster file on remote server with http:// or https:// URLs."""
    zoom = 13
    config = files_bounds.dict
    config.update(input=dict(file1=http_raster), zoom_levels=zoom)
    # TODO make tests more performant
    with mapchete.open(config) as mp:
        assert mp.config.area_at_zoom(zoom).area > 0
        process_tile = next(mp.get_process_tiles(13))
        process = MapcheteProcess(
            config=mp.config, tile=process_tile,
            params=mp.config.params_at_zoom(process_tile.zoom)
        )
        with process.open("file1") as f:
            assert f.read().any()


def test_read_from_raster_file(cleantopo_br):
    """Read different bands from source raster."""
    with mapchete.open(cleantopo_br.path) as mp:
        process_tile = mp.config.process_pyramid.tile(5, 0, 0)
        process = MapcheteProcess(
            config=mp.config, tile=process_tile,
            params=mp.config.params_at_zoom(process_tile.zoom)
        )
        with process.open("file1") as f:
            assert f.read().shape == f.read([1]).shape == f.read(1).shape


def test_invalid_input_type(example_mapchete):
    """Raise MapcheteDriverError."""
    # invalid input type
    config = example_mapchete.dict
    config.update(input=dict(invalid_type=1))
    with pytest.raises(errors.MapcheteConfigError):
        mapchete.open(config)
