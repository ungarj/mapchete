#!/usr/bin/env python
"""Test Mapchete default formats."""

import pytest
import os
import yaml
from tilematrix import TilePyramid
from rasterio.crs import CRS

import mapchete
from mapchete.formats import (
    available_input_formats, available_output_formats, driver_from_file, base,
    load_output_writer, load_input_reader
)
from mapchete import errors


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")
HTTP_RASTER = (
    "http://sentinel-s2-l1c.s3.amazonaws.com/tiles/33/T/WN/2016/4/3/0/B02.jp2"
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


def test_mapchete_input():
    """Mapchete process as input for other process."""
    mp = mapchete.open(os.path.join(TESTDATA_DIR, "mapchete_input.mapchete"))
    config = mp.config.at_zoom(5)
    input_data = config["input"]["file2"]
    assert input_data.bbox()
    assert input_data.bbox(CRS.from_epsg(3857))
    mp_input = input_data.open(mp.get_process_tiles(5).next())
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
        tmp.tiles_exist(None)
    with pytest.raises(NotImplementedError):
        tmp.is_valid_with_config(None)
    with pytest.raises(NotImplementedError):
        tmp.for_web(None)
    with pytest.raises(NotImplementedError):
        tmp.empty(None)
    with pytest.raises(NotImplementedError):
        tmp.open(None, None)


def test_http_rasters():
    """Raster file on remote server with http:// or https:// URLs."""
    zoom = 13
    with open(
        os.path.join(SCRIPTDIR, "testdata/files_bounds.mapchete"), "r"
    ) as src:
        config = yaml.load(src.read())
        config.update(
            input=dict(file1=HTTP_RASTER),
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_zoom=zoom
        )
    # TODO make tests more performant
    with mapchete.open(config) as mp:
        assert mp.config.process_area(zoom).area > 0
