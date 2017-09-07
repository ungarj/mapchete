#!/usr/bin/env python
"""Test Mapchete default formats."""

import os
import yaml
from tilematrix import TilePyramid

import mapchete
from mapchete.formats import (
    available_input_formats, available_output_formats, driver_from_file, base)

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


def test_mapchete_input():
    """Mapchete process as input for other process."""
    mp = mapchete.open(os.path.join(TESTDATA_DIR, "mapchete_input.mapchete"))
    config = mp.config.at_zoom(5)
    mp_input = config["input"]["file2"].open(
        mp.get_process_tiles(5).next())
    assert mp_input.is_empty()


def test_base_format_classes():
    """Base format classes."""
    # InputData
    tp = TilePyramid("geodetic")
    tmp = base.InputData(dict(pyramid=tp, pixelbuffer=0))
    assert tmp.pyramid
    assert tmp.pixelbuffer == 0
    assert tmp.crs
    assert tmp.srid
    try:
        tmp.open(None)
        raise Exception()
    except NotImplementedError:
        pass
    try:
        tmp.bbox()
        raise Exception()
    except NotImplementedError:
        pass
    try:
        tmp.exists()
        raise Exception()
    except NotImplementedError:
        pass

    # InputTile
    tmp = base.InputTile(None)
    try:
        tmp.read()
        raise Exception()
    except NotImplementedError:
        pass
    try:
        tmp.is_empty()
        raise Exception()
    except NotImplementedError:
        pass

    # OutputData
    tmp = base.OutputData(dict(pixelbuffer=0, type="geodetic", metatiling=1))
    assert tmp.pyramid
    assert tmp.pixelbuffer == 0
    assert tmp.crs
    assert tmp.srid
    try:
        tmp.read(None)
        raise Exception()
    except NotImplementedError:
        pass
    try:
        tmp.write(None)
        raise Exception()
    except NotImplementedError:
        pass
    try:
        tmp.tiles_exist(None)
        raise Exception()
    except NotImplementedError:
        pass
    try:
        tmp.is_valid_with_config(None)
        raise Exception()
    except NotImplementedError:
        pass
    try:
        tmp.for_web(None)
        raise Exception()
    except NotImplementedError:
        pass
    try:
        tmp.empty(None)
        raise Exception()
    except NotImplementedError:
        pass
    try:
        tmp.open(None, None)
        raise Exception()
    except NotImplementedError:
        pass


def test_http_rasters():
    """Raster file on remote server with http:// or https:// URLs."""
    zoom = 13
    with open(
        os.path.join(SCRIPTDIR, "testdata/files_bounds.mapchete"), "r"
    ) as src:
        config = yaml.load(src.read())
        config.update(
            input_files=dict(file1=HTTP_RASTER),
            config_dir=os.path.join(SCRIPTDIR, "testdata"),
            process_zoom=zoom
        )
    # TODO make tests more performant
    with mapchete.open(config) as mp:
        assert mp.config.process_area(zoom).area > 0
        # tile = mp.get_raw_output(mp.get_process_tiles(zoom).next())
        # assert not tile.data.mask.all()
