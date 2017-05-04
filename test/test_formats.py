#!/usr/bin/env python
"""Test Mapchete default formats."""

import os
from tilematrix import TilePyramid

import mapchete
from mapchete.formats import (
    available_input_formats, available_output_formats, driver_from_file, base)

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


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
    mp_input = config["input_files"]["file2"].open(
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
    except NotImplementedError:
        pass
    try:
        tmp.bbox()
    except NotImplementedError:
        pass
    try:
        tmp.exists()
    except NotImplementedError:
        pass

    # InputTile
    tmp = base.InputTile(None)
    try:
        tmp.read()
    except NotImplementedError:
        pass
    try:
        tmp.is_empty()
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
    except NotImplementedError:
        pass
    try:
        tmp.write(None)
    except NotImplementedError:
        pass
    try:
        tmp.tiles_exist(None)
    except NotImplementedError:
        pass
    try:
        tmp.is_valid_with_config(None)
    except NotImplementedError:
        pass
    try:
        tmp.for_web(None)
    except NotImplementedError:
        pass
    try:
        tmp.empty(None)
    except NotImplementedError:
        pass
    try:
        tmp.open(None, None)
    except NotImplementedError:
        pass
