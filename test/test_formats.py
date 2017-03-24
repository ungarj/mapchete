#!/usr/bin/env python
"""Test Mapchete default formats."""

from mapchete.formats import (
    available_input_formats, available_output_formats, driver_from_file)


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
