#!/usr/bin/env python
"""Test GeoJSON as process output."""

import os

import mapchete
from mapchete.formats.default import geojson
from mapchete.tile import BufferedTile

scriptdir = os.path.dirname(os.path.realpath(__file__))


def test_input_data_read():
    """Check GeoJSON as input data."""
    mp = mapchete.open(os.path.join(scriptdir, "testdata/geojson.mapchete"))
    for tile in mp.get_process_tiles():
        assert isinstance(tile, BufferedTile)
        input_tile = geojson.InputTile(tile, mp)
        assert isinstance(input_tile.read(), list)
        for feature in input_tile.read():
            assert isinstance(feature, dict)


def test_output_data():
    """Check GeoJSON as output data."""
    output_params = dict(
        type="geodetic",
        format="GeoJSON",
        path="my/output/directory",
        schema=dict(properties=dict(id="int"), geometry="Polygon"),
        pixelbuffer=0,
        metatiling=1
    )
    output = geojson.OutputData(output_params)
    assert output.path == "my/output/directory"
    assert output.file_extension == ".geojson"
    assert isinstance(output_params, dict)
    # TODO output.read()
    # TODO output.write() --> also malformed data
    # TODO ouput.tiles_exist()
    # TODO ouput.get_path()
    # TODO output.prepare_path()
    # TODO output.open() --> InputTile
