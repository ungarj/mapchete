#!/usr/bin/env python
"""Test GeoJSON as process output."""

import os
import shutil
import yaml

import mapchete
from mapchete.formats.default import geojson
from mapchete.tile import BufferedTile

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")
TEMP_DIR = os.path.join(TESTDATA_DIR, "tmp")


def test_input_data_read():
    """Check GeoJSON as input data."""
    try:
        mp = mapchete.open(os.path.join(SCRIPTDIR, "testdata/geojson.mapchete"))
        for tile in mp.get_process_tiles():
            assert isinstance(tile, BufferedTile)
            input_tile = geojson.InputTile(tile, mp)
            assert isinstance(input_tile.read(), list)
            for feature in input_tile.read():
                assert isinstance(feature, dict)

        # reprojected GeoJSON
        with open(os.path.join(SCRIPTDIR, "testdata/geojson.mapchete")) as src:
            config = yaml.load(src.read())
            config["input_files"].update(
                file1=os.path.join(TESTDATA_DIR, "landpoly_3857.geojson"))
            config.update(config_dir=TESTDATA_DIR)
        mp = mapchete.open(config, mode="readonly")
        for tile in mp.get_process_tiles(4):
            assert isinstance(tile, BufferedTile)
            with mp.config.output.open(tile, mp) as input_tile:
                # input_tile = geojson.InputTile(tile, mp)
                assert input_tile.is_empty() in [False]
                assert isinstance(input_tile.read(), list)
                for feature in input_tile.read():
                    assert isinstance(feature, dict)
    finally:
        shutil.rmtree(TEMP_DIR, ignore_errors=True)


def test_output_data():
    """Check GeoJSON as output data."""
    output_params = dict(
        type="geodetic",
        format="GeoJSON",
        path=TEMP_DIR,
        schema=dict(properties=dict(id="int"), geometry="Polygon"),
        pixelbuffer=0,
        metatiling=1
    )
    output = geojson.OutputData(output_params)
    assert output.path == TEMP_DIR
    assert output.file_extension == ".geojson"
    assert isinstance(output_params, dict)

    try:
        mp = mapchete.open(os.path.join(SCRIPTDIR, "testdata/geojson.mapchete"))
        for tile in mp.get_process_tiles(4):
            # write empty
            mp.write(tile)
            # write data
            output = mp.get_raw_output(tile)
            mp.write(output)
            # read data
            out_tile = mp.config.output.read(tile)
            assert isinstance(out_tile.data, list)
            if output.data:
                assert out_tile.data
    finally:
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
