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
        with mapchete.open(
            os.path.join(SCRIPTDIR, "testdata/geojson.mapchete")
        ) as mp:
            for tile in mp.get_process_tiles():
                assert isinstance(tile, BufferedTile)
                input_tile = geojson.InputTile(tile, mp)
                assert isinstance(input_tile.read(), list)
                for feature in input_tile.read():
                    assert isinstance(feature, dict)

        # reprojected GeoJSON
        with open(os.path.join(SCRIPTDIR, "testdata/geojson.mapchete")) as src:
            config = yaml.load(src.read())
            config["input"].update(
                file1=os.path.join(TESTDATA_DIR, "landpoly_3857.geojson"))
            config.update(config_dir=TESTDATA_DIR)
        # first, write tiles
        with mapchete.open(config, mode="overwrite") as mp:
            for tile in mp.get_process_tiles(4):
                assert isinstance(tile, BufferedTile)
                output = mp.get_raw_output(tile)
                mp.write(tile, output)
        # then, read output
        with mapchete.open(config, mode="readonly") as mp:
            any_data = False
            for tile in mp.get_process_tiles(4):
                with mp.config.output.open(tile, mp) as input_tile:
                    if input_tile.is_empty():
                        continue
                    any_data = True
                    assert isinstance(input_tile.read(), list)
                    for feature in input_tile.read():
                        assert isinstance(feature, dict)
            assert any_data
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
            # mp.write(tile, None)
            # write data
            raw_output = mp.get_raw_output(tile)
            mp.write(tile, raw_output)
            # read data
            read_output = mp.config.output.read(tile)
            assert isinstance(read_output, list)
            # TODO
            # if raw_output:
            #     assert read_output
    finally:
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
