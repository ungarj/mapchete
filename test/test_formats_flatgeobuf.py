"""Test FlatGeobuf as process output."""
import pytest
from shapely.geometry import shape

import mapchete
from mapchete import formats
from mapchete.tile import BufferedTile


def test_input_data_read(mp_tmpdir, flatgeobuf, landpoly_3857):
    """Check FlatGeobuf as input data."""
    with mapchete.open(flatgeobuf.path) as mp:
        for tile in mp.get_process_tiles():
            assert isinstance(tile, BufferedTile)
            input_tile = formats.default.geojson.InputTile(tile, mp)
            assert isinstance(input_tile.read(), list)
            for feature in input_tile.read():
                assert isinstance(feature, dict)

    # reprojected FlatGeobuf
    config = flatgeobuf.dict
    config["input"].update(file1=landpoly_3857)
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


def test_output_data(mp_tmpdir, flatgeobuf):
    """Check FlatGeobuf as output data."""
    output_params = dict(
        grid="geodetic",
        format="FlatGeobuf",
        path=mp_tmpdir,
        schema=dict(properties=dict(id="int"), geometry="Polygon"),
        pixelbuffer=0,
        metatiling=1,
    )
    output = formats.default.flatgeobuf.OutputDataWriter(output_params)
    assert output.path == mp_tmpdir
    assert output.file_extension == ".fgb"
    assert isinstance(output_params, dict)

    with mapchete.open(flatgeobuf.path) as mp:
        tile = mp.config.process_pyramid.tile(4, 3, 7)
        # write empty
        mp.write(tile, None)
        # write data
        raw_output = mp.get_raw_output(tile)
        mp.write(tile, raw_output)
        # read data
        read_output = mp.get_raw_output(tile)
        assert isinstance(read_output, list)
        assert len(read_output)


@pytest.mark.remote
def test_s3_output_data(mp_s3_tmpdir, flatgeobuf_s3):
    """Check FlatGeobuf as output data."""
    output_params = dict(
        grid="geodetic",
        format="FlatGeobuf",
        path=mp_s3_tmpdir,
        schema=dict(properties=dict(id="int"), geometry="Polygon"),
        pixelbuffer=0,
        metatiling=1,
    )
    output = formats.default.flatgeobuf.OutputDataWriter(output_params)
    assert output.path == mp_s3_tmpdir
    assert output.file_extension == ".fgb"
    assert isinstance(output_params, dict)


@pytest.mark.remote
def test_s3_output_data_rw(mp_s3_tmpdir, flatgeobuf_s3):
    with mapchete.open(flatgeobuf_s3.dict) as mp:
        tile = mp.config.process_pyramid.tile(4, 3, 7)
        # write empty
        mp.write(tile, None)
        # write data
        raw_output = mp.execute(tile)
        mp.write(tile, raw_output)
        # read data
        read_output = mp.get_raw_output(tile)
        assert isinstance(read_output, list)
        assert len(read_output)


def test_multipolygon_output_data(mp_tmpdir, flatgeobuf):
    """Check FlatGeobuf as output data."""
    output_params = dict(
        grid="geodetic",
        format="FlatGeobuf",
        path=mp_tmpdir,
        schema=dict(
            properties=dict(id="int", name="str", area="float"), geometry="MultiPolygon"
        ),
        pixelbuffer=0,
        metatiling=2,
    )
    output = formats.default.flatgeobuf.OutputDataWriter(output_params)
    assert output.path == mp_tmpdir
    assert output.file_extension == ".fgb"
    assert isinstance(output_params, dict)

    with mapchete.open(
        dict(flatgeobuf.dict, zoom_levels=8, output=output_params)
    ) as mp:
        tile = mp.config.process_pyramid.tile(8, 45, 126)
        # write empty
        mp.write(tile, None)
        # write data
        raw_output = mp.get_raw_output(tile)
        mp.write(tile, raw_output)
        # read data
        read_output = mp.get_raw_output(tile)
        assert isinstance(read_output, list)
        assert len(read_output)
        for i in read_output:
            if i["geometry"]["type"] == "MultiPolygon":
                break
        else:
            raise TypeError("no MultiPolygon geometries found")
