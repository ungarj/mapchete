"""Test Geobuf as process output."""
import geobuf as gb
import pytest
from shapely.geometry import shape

import mapchete
from mapchete import formats
from mapchete.tile import BufferedTile


def test_input_data_read(mp_tmpdir, geobuf, landpoly_3857):
    """Check Geobuf as input data."""
    with mapchete.open(geobuf.path) as mp:
        for tile in mp.get_process_tiles():
            assert isinstance(tile, BufferedTile)
            input_tile = formats.default.geojson.InputTile(tile, mp)
            assert isinstance(input_tile.read(), list)
            for feature in input_tile.read():
                assert isinstance(feature, dict)

    # reprojected Geobuf
    config = geobuf.dict
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


def test_for_web(client, mp_tmpdir):
    """Send Geobuf via flask."""
    tile_base_url = "/wmts_simple/1.0.0/geobuf/default/WGS84/"
    for url in ["/"]:
        response = client.get(url)
        assert response.status_code == 200
    features = 0
    for url in [
        tile_base_url + "4/12/31.pbf",
        tile_base_url + "4/12/30.pbf",
        tile_base_url + "4/11/31.pbf",
        tile_base_url + "4/11/30.pbf",
    ]:
        response = client.get(url)
        assert response.status_code == 200
        assert response.mimetype == "application/octet-stream"
        pbf = gb.decode(response.data)
        for feature in pbf["features"]:
            print(shape(feature["geometry"]))
            assert shape(feature["geometry"]).buffer(0).is_valid
            features += 1
    assert features


def test_output_data(mp_tmpdir, geobuf):
    """Check Geobuf as output data."""
    output_params = dict(
        grid="geodetic",
        format="Geobuf",
        path=mp_tmpdir,
        schema=dict(properties=dict(id="int"), geometry="Polygon"),
        pixelbuffer=0,
        metatiling=1,
    )
    output = formats.default.geobuf.OutputDataWriter(output_params)
    assert output.path == mp_tmpdir
    assert output.file_extension == ".pbf"
    assert isinstance(output_params, dict)

    with mapchete.open(geobuf.path) as mp:
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
def test_s3_output_data(mp_s3_tmpdir, geobuf_s3):
    """Check Geobuf as output data."""
    output_params = dict(
        grid="geodetic",
        format="Geobuf",
        path=mp_s3_tmpdir,
        schema=dict(properties=dict(id="int"), geometry="Polygon"),
        pixelbuffer=0,
        metatiling=1,
    )
    output = formats.default.geobuf.OutputDataWriter(output_params)
    assert output.path == mp_s3_tmpdir
    assert output.file_extension == ".pbf"
    assert isinstance(output_params, dict)


@pytest.mark.remote
def test_s3_output_data_rw(mp_s3_tmpdir, geobuf_s3):
    with mapchete.open(geobuf_s3.dict) as mp:
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


def test_multipolygon_output_data(mp_tmpdir, geobuf):
    """Check Geobuf as output data."""
    output_params = dict(
        grid="geodetic",
        format="Geobuf",
        path=mp_tmpdir,
        schema=dict(
            properties=dict(id="int", name="str", area="float"), geometry="MultiPolygon"
        ),
        pixelbuffer=0,
        metatiling=2,
    )
    output = formats.default.geobuf.OutputDataWriter(output_params)
    assert output.path == mp_tmpdir
    assert output.file_extension == ".pbf"
    assert isinstance(output_params, dict)

    with mapchete.open(dict(geobuf.dict, zoom_levels=8, output=output_params)) as mp:
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
