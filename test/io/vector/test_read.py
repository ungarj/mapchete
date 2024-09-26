import os

import pytest
from pytest_lazyfixture import lazy_fixture
from shapely.geometry import shape

from mapchete.config import MapcheteConfig
from mapchete.errors import MapcheteIOError
from mapchete.geometry import reproject_geometry
from mapchete.io.vector import (
    convert_vector,
    fiona_open,
    read_vector_window,
)
from mapchete.tile import BufferedTilePyramid
from mapchete.types import Bounds


@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("landpoly"),
    ],
)
@pytest.mark.parametrize("grid", ["geodetic", "mercator"])
@pytest.mark.parametrize("pixelbuffer", [0, 10, 500])
@pytest.mark.parametrize("zoom", [5, 3])
def test_read_vector_window(path, grid, pixelbuffer, zoom):
    """Read vector data from read_vector_window."""
    tile_pyramid = BufferedTilePyramid(grid, pixelbuffer=pixelbuffer)
    with fiona_open(path) as src:
        bbox = reproject_geometry(
            shape(Bounds.from_inp(src.bounds)), src.crs, tile_pyramid.crs
        )

    tiles = list(tile_pyramid.tiles_from_geom(bbox, zoom))

    for tile in tiles:
        features = read_vector_window(path, tile)
        if features:
            for feature in features:
                assert "properties" in feature
                assert shape(feature["geometry"]).is_valid
            break
    else:
        raise RuntimeError("no features read!")


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("landpoly_s3"),
        lazy_fixture("landpoly_http"),
        lazy_fixture("landpoly_secure_http"),
    ],
)
@pytest.mark.parametrize("grid", ["geodetic", "mercator"])
@pytest.mark.parametrize("pixelbuffer", [0, 10, 500])
@pytest.mark.parametrize("zoom", [5, 3])
def test_read_vector_window_remote(path, grid, pixelbuffer, zoom):
    test_read_vector_window(path, grid, pixelbuffer, zoom)


def test_read_vector_window_reproject(geojson, landpoly_3857):
    zoom = 4
    raw_config = geojson.dict
    raw_config["input"].update(file1=landpoly_3857)
    config = MapcheteConfig(raw_config)
    vectorfile = config.params_at_zoom(zoom)["input"]["file1"]
    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = tile_pyramid.tiles_from_geom(
        vectorfile.bbox(out_crs=tile_pyramid.crs), zoom
    )
    for tile in tiles:
        features = read_vector_window(vectorfile.path, tile)
        if features:
            for feature in features:
                assert "properties" in feature
                assert shape(feature["geometry"]).is_valid
            break
    else:
        raise RuntimeError("no features read!")


def test_read_vector_window_errors(invalid_geojson):
    with pytest.raises(FileNotFoundError):
        read_vector_window(
            "invalid_path", BufferedTilePyramid("geodetic").tile(0, 0, 0)
        )
    with pytest.raises(MapcheteIOError):
        read_vector_window(
            invalid_geojson, BufferedTilePyramid("geodetic").tile(0, 0, 0)
        )


def test_convert_vector_copy(aoi_br_geojson, tmpdir):
    out = os.path.join(tmpdir, "copied.geojson")

    # copy
    convert_vector(aoi_br_geojson, out)
    with fiona_open(str(out)) as src:
        assert list(iter(src))

    # raise error if output exists
    with pytest.raises(IOError):
        convert_vector(aoi_br_geojson, out, exists_ok=False)

    # do nothing if output exists
    convert_vector(aoi_br_geojson, out)
    with fiona_open(str(out)) as src:
        assert list(iter(src))


def test_convert_vector_overwrite(aoi_br_geojson, tmpdir):
    out = os.path.join(tmpdir, "copied.geojson")

    # write an invalid file
    with open(out, "w") as dst:
        dst.write("invalid")

    # overwrite
    convert_vector(aoi_br_geojson, out, overwrite=True)
    with fiona_open(str(out)) as src:
        assert list(iter(src))


def test_convert_vector_other_format_copy(aoi_br_geojson, tmpdir):
    out = os.path.join(tmpdir, "copied.gpkg")

    convert_vector(aoi_br_geojson, out, driver="GPKG")
    with fiona_open(str(out)) as src:
        assert list(iter(src))

    # raise error if output exists
    with pytest.raises(IOError):
        convert_vector(aoi_br_geojson, out, exists_ok=False)


def test_convert_vector_other_format_overwrite(aoi_br_geojson, tmpdir):
    out = os.path.join(tmpdir, "copied.gkpk")

    # write an invalid file
    with open(out, "w") as dst:
        dst.write("invalid")

    # overwrite
    convert_vector(aoi_br_geojson, out, driver="GPKG", overwrite=True)
    with fiona_open(str(out)) as src:
        assert list(iter(src))
