import pytest
from pytest_lazyfixture import lazy_fixture
from shapely.geometry import shape

from mapchete.config import MapcheteConfig
from mapchete.errors import MapcheteIOError
from mapchete.geometry import reproject_geometry
from mapchete.geometry.filter import is_type
from mapchete.io.vector import (
    fiona_open,
    read_vector_window,
)
from mapchete.tile import BufferedTilePyramid
from mapchete.bounds import Bounds


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

    for tile in tile_pyramid.tiles_from_geom(bbox, zoom):
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


@pytest.mark.parametrize(
    "geom_type",
    [
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
    ],
)
def test_read_vector_window_target_geometry_type(landpoly, geom_type):
    """Read vector data from read_vector_window."""
    tile_pyramid = BufferedTilePyramid("geodetic")
    with fiona_open(landpoly) as src:
        if src.bounds:
            bbox = reproject_geometry(
                shape(Bounds.from_inp(src.bounds)), src.crs, tile_pyramid.crs
            )
    for tile in tile_pyramid.tiles_from_geom(bbox, 2):
        features = read_vector_window(landpoly, tile, target_geometry_type=geom_type)
        if features:
            for feature in features:
                assert "properties" in feature
                geometry = shape(feature["geometry"])
                assert geometry.is_valid
                assert is_type(geometry, geom_type)
            break
    else:
        if geom_type == "Polygon":
            raise RuntimeError("no features read!")
