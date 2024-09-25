import os

import pytest
from fiona.errors import DriverError
from pytest_lazyfixture import lazy_fixture
from rasterio.crs import CRS
from shapely.geometry import box, mapping, shape

from mapchete.config import MapcheteConfig
from mapchete.errors import MapcheteIOError, NoCRSError, NoGeoError
from mapchete.geometry import reproject_geometry
from mapchete.io.vector import (
    IndexedFeatures,
    convert_vector,
    fiona_open,
    read_vector_window,
    write_vector_window,
)
from mapchete.io.vector.indexed_features import object_bounds, object_crs
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


def test_write_vector_window_errors(landpoly):
    with fiona_open(str(landpoly)) as src:
        feature = next(iter(src))
    with pytest.raises((DriverError, ValueError, TypeError)):
        write_vector_window(
            in_data=["invalid", feature],
            out_tile=BufferedTilePyramid("geodetic").tile(0, 0, 0),
            out_path="/invalid_path",
            out_schema=dict(geometry="Polygon", properties=dict()),
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


def test_indexed_features(landpoly):
    with fiona_open(str(landpoly)) as src:
        some_id = next(iter(src))["id"]
        features = IndexedFeatures(src)

    assert features[some_id]
    with pytest.raises(KeyError):
        features["invalid_key"]

    assert features.items()
    assert features.keys()

    for f in features:
        assert "properties" in f
        assert "geometry" in f


def test_indexed_features_bounds():
    feature = {"bounds": (0, 1, 2, 3)}
    assert IndexedFeatures([(0, feature)])

    feature = {"geometry": mapping(box(0, 1, 2, 3))}
    assert IndexedFeatures([(0, feature)])

    feature = {"geometry": box(0, 1, 2, 3)}
    assert IndexedFeatures([(0, feature)])

    # no featuere id
    with pytest.raises(TypeError):
        IndexedFeatures([feature])

    class Foo:
        __geo_interface__ = mapping(box(0, 1, 2, 3))
        id = 0

    feature = Foo()
    assert IndexedFeatures([(0, feature)])

    # no bounds
    feature = {"properties": {"foo": "bar"}, "id": 0}
    with pytest.raises(NoGeoError):
        IndexedFeatures([feature])


def test_indexed_features_fakeindex(landpoly):
    with fiona_open(str(landpoly)) as src:
        features = list(src)
        idx = IndexedFeatures(features)
        fake_idx = IndexedFeatures(features, index=None)
    bounds = (-135.0, 60, -90, 80)
    assert len(idx.filter(bounds)) == len(fake_idx.filter(bounds))


def test_indexed_features_polygon(aoi_br_geojson):
    with fiona_open(str(aoi_br_geojson)) as src:
        index = IndexedFeatures(src)
    tp = BufferedTilePyramid("geodetic")
    for tile in tp.tiles_from_bounds(bounds=index.bounds, zoom=5):
        assert len(index.filter(tile.bounds)) == 1


def test_object_bounds_attr_bounds():
    # if hasattr(obj, "bounds"):
    #     return validate_bounds(obj.bounds)
    control = (0, 1, 2, 3)

    class Foo:
        bounds = control

    assert object_bounds(Foo()) == (0, 1, 2, 3)


def test_object_bounds_geo_interface():
    # elif hasattr(obj, "__geo_interface__"):
    #     return validate_bounds(shape(obj).bounds)
    control = (0, 1, 2, 3)

    class Foo:
        __geo_interface__ = mapping(box(*control))

    assert object_bounds(Foo()) == (0, 1, 2, 3)


def test_object_bounds_attr_geometry():
    # elif hasattr(obj, "geometry"):
    #     return validate_bounds(to_shape(obj.geometry).bounds)
    control = (0, 1, 2, 3)

    class Foo:
        geometry = mapping(box(*control))

    assert object_bounds(Foo()) == (0, 1, 2, 3)


def test_object_bounds_attr_bbox():
    # elif hasattr(obj, "bbox"):
    #     return validate_bounds(obj.bbox)
    control = (0, 1, 2, 3)

    class Foo:
        bbox = control

    assert object_bounds(Foo()) == (0, 1, 2, 3)


def test_object_bounds_key_bbox():
    # elif obj.get("bounds"):
    #     return validate_bounds(obj["bounds"])
    control = (0, 1, 2, 3)

    foo = {"bounds": control}

    assert object_bounds(foo) == (0, 1, 2, 3)


def test_object_bounds_key_geometry():
    control = (0, 1, 2, 3)

    foo = {"geometry": mapping(box(*control))}

    assert object_bounds(foo) == (0, 1, 2, 3)


def test_object_crs_obj():
    class Foo:
        crs = "EPSG:4326"

    assert object_crs(Foo()) == CRS.from_epsg(4326)


def test_object_crs_dict():
    foo = dict(crs="EPSG:4326")

    assert object_crs(foo) == CRS.from_epsg(4326)


def test_object_crs_error():
    with pytest.raises(NoCRSError):
        object_crs("foo")


def test_object_bounds_reproject():
    obj = dict(bounds=(1, 2, 3, 4), crs="EPSG:4326")
    out = object_bounds(obj, dst_crs="EPSG:3857")
    control = reproject_geometry(box(1, 2, 3, 4), "EPSG:4326", "EPSG:3857")
    assert out == control


@pytest.mark.parametrize("path", [lazy_fixture("mp_tmpdir")])
@pytest.mark.parametrize("in_memory", [True, False])
def test_fiona_open_write(path, in_memory, landpoly):
    path = path / f"test_fiona_write-{in_memory}.tif"
    with fiona_open(landpoly) as src:
        with fiona_open(path, "w", in_memory=in_memory, **src.profile) as dst:
            dst.writerecords(src)
    assert path.exists()
    with fiona_open(path) as src:
        written = list(src)
        assert written


@pytest.mark.integration
@pytest.mark.parametrize("path", [lazy_fixture("mp_s3_tmpdir")])
@pytest.mark.parametrize("in_memory", [True, False])
def test_fiona_open_write_remote(path, in_memory, landpoly):
    test_fiona_open_write(path, in_memory, landpoly)
