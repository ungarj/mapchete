import pytest
from rasterio.crs import CRS
from shapely.geometry import mapping, box

from mapchete.errors import NoCRSError, NoGeoError
from mapchete.geometry.reproject import reproject_geometry
from mapchete.io.vector import fiona_open, IndexedFeatures
from mapchete.io.vector.indexed_features import (
    object_bounds,
    object_crs,
    object_geometry,
    read_union_geometry,
)
from mapchete.tile import BufferedTilePyramid


def test_indexed_features(landpoly):
    with fiona_open(str(landpoly)) as src:
        some_id = next(iter(src))["id"]
        features = IndexedFeatures(src)

    assert features[some_id]
    with pytest.raises(KeyError):
        features[-999]

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


def test_indexed_features_filter(aoi_br_geojson):
    with fiona_open(str(aoi_br_geojson)) as src:
        count = len(src)
        index = IndexedFeatures(src)
    tp = BufferedTilePyramid("geodetic")
    for tile in tp.tiles_from_bounds(bounds=index.bounds, zoom=5):
        assert len(index.filter(bounds=tile.bounds)) == 1
    assert len(index.filter()) == count


def test_indexed_features_filter_geom_type(aoi_br_geojson):
    with fiona_open(str(aoi_br_geojson)) as src:
        count = len(src)
        index = IndexedFeatures(src)
        geom_type = src.schema["geometry"]
    assert len(index.filter(target_geometry_type=geom_type)) == count
    assert len(index.filter(target_geometry_type="Point")) == 0


def test_indexed_features_read(aoi_br_geojson):
    with fiona_open(str(aoi_br_geojson)) as src:
        count = len(src)
        index = IndexedFeatures(src)
    assert len(index.read()) == count


def test_indexed_features_from_file(aoi_br_geojson):
    index = IndexedFeatures.from_file(aoi_br_geojson)
    assert index.read()


def test_indexed_features_from_file_grid(aoi_br_geojson):
    with fiona_open(aoi_br_geojson) as src:
        bounds = src.bounds
    tp = BufferedTilePyramid("geodetic")
    tile = next(tp.tiles_from_bounds(bounds=bounds, zoom=5))
    index = IndexedFeatures.from_file(aoi_br_geojson, grid=tile)
    assert not index.read()


def test_indexed_features_read_union_geometry(aoi_br_geojson):
    features = IndexedFeatures.from_file(aoi_br_geojson)
    assert features.read_union_geometry().is_valid

    tp = BufferedTilePyramid("geodetic")
    tile = next(tp.tiles_from_bounds(bounds=features.bounds, zoom=5))
    assert features.read_union_geometry(bounds=tile.bounds, clip=True).is_empty
    assert not features.read_union_geometry(
        bounds=(-180, -90, 180, 90), clip=True
    ).is_empty


def test_read_union_geometry(aoi_br_geojson):
    assert read_union_geometry(aoi_br_geojson).is_valid

    features = IndexedFeatures.from_file(aoi_br_geojson)
    tp = BufferedTilePyramid("geodetic")
    tile = next(tp.tiles_from_bounds(bounds=features.bounds, zoom=5))

    assert read_union_geometry(aoi_br_geojson, bounds=tile.bounds, clip=True).is_empty
    assert not read_union_geometry(
        aoi_br_geojson, bounds=(-180, -90, 180, 90), clip=True
    ).is_empty


def test_object_bounds_attr_bounds():
    control = (0, 1, 2, 3)

    class Foo:
        bounds = control

    assert object_bounds(Foo()) == control


def test_object_bounds_geo_interface():
    control = (0, 1, 2, 3)

    class Foo:
        __geo_interface__ = mapping(box(*control))

    assert object_bounds(Foo()) == control


def test_object_bounds_attr_geometry():
    control = (0, 1, 2, 3)

    class Foo:
        geometry = mapping(box(*control))

    assert object_bounds(Foo()) == control


def test_object_bounds_attr_bbox():
    control = (0, 1, 2, 3)

    class Foo:
        bbox = control

    assert object_bounds(Foo()) == control


def test_object_bounds_key_bbox():
    control = (0, 1, 2, 3)

    foo = {"bounds": control}

    assert object_bounds(foo) == control


def test_object_bounds_key_geometry():
    control = (0, 1, 2, 3)

    foo = {"geometry": mapping(box(*control))}

    assert object_bounds(foo) == control


def test_object_bounds_reproject():
    obj = dict(bounds=(1, 2, 3, 4), crs="EPSG:4326")
    out = object_bounds(obj, dst_crs="EPSG:3857")
    control = reproject_geometry(box(1, 2, 3, 4), "EPSG:4326", "EPSG:3857")
    assert out == control


def test_object_geometry_attr_bounds():
    control = box(0, 1, 2, 3)

    class Foo:
        bounds = control.bounds

    assert object_geometry(Foo()).equals(control)


def test_object_geometry_geo_interface():
    control = box(0, 1, 2, 3)

    class Foo:
        __geo_interface__ = mapping(control)

    assert object_geometry(Foo()).equals(control)


def test_object_geometry_attr_geometry():
    control = box(0, 1, 2, 3)

    class Foo:
        geometry = mapping(control)

    assert object_geometry(Foo()).equals(control)


def test_object_geometry_attr_bbox():
    control = box(0, 1, 2, 3)

    class Foo:
        bbox = control.bounds

    assert object_geometry(Foo()).equals(control)


def test_object_geometry_key_bbox():
    control = box(0, 1, 2, 3)

    foo = {"bounds": control.bounds}

    assert object_geometry(foo).equals(control)


def test_object_geometry_key_geometry():
    control = box(0, 1, 2, 3)

    foo = {"geometry": mapping(control)}

    assert object_geometry(foo).equals(control)


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
