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
    bounds_intersect,
    convert_vector,
    fiona_open,
    object_bounds,
    object_crs,
    read_vector_window,
    write_vector_window,
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


def test_bounds_intersect():
    b1 = (0, 0, 2, 2)
    b2 = (1, 1, 3, 3)
    b3 = (0, 1, 2, 3)
    b4 = (1, 0, 3, 2)
    b5 = (0, -1, 2, 1)
    b6 = (0, -2, 2, 0)
    b7 = (0, 0, 2, 2)
    b8 = (0.5, 0.5, 1.5, 1.5)

    assert bounds_intersect(b1, b1)
    assert bounds_intersect(b1, b2)
    assert bounds_intersect(b1, b3)
    assert bounds_intersect(b1, b4)
    assert bounds_intersect(b1, b5)
    assert bounds_intersect(b1, b6)
    assert bounds_intersect(b1, b7)
    assert bounds_intersect(b1, b8)


@pytest.mark.parametrize(
    "intersecting",
    [
        (-121.2451171875, 75.7177734375, -120.849609375, 76.025390625),
        (-119.3994140625, 75.498046875, -117.4658203125, 76.11328125),
        (-122.958984375, 75.8056640625, -115.400390625, 77.5634765625),
        (-125.859375, 71.0595703125, -115.3125, 74.5751953125),
        (-115.048828125, 77.6953125, -113.5986328125, 78.0908203125),
        (-114.8291015625, 76.6845703125, -113.4228515625, 76.904296875),
        (-113.3349609375, 77.34375, -109.5556640625, 78.134765625),
        (-109.423828125, 67.8955078125, -109.2041015625, 68.0712890625),
        (-113.3349609375, 78.2666015625, -109.2041015625, 78.75),
        (-108.017578125, 73.5205078125, -107.578125, 73.6083984375),
        (-117.685546875, 74.3994140625, -105.380859375, 76.8603515625),
        (-105.1171875, 68.37890625, -104.4140625, 68.5986328125),
        (-106.083984375, 77.080078125, -103.974609375, 77.783203125),
        (-104.8974609375, 75.0146484375, -103.5791015625, 75.4541015625),
        (-104.677734375, 76.3330078125, -103.0078125, 76.6845703125),
        (-102.2607421875, 68.5546875, -101.689453125, 68.818359375),
        (-102.5244140625, 77.6953125, -100.9423828125, 77.9150390625),
        (-119.1357421875, 68.466796875, -100.8544921875, 73.7841796875),
        (-101.6015625, 76.552734375, -100.4150390625, 76.7724609375),
        (-100.634765625, 70.4443359375, -100.1953125, 70.6640625),
        (-100.5908203125, 68.6865234375, -99.9755859375, 69.169921875),
        (-105.64453125, 77.783203125, -98.9208984375, 79.4091796875),
        (-100.1513671875, 79.6728515625, -98.6572265625, 80.15625),
        (-99.404296875, 73.828125, -97.6025390625, 74.1357421875),
        (-104.4580078125, 74.970703125, -97.3388671875, 76.6845703125),
        (-97.7783203125, 74.443359375, -97.2509765625, 74.619140625),
        (-97.119140625, 72.9052734375, -96.5478515625, 73.212890625),
        (-96.9873046875, 75.3662109375, -96.416015625, 75.5859375),
        (-102.744140625, 71.279296875, -96.2841796875, 73.9599609375),
        (-96.591796875, 69.345703125, -96.064453125, 69.5654296875),
        (-96.3720703125, 75.4541015625, -95.9326171875, 75.673828125),
        (-95.9765625, 69.3017578125, -95.361328125, 69.6533203125),
        (-99.5361328125, 68.466796875, -95.2294921875, 69.9169921875),
        (-95.7568359375, 74.4873046875, -95.2294921875, 74.6630859375),
        (-98.3935546875, 77.783203125, -94.833984375, 78.837890625),
        (-94.658203125, 78.1787109375, -94.306640625, 78.2666015625),
        (-94.8779296875, 75.76171875, -94.2626953125, 75.9814453125),
        (-96.6357421875, 74.619140625, -93.3837890625, 75.6298828125),
        (-96.2841796875, 77.431640625, -93.1201171875, 77.8271484375),
        (-91.23046875, 77.1240234375, -90.703125, 77.255859375),
        (-90.0, 68.5986328125, -89.9560546875, 68.642578125),
        (-90.0439453125, 68.6865234375, -89.9560546875, 68.818359375),
        (-90.087890625, 71.8505859375, -89.9560546875, 72.0703125),
        (-96.8994140625, 74.53125, -89.9560546875, 77.255859375),
        (-90.5712890625, 76.46484375, -89.9560546875, 76.8603515625),
        (-91.1865234375, 77.2119140625, -89.9560546875, 77.6513671875),
        (-96.7236328125, 78.134765625, -89.9560546875, 81.38671875),
        (-168.134765625, 13.6669921875, -89.9560546875, 74.1796875),
    ],
)
def test_bounds_intersect_custom(intersecting):
    assert bounds_intersect(intersecting, (-135.0, 60, -90, 80))


def test_bounds_not_intersect():
    b1 = (-3, -3, -2, -2)
    b2 = (1, 1, 3, 3)
    b3 = (0, 1, 2, 3)
    b4 = (1, 0, 3, 2)
    b5 = (0, -1, 2, 1)
    b6 = (0, -2, 2, 0)
    b7 = (0, 0, 2, 2)
    b8 = (0.5, 0.5, 1.5, 1.5)

    assert not bounds_intersect(b1, b2)
    assert not bounds_intersect(b1, b3)
    assert not bounds_intersect(b1, b4)
    assert not bounds_intersect(b1, b5)
    assert not bounds_intersect(b1, b6)
    assert not bounds_intersect(b1, b8)
    assert not bounds_intersect(b1, b7)


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
