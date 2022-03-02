import pytest
import fiona
from fiona.errors import DriverError
import os
from rasterio.crs import CRS
from shapely import wkt
from shapely.errors import TopologicalError
from shapely.geometry import shape, box, Polygon, MultiPolygon, LineString, mapping

from mapchete.config import MapcheteConfig
from mapchete.errors import GeometryTypeError, MapcheteIOError, ReprojectionFailed
from mapchete.io.vector import (
    read_vector_window,
    reproject_geometry,
    clean_geometry_type,
    segmentize_geometry,
    write_vector_window,
    _repair,
    convert_vector,
    IndexedFeatures,
    bounds_intersect,
)
from mapchete.tile import BufferedTilePyramid


def test_read_vector_window(geojson, landpoly_3857):
    """Read vector data from read_vector_window."""
    zoom = 4
    config = MapcheteConfig(geojson.dict)
    vectorfile = config.params_at_zoom(zoom)["input"]["file1"]
    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = tile_pyramid.tiles_from_geom(vectorfile.bbox(), zoom)
    feature_count = 0
    for tile in tiles:
        for feature in read_vector_window(vectorfile.path, tile):
            assert "properties" in feature
            assert shape(feature["geometry"]).is_valid
            feature_count += 1
    assert feature_count
    # into different CRS
    raw_config = geojson.dict
    raw_config["input"].update(file1=landpoly_3857)
    config = MapcheteConfig(raw_config)
    vectorfile = config.params_at_zoom(zoom)["input"]["file1"]
    pixelbuffer = 5
    tile_pyramid = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles = tile_pyramid.tiles_from_geom(vectorfile.bbox(), zoom)
    feature_count = 0
    for tile in tiles:
        for feature in read_vector_window(vectorfile.path, tile):
            assert "properties" in feature
            assert shape(feature["geometry"]).is_valid
            feature_count += 1
    assert feature_count


def test_read_vector_window_errors(invalid_geojson):
    with pytest.raises(FileNotFoundError):
        read_vector_window(
            "invalid_path", BufferedTilePyramid("geodetic").tile(0, 0, 0)
        )
    with pytest.raises(MapcheteIOError):
        read_vector_window(
            invalid_geojson, BufferedTilePyramid("geodetic").tile(0, 0, 0)
        )


def test_reproject_geometry(landpoly):
    """Reproject geometry."""
    with fiona.open(landpoly, "r") as src:
        for feature in src:

            # WGS84 to Spherical Mercator
            out_geom = reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs), CRS().from_epsg(3857)
            )
            assert out_geom.is_valid

            # WGS84 to LAEA
            out_geom = reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs), CRS().from_epsg(3035)
            )
            assert out_geom.is_valid

            # WGS84 to WGS84
            out_geom = reproject_geometry(
                shape(feature["geometry"]), CRS(src.crs), CRS().from_epsg(4326)
            )
            assert out_geom.is_valid


def test_reproject_geometry_latlon2mercator():
    # WGS84 bounds to Spherical Mercator
    big_box = box(-180, -90, 180, 90)
    reproject_geometry(big_box, CRS().from_epsg(4326), CRS().from_epsg(3857))

    # WGS84 bounds to Spherical Mercator raising clip error
    with pytest.raises(RuntimeError):
        reproject_geometry(
            big_box, CRS().from_epsg(4326), CRS().from_epsg(3857), error_on_clip=True
        )
    outside_box = box(-180, 87, 180, 90)
    assert reproject_geometry(
        outside_box,
        CRS().from_epsg(4326),
        CRS().from_epsg(3857),
    ).is_valid


def test_reproject_geometry_empty_geom():
    # empty geometry
    assert reproject_geometry(
        Polygon(), CRS().from_epsg(4326), CRS().from_epsg(3857)
    ).is_empty
    assert reproject_geometry(
        Polygon(), CRS().from_epsg(4326), CRS().from_epsg(4326)
    ).is_empty


def test_reproject_geometry_latlon2mercator_epsg():
    # CRS parameter
    big_box = box(-180, -90, 180, 90)
    assert reproject_geometry(big_box, 4326, 3857) == reproject_geometry(
        big_box, "4326", "3857"
    )
    with pytest.raises(TypeError):
        reproject_geometry(big_box, 1.0, 1.0)


def test_reproject_geometry_clip_crs_bounds_epsg():
    bbox = wkt.loads(
        "Polygon ((6.05416952699480682 49.79497943046440867, 6.04100166381764581 50.01055350300158864, 5.70657677854139056 50.00153486687963778, 5.72122668311700089 49.78602894452072292, 6.05416952699480682 49.79497943046440867))"
    )
    dst_crs = "EPSG:32632"

    # reproject to UTM
    bbox_utm = reproject_geometry(bbox, 4326, dst_crs, clip_to_crs_bounds=True)
    assert bbox_utm.is_valid
    assert bbox_utm.area
    # revert to WGS84 and test geometry clipping
    bbox_wgs84 = reproject_geometry(bbox_utm, dst_crs, 4326)
    assert bbox_wgs84.area < bbox.area

    # reproject to UTM but don't clip
    bbox_utm = reproject_geometry(bbox, 4326, dst_crs, clip_to_crs_bounds=False)
    assert bbox_utm.is_valid
    assert bbox_utm.area
    # make sure geometry was not clipped
    bbox_wgs84 = reproject_geometry(bbox_utm, dst_crs, 4326)
    assert bbox_wgs84.intersects(bbox)
    assert (
        bbox_wgs84.intersection(bbox).area
        == pytest.approx(bbox_wgs84.area)
        == pytest.approx(bbox.area)
    )

    # reproject to UTM don't clip but segmentize
    bbox_utm = reproject_geometry(
        bbox, 4326, dst_crs, clip_to_crs_bounds=False, segmentize=True
    )
    assert bbox_utm.is_valid
    assert bbox_utm.area
    # make sure geometry was not clipped
    bbox_wgs84 = reproject_geometry(bbox_utm, dst_crs, 4326)
    assert bbox_wgs84.intersects(bbox)
    assert (
        bbox_wgs84.intersection(bbox).area
        == pytest.approx(bbox_wgs84.area)
        == pytest.approx(bbox.area)
    )


def test_reproject_geometry_clip_crs_bounds_proj():
    bbox = wkt.loads(
        "Polygon ((6.05416952699480682 49.79497943046440867, 6.04100166381764581 50.01055350300158864, 5.70657677854139056 50.00153486687963778, 5.72122668311700089 49.78602894452072292, 6.05416952699480682 49.79497943046440867))"
    )
    dst_crs = "+proj=utm +zone=32 +datum=WGS84 +units=m +no_defs"

    # reproject to UTM
    bbox_utm = reproject_geometry(bbox, 4326, dst_crs, clip_to_crs_bounds=True)
    assert bbox_utm.is_valid
    assert bbox_utm.area
    # revert to WGS84 and test geometry clipping
    bbox_wgs84 = reproject_geometry(bbox_utm, dst_crs, 4326)
    # NOTE: on some proj versions (TBD), pyproj cannot detect the CRS bounds of a CRS passed on by a proj string
    # assert bbox_wgs84.area < bbox.area

    # reproject to UTM but don't clip
    bbox_utm = reproject_geometry(bbox, 4326, dst_crs, clip_to_crs_bounds=False)
    assert bbox_utm.is_valid
    assert bbox_utm.area
    # make sure geometry was not clipped
    bbox_wgs84 = reproject_geometry(bbox_utm, dst_crs, 4326)
    assert bbox_wgs84.intersects(bbox)
    assert (
        bbox_wgs84.intersection(bbox).area
        == pytest.approx(bbox_wgs84.area)
        == pytest.approx(bbox.area)
    )

    # reproject to UTM don't clip but segmentize
    bbox_utm = reproject_geometry(
        bbox, 4326, dst_crs, clip_to_crs_bounds=False, segmentize=True
    )
    assert bbox_utm.is_valid
    assert bbox_utm.area
    # make sure geometry was not clipped
    bbox_wgs84 = reproject_geometry(bbox_utm, dst_crs, 4326)
    assert bbox_wgs84.intersects(bbox)
    assert (
        bbox_wgs84.intersection(bbox).area
        == pytest.approx(bbox_wgs84.area)
        == pytest.approx(bbox.area)
    )


def test_repair_geometry():
    # invalid LineString
    l = LineString([(0, 0), (0, 0), (0, 0)])
    with pytest.raises(TopologicalError):
        _repair(l)


def test_write_vector_window_errors(landpoly):
    with fiona.open(landpoly) as src:
        feature = next(iter(src))
    with pytest.raises((DriverError, ValueError, TypeError)):
        write_vector_window(
            in_data=["invalid", feature],
            out_tile=BufferedTilePyramid("geodetic").tile(0, 0, 0),
            out_path="/invalid_path",
            out_schema=dict(geometry="Polygon", properties=dict()),
        )


def test_segmentize_geometry():
    """Segmentize function."""
    # Polygon
    polygon = box(-18, -9, 18, 9)
    out = segmentize_geometry(polygon, 1)
    assert out.is_valid
    # wrong type
    with pytest.raises(TypeError):
        segmentize_geometry(polygon.centroid, 1)


def test_clean_geometry_type(geometrycollection):
    """Filter and break up geometries."""
    polygon = box(-18, -9, 18, 9)
    # invalid type
    with pytest.raises(TypeError):
        clean_geometry_type(polygon, "invalid_type")

    # don't return geometry
    with pytest.raises(GeometryTypeError):
        clean_geometry_type(polygon, "LineString", raise_exception=True)

    # return geometry as is
    assert clean_geometry_type(polygon, "Polygon").geom_type == "Polygon"
    assert clean_geometry_type(polygon, "MultiPolygon").geom_type == "Polygon"

    # don't allow multipart geometries
    with pytest.raises(GeometryTypeError):
        clean_geometry_type(
            MultiPolygon([polygon]),
            "Polygon",
            allow_multipart=False,
            raise_exception=True,
        )

    # multipolygons from geometrycollection
    result = clean_geometry_type(
        geometrycollection, "Polygon", allow_multipart=True, raise_exception=False
    )
    assert result.geom_type == "MultiPolygon"
    assert not result.is_empty

    # polygons from geometrycollection
    result = clean_geometry_type(
        geometrycollection, "Polygon", allow_multipart=False, raise_exception=False
    )
    assert result.geom_type == "GeometryCollection"
    assert result.is_empty


def test_convert_vector_copy(aoi_br_geojson, tmpdir):
    out = os.path.join(tmpdir, "copied.geojson")

    # copy
    convert_vector(aoi_br_geojson, out)
    with fiona.open(out) as src:
        assert list(iter(src))

    # raise error if output exists
    with pytest.raises(IOError):
        convert_vector(aoi_br_geojson, out, exists_ok=False)

    # do nothing if output exists
    convert_vector(aoi_br_geojson, out)
    with fiona.open(out) as src:
        assert list(iter(src))


def test_convert_vector_overwrite(aoi_br_geojson, tmpdir):
    out = os.path.join(tmpdir, "copied.geojson")

    # write an invalid file
    with open(out, "w") as dst:
        dst.write("invalid")

    # overwrite
    convert_vector(aoi_br_geojson, out, overwrite=True)
    with fiona.open(out) as src:
        assert list(iter(src))


def test_convert_vector_other_format_copy(aoi_br_geojson, tmpdir):
    out = os.path.join(tmpdir, "copied.gpkg")

    convert_vector(aoi_br_geojson, out, driver="GPKG")
    with fiona.open(out) as src:
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
    with fiona.open(out) as src:
        assert list(iter(src))


def test_indexed_features(landpoly):
    with fiona.open(landpoly) as src:
        some_id = next(iter(src))["id"]
        features = IndexedFeatures(src)

    assert features[some_id]
    with pytest.raises(KeyError):
        features["invalid_key"]

    assert features.items()
    assert features.keys()

    for f in features:
        assert isinstance(f, dict)
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
    with pytest.raises(TypeError):
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

    intersecting = [
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
    ]
    for b in intersecting:
        assert bounds_intersect(b, (-135.0, 60, -90, 80))


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
    with fiona.open(landpoly) as src:
        features = list(src)
        idx = IndexedFeatures(features)
        fake_idx = IndexedFeatures(features, index=None)
    bounds = (-135.0, 60, -90, 80)
    assert len(idx.filter(bounds)) == len(fake_idx.filter(bounds))


def test_indexed_features_polygon(aoi_br_geojson):
    with fiona.open(aoi_br_geojson) as src:
        index = IndexedFeatures(src)
    tp = BufferedTilePyramid("geodetic")
    for tile in tp.tiles_from_bounds(bounds=index.bounds, zoom=5):
        assert len(index.filter(tile.bounds)) == 1


def test_reproject_from_crs_wkt():
    geom = wkt.loads(
        "POLYGON ((6453888 -6453888, 6453888 6453888, -6453888 6453888, -6453888 -6453888, 6453888 -6453888))"
    )
    src_crs = 'PROJCS["unknown",GEOGCS["unknown",DATUM["Unknown based on WGS84 ellipsoid",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]]],PROJECTION["Orthographic"],PARAMETER["latitude_of_origin",-90],PARAMETER["central_meridian",0],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'
    dst_crs = "EPSG:4326"
    with pytest.raises(ReprojectionFailed):
        reproject_geometry(geom, src_crs, dst_crs).is_valid
