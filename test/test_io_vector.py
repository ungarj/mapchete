import pytest
import fiona
from fiona.errors import DriverError
import os
from rasterio.crs import CRS
from shapely import wkt
from shapely.errors import TopologicalError
from shapely.geometry import shape, box, Polygon, MultiPolygon, LineString, mapping

from mapchete.config import MapcheteConfig
from mapchete.errors import GeometryTypeError, MapcheteIOError
from mapchete.io.vector import (
    read_vector_window,
    reproject_geometry,
    clean_geometry_type,
    segmentize_geometry,
    write_vector_window,
    _repair,
    convert_vector,
    IndexedFeatures,
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

    # no bounds
    feature = {"properties": {"foo": "bar"}, "id": 0}
    with pytest.raises(TypeError):
        IndexedFeatures([feature])
