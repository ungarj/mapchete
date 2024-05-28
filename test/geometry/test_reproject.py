import pytest
from fiona.crs import CRS  # type: ignore
from shapely import wkt
from shapely.geometry import Point, Polygon, box, shape

from mapchete.geometry import reproject_geometry
from mapchete.io import fiona_open
from mapchete.tile import BufferedTilePyramid


def test_reproject_geometry(landpoly):
    """Reproject geometry."""
    with fiona_open(str(landpoly), "r") as src:
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


@pytest.mark.skip(reason="antimeridian cutting does not work")
def test_reproject_geometry_over_antimeridian():
    tp = BufferedTilePyramid("mercator", pixelbuffer=96, metatiling=16)
    tile = tp.tile(5, 0, 0)

    # reproject to lat/lon
    tile_4326 = reproject_geometry(tile.bbox, src_crs=tile.crs, dst_crs="EPSG:4326")

    # this point should lie within tile bounds
    point = Point(-90, 45)
    assert point.within(tile_4326)

    # reproject again and make sure it is the same geometry as the original one
    tile_4326_3857 = reproject_geometry(
        tile_4326, src_crs="EPSG:4326", dst_crs="EPSG:3857"
    )
    assert tile.bbox == tile_4326_3857
