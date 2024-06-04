import pytest
from fiona.crs import CRS  # type: ignore
from pytest_lazyfixture import lazy_fixture
from shapely import wkt
from shapely.geometry import Point, Polygon, box

from mapchete.errors import ReprojectionFailed
from mapchete.geometry import reproject_geometry
from mapchete.geometry.reproject import get_crs_bounds
from mapchete.tile import BufferedTilePyramid


@pytest.mark.parametrize("segmentize", [True, False])
@pytest.mark.parametrize("clip_to_crs_bounds", [True, False])
@pytest.mark.parametrize("validity_check", [True, False])
@pytest.mark.parametrize("antimeridian_cutting", [True, False])
@pytest.mark.parametrize(
    "dst_crs",
    [
        # Spherical Mercator
        CRS.from_epsg(3857),
        # LAEA
        CRS.from_epsg(3035),
        # WGS84
        CRS.from_epsg(4326),
        # some custom polar
        CRS.from_string(
            "+proj=ortho +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
        ),
    ],
)
@pytest.mark.parametrize(
    "geometry",
    [
        lazy_fixture("point"),
        lazy_fixture("multipoint"),
        lazy_fixture("linestring"),
        lazy_fixture("multilinestring"),
        lazy_fixture("polygon"),
        lazy_fixture("multipolygon"),
        lazy_fixture("geometrycollection"),
    ],
)
def test_reproject_geometry(
    geometry,
    dst_crs,
    segmentize,
    clip_to_crs_bounds,
    validity_check,
    antimeridian_cutting,
):
    """Reproject geometry."""
    src_crs = CRS.from_epsg(4326)
    out_geom = reproject_geometry(
        geometry,
        src_crs=src_crs,
        dst_crs=dst_crs,
        segmentize=segmentize,
        clip_to_crs_bounds=clip_to_crs_bounds,
        validity_check=validity_check,
        antimeridian_cutting=antimeridian_cutting,
    )
    assert out_geom.is_valid


@pytest.mark.parametrize("enable_partial_reprojection", [True, False])
def test_reproject_from_crs_wkt(enable_partial_reprojection):
    geom = wkt.loads(
        "POLYGON ((6453888 -6453888, 6453888 6453888, -6453888 6453888, -6453888 -6453888, 6453888 -6453888))"
    )
    src_crs = 'PROJCS["unknown",GEOGCS["unknown",DATUM["Unknown based on WGS84 ellipsoid",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]]],PROJECTION["Orthographic"],PARAMETER["latitude_of_origin",-90],PARAMETER["central_meridian",0],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'
    dst_crs = "EPSG:4326"
    with pytest.raises(ReprojectionFailed):
        reproject_geometry(
            geom,
            src_crs,
            dst_crs,
            fiona_env={"OGR_ENABLE_PARTIAL_REPROJECTION": enable_partial_reprojection},
        ).is_valid


def test_reproject_geometry_latlon2mercator():
    # WGS84 bounds to Spherical Mercator
    big_box = box(-180, -90, 180, 90)
    assert reproject_geometry(
        big_box, CRS.from_epsg(4326), CRS.from_epsg(3857)
    ).is_valid

    # WGS84 bounds to Spherical Mercator raising clip error
    with pytest.raises(RuntimeError):
        reproject_geometry(
            big_box, CRS.from_epsg(4326), CRS.from_epsg(3857), error_on_clip=True
        )


def test_reproject_geometry_empty_geom():
    # empty geometry
    assert reproject_geometry(
        Polygon(), CRS.from_epsg(4326), CRS.from_epsg(3857)
    ).is_empty
    assert reproject_geometry(
        Polygon(), CRS.from_epsg(4326), CRS.from_epsg(4326)
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


@pytest.mark.parametrize(
    "crs", [CRS.from_epsg(4326), CRS.from_epsg(3857), CRS.from_epsg(32633)]
)
def test_get_crs_bounds(crs):
    assert get_crs_bounds(crs)


def test_get_crs_bounds_custom():
    with pytest.raises(ValueError):
        get_crs_bounds(
            CRS.from_string(
                "+proj=ortho +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
            )
        )
