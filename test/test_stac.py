import pystac
import pytest
from shapely.geometry import box, shape

from mapchete.io.vector import reproject_geometry
from mapchete.stac import create_stac_item
from mapchete.tile import BufferedTilePyramid


def test_wkss_geodetic():
    tp = BufferedTilePyramid("geodetic")
    item = create_stac_item(
        item_id="foo", item_basepath="foo/bar", tile_pyramid=tp, max_zoom=5
    )
    assert item.id == "foo"
    assert shape(item.geometry).difference(box(*tp.bounds)).is_empty
    assert item.bbox == list(tp.bounds)
    assert item.datetime
    assert "tiled-assets" in item.stac_extensions
    assert "bands" in item.extra_fields["asset_templates"]
    assert "tiles:tile_matrix_links" in item.properties
    assert "tiles:tile_matrix_sets" in item.properties
    assert "WorldCRS84Quad" in item.properties["tiles:tile_matrix_sets"]


def test_wkss_mercator():
    tp = BufferedTilePyramid("mercator")
    item = create_stac_item(
        item_id="foo", item_basepath="foo/bar", tile_pyramid=tp, max_zoom=5
    )
    assert item.id == "foo"
    item_geometry = reproject_geometry(
        shape(item.geometry), src_crs="EPSG:4326", dst_crs=tp.crs
    )
    assert item_geometry.difference(box(*tp.bounds)).is_empty
    assert item.datetime
    assert "tiled-assets" in item.stac_extensions
    assert "bands" in item.extra_fields["asset_templates"]
    assert "tiles:tile_matrix_links" in item.properties
    assert "tiles:tile_matrix_sets" in item.properties
    assert "WebMercatorQuad" in item.properties["tiles:tile_matrix_sets"]


def test_custom_datetime():
    tp = BufferedTilePyramid("geodetic")
    item = create_stac_item(
        item_id="foo",
        item_basepath="foo/bar",
        tile_pyramid=tp,
        max_zoom=5,
        item_metadata=dict(properties=dict(start_datetime="2021-01-01 00:00:00")),
    )
    assert str(item.datetime) == "2021-01-01 00:00:00"


def test_custom_tilematrix():
    tp = BufferedTilePyramid(
        grid=dict(
            shape=[117, 9],
            bounds=[145980, 0, 883260, 9584640.0],
            is_global=False,
            epsg=32630,
        ),
        metatiling=4,
    )
    item = create_stac_item(
        item_id="foo",
        item_basepath="foo/bar",
        tile_pyramid=tp,
        max_zoom=5,
        item_metadata=dict(properties=dict(start_datetime="2021-01-01 00:00:00")),
    )
    assert str(item.datetime) == "2021-01-01 00:00:00"
    assert "custom" in item.properties["tiles:tile_matrix_sets"]
