import json
import pytest
from shapely.geometry import box, shape

from mapchete.io.vector import reproject_geometry
from mapchete.stac import (
    tile_directory_stac_item,
    update_tile_directory_stac_item,
    tile_pyramid_from_item,
    zoom_levels_from_item,
)
from mapchete.tile import BufferedTilePyramid


def test_wkss_geodetic():
    tp = BufferedTilePyramid("geodetic")
    item = tile_directory_stac_item(
        item_id="foo", item_path="foo/bar.json", tile_pyramid=tp, zoom_levels=range(6)
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
    item = tile_directory_stac_item(
        item_id="foo", item_path="foo/bar.json", tile_pyramid=tp, zoom_levels=range(6)
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
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        tile_pyramid=BufferedTilePyramid("geodetic"),
        zoom_levels=range(6),
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
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        tile_pyramid=tp,
        zoom_levels=range(6),
        item_metadata=dict(properties=dict(start_datetime="2021-01-01 00:00:00")),
    )
    assert str(item.datetime) == "2021-01-01 00:00:00"
    assert "custom" in item.properties["tiles:tile_matrix_sets"]


def test_tiled_asset_path():
    # default: create absolute path from item basepath
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        tile_pyramid=BufferedTilePyramid("geodetic"),
        zoom_levels=range(0),
    )
    basepath = item.to_dict()["asset_templates"]["bands"]["href"]
    assert basepath.startswith("foo/")

    # use alternative asset basepath
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        asset_basepath="s3://bar/",
        tile_pyramid=BufferedTilePyramid("geodetic"),
        zoom_levels=range(0),
    )
    basepath = item.to_dict()["asset_templates"]["bands"]["href"]
    assert basepath.startswith("s3://bar/")

    # create relative path
    item = tile_directory_stac_item(
        item_id="foo",
        relative_paths=True,
        tile_pyramid=BufferedTilePyramid("geodetic"),
        zoom_levels=range(0),
    )
    basepath = item.to_dict()["asset_templates"]["bands"]["href"]
    assert basepath.startswith("{TileMatrix}/{TileRow}")


def test_tiled_asset_eo_bands_metadata():
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        tile_pyramid=BufferedTilePyramid("geodetic"),
        zoom_levels=range(6),
        item_metadata={"eo:bands": {"foo": "bar"}},
    )
    assert "eo" in item.to_dict()["stac_extensions"]
    assert "eo:bands" in item.to_dict()["asset_templates"]["bands"]


def test_create_stac_item_errors():
    tp = BufferedTilePyramid("geodetic")
    # no item_id
    with pytest.raises(ValueError):
        tile_directory_stac_item(
            item_path="foo/bar.json",
            tile_pyramid=tp,
            zoom_levels=range(6),
        )

    # no zoom_level
    with pytest.raises(ValueError):
        tile_directory_stac_item(
            item_id="foo",
            item_path="foo/bar.json",
            tile_pyramid=tp,
        )

    # no tile_pyramid
    with pytest.raises(ValueError):
        tile_directory_stac_item(
            item_id="foo",
            item_path="foo/bar.json",
            zoom_levels=range(6),
        )

    # no item_path or asset_basepath
    with pytest.raises(ValueError):
        tile_directory_stac_item(
            item_id="foo",
            tile_pyramid=tp,
            zoom_levels=range(6),
        )


def test_update_stac():
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        tile_pyramid=BufferedTilePyramid("geodetic"),
        zoom_levels=range(6),
    )
    assert (
        len(item.properties["tiles:tile_matrix_sets"]["WorldCRS84Quad"]["tileMatrix"])
        == 6
    )
    new_item = update_tile_directory_stac_item(item=item, zoom_levels=[6, 7])
    assert (
        len(
            new_item.properties["tiles:tile_matrix_sets"]["WorldCRS84Quad"][
                "tileMatrix"
            ]
        )
        == 8
    )


def test_update_stac_errors():
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        tile_pyramid=BufferedTilePyramid("geodetic"),
        zoom_levels=range(6),
    )
    with pytest.raises(TypeError):
        update_tile_directory_stac_item(
            item=item, tile_pyramid=BufferedTilePyramid("geodetic", metatiling=4)
        )


def test_tile_pyramid_from_item():
    for metatiling in [1, 2, 4, 8, 16, 64]:
        tp = BufferedTilePyramid("geodetic", metatiling=metatiling)
        item = tile_directory_stac_item(
            item_id="foo",
            item_path="foo/bar.json",
            tile_pyramid=tp,
            zoom_levels=range(6),
        )
        assert tp == tile_pyramid_from_item(item)


def test_tile_pyramid_from_item_no_tilesets_error():
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        tile_pyramid=BufferedTilePyramid("geodetic"),
        zoom_levels=range(6),
    )
    # remove properties including tiled assets information
    item.properties = {}

    with pytest.raises(AttributeError):
        tile_pyramid_from_item(item)


def test_tile_pyramid_from_item_no_known_wkss_error(custom_grid_json):
    with open(custom_grid_json) as src:
        grid_def = json.loads(src.read())
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        tile_pyramid=BufferedTilePyramid(**grid_def),
        zoom_levels=range(3),
    )

    with pytest.raises(ValueError):
        tile_pyramid_from_item(item)


def test_zoom_levels_from_item_errors():
    item = tile_directory_stac_item(
        item_id="foo",
        item_path="foo/bar.json",
        tile_pyramid=BufferedTilePyramid("geodetic"),
        zoom_levels=range(6),
    )
    # remove properties including tiled assets information
    item.properties = {}
    with pytest.raises(AttributeError):
        zoom_levels_from_item(item)
