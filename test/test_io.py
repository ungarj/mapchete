"""Test Mapchete io module."""

import os

import pytest
from pytest_lazyfixture import lazy_fixture
from shapely.errors import TopologicalError

import mapchete
from mapchete.io import (
    MatchingMethod,
    absolute_path,
    copy,
    get_best_zoom_level,
    path_exists,
    rasterio_open,
    read_json,
    tile_to_zoom_level,
    tiles_exist,
)
from mapchete.tile import BufferedTilePyramid

SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


def test_best_zoom_level(dummy1_tif):
    """Test best zoom level determination."""
    assert get_best_zoom_level(dummy1_tif, "geodetic")
    assert get_best_zoom_level(dummy1_tif, "mercator")


@pytest.mark.integration
def test_s3_path_exists(raster_4band_s3):
    assert path_exists(raster_4band_s3)


@pytest.mark.integration
def test_remote_path_exists(http_raster):
    assert path_exists(http_raster)
    assert not path_exists(http_raster / "non_existing.tif")


def test_absolute_path():
    assert (
        str(absolute_path(path="file.tif", base_dir="/mnt/data"))
        == "/mnt/data/file.tif"
    )
    assert (
        str(absolute_path(path="/mnt/data/file.tif", base_dir="/mnt/other_data"))
        == "/mnt/data/file.tif"
    )
    with pytest.raises(TypeError):
        absolute_path(path="file.tif", base_dir="no/abs/dir")
    assert (
        str(absolute_path(path="https://example.com/file.tif", base_dir="/mnt/data"))
        == "https://example.com/file.tif"
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("s3_metadata_json"),
        lazy_fixture("http_metadata_json"),
    ],
)
def test_read_remote_json(path):
    assert isinstance(read_json(path), dict)


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        lazy_fixture("s3_metadata_json"),
        lazy_fixture("http_metadata_json"),
    ],
)
def test_read_remote_json_errors(path):
    # keep access credentials but invalidate URI
    path = path + "not_here"
    with pytest.raises(FileNotFoundError):  # type: ignore
        read_json(path)


def test_tile_to_zoom_level():
    tp_merc = BufferedTilePyramid("mercator")
    tp_geod = BufferedTilePyramid("geodetic")
    zoom = 9
    col = 0

    # mercator from geodetic
    # at Northern boundary
    assert tile_to_zoom_level(tp_merc.tile(zoom, 0, col), tp_geod) == 9
    assert (
        tile_to_zoom_level(
            tp_merc.tile(zoom, 0, col), tp_geod, matching_method=MatchingMethod.min
        )
        == 12
    )
    # at Equator
    assert (
        tile_to_zoom_level(
            tp_merc.tile(zoom, tp_merc.matrix_height(zoom) // 2, col), tp_geod
        )
        == 9
    )
    assert (
        tile_to_zoom_level(
            tp_merc.tile(zoom, tp_merc.matrix_height(zoom) // 2, col),
            tp_geod,
            matching_method=MatchingMethod.min,
        )
        == 9
    )
    # at Southern boundary
    assert (
        tile_to_zoom_level(
            tp_merc.tile(zoom, tp_merc.matrix_height(zoom) - 1, col), tp_geod
        )
        == 9
    )
    assert (
        tile_to_zoom_level(
            tp_merc.tile(zoom, tp_merc.matrix_height(zoom) - 1, col),
            tp_geod,
            matching_method=MatchingMethod.min,
        )
        == 12
    )
    assert (
        tile_to_zoom_level(
            BufferedTilePyramid("mercator", metatiling=2, pixelbuffer=20).tile(4, 0, 7),
            BufferedTilePyramid("geodetic", metatiling=8, pixelbuffer=20),
            matching_method=MatchingMethod.gdal,
        )
        == 4
    )


def test_tile_to_zoom_level_geodetic_from_mercator():
    tp_merc = BufferedTilePyramid("mercator")
    tp_geod = BufferedTilePyramid("geodetic")
    zoom = 9
    col = 0

    # geodetic from mercator
    # at Northern boundary
    # NOTE: using a newer proj version (8.2.0) will yield different results
    assert tile_to_zoom_level(tp_geod.tile(zoom, 0, col), tp_merc) in [0, 2]
    with pytest.raises(TopologicalError):
        tile_to_zoom_level(
            tp_geod.tile(zoom, 0, col), tp_merc, matching_method=MatchingMethod.min
        )
    # at Equator
    assert (
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) // 2, col), tp_merc
        )
        == 10
    )
    assert (
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) // 2, col),
            tp_merc,
            matching_method=MatchingMethod.min,
        )
        == 10
    )
    # at Southern boundary
    # NOTE: using a newer proj version (8.2.0) will yield different results
    assert tile_to_zoom_level(
        tp_geod.tile(zoom, tp_geod.matrix_height(zoom) - 1, col), tp_merc
    ) in [0, 2]
    with pytest.raises(TopologicalError):
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) - 1, col),
            tp_merc,
            matching_method=MatchingMethod.min,
        )

    # check wrong method
    with pytest.raises(ValueError):
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) - 1, col),
            tp_merc,
            matching_method="invalid_method",  # type: ignore
        )


def test_tiles_exist_local(example_mapchete):
    bounds = (2.0, 0.0, 4.0, 2.0)
    zoom = 10
    with mapchete.open(
        dict(
            example_mapchete.dict,
            pyramid=dict(example_mapchete.dict["pyramid"], metatiling=4),
            output=dict(example_mapchete.dict["output"], metatiling=1),
        ),
        bounds=bounds,
    ) as mp:
        # generate tile directory
        list(mp.execute(zoom=zoom))
        process_tiles = list(mp.config.process_pyramid.tiles_from_bounds(bounds, zoom))
        output_tiles = list(mp.config.output_pyramid.tiles_from_bounds(bounds, zoom))

        # see which files were written and create set for output_tiles and process_tiles
        written_output_tiles = set()
        for rowdir in (example_mapchete.output_path / zoom).ls():
            for file in rowdir.ls():
                zoom, row, col = map(int, file.without_suffix().elements[-3:])
                written_output_tiles.add(mp.config.output_pyramid.tile(zoom, row, col))

        full_process_tiles = set(
            [
                tile
                for tile in process_tiles
                if all(
                    [
                        output_tile in written_output_tiles
                        for output_tile in mp.config.output_pyramid.intersecting(tile)
                    ]
                )
            ]
        )

        # process tiles
        existing = set()
        not_existing = set()
        for tile, exists in tiles_exist(config=mp.config, process_tiles=process_tiles):
            if exists:
                existing.add(tile)
            else:
                not_existing.add(tile)
        assert existing == full_process_tiles
        assert not_existing
        assert set(process_tiles) == existing.union(not_existing)

        # output tiles
        existing = set()
        not_existing = set()
        for tile, exists in tiles_exist(config=mp.config, output_tiles=output_tiles):
            if exists:
                existing.add(tile)
            else:
                not_existing.add(tile)
        assert existing == written_output_tiles
        assert not_existing
        assert set(output_tiles) == existing.union(not_existing)


@pytest.mark.integration
def test_tiles_exist_s3(gtiff_s3):
    bounds = (0, 0, 10, 10)
    # bounds = (3, 1, 4, 2)
    zoom = 5
    with mapchete.open(
        dict(
            gtiff_s3.dict,
            pyramid=dict(gtiff_s3.dict["pyramid"], metatiling=8),
            output=dict(gtiff_s3.dict["output"], metatiling=1),
        ),
        bounds=bounds,
        mode="overwrite",
    ) as mp:
        # generate tile directory
        list(mp.execute(zoom=zoom))
        process_tiles = list(mp.config.process_pyramid.tiles_from_bounds(bounds, zoom))
        output_tiles = list(mp.config.output_pyramid.tiles_from_bounds(bounds, zoom))

        # manually check which tiles exist
        written_output_tiles = set()
        for tt in output_tiles:
            if mp.config.output_reader.tiles_exist(output_tile=tt):  # type: ignore
                written_output_tiles.add(tt)
        full_process_tiles = set(
            [
                tile
                for tile in process_tiles
                if all(
                    [
                        output_tile in written_output_tiles
                        for output_tile in mp.config.output_pyramid.intersecting(tile)
                    ]
                )
            ]
        )

        # process tiles
        existing = set()
        not_existing = set()
        for tile, exists in tiles_exist(
            config=mp.config, process_tiles=process_tiles, multi=4
        ):
            if exists:
                existing.add(tile)
            else:
                not_existing.add(tile)
        assert existing == full_process_tiles
        assert set(process_tiles) == existing.union(not_existing)

        # output tiles
        existing = set()
        not_existing = set()
        for tile, exists in tiles_exist(
            config=mp.config, output_tiles=output_tiles, multi=1
        ):
            if exists:
                existing.add(tile)
            else:
                not_existing.add(tile)
        assert existing == written_output_tiles
        assert set(output_tiles) == existing.union(not_existing)


def test_copy(cleantopo_br_tif, tmpdir):
    out = os.path.join(tmpdir, "copied.tif")

    # copy and verify file is valid
    copy(cleantopo_br_tif, out)
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()

    # try to copy again, catching the IOError
    with pytest.raises(IOError):
        copy(cleantopo_br_tif, out)

    # copy again but overwrite
    copy(cleantopo_br_tif, out, overwrite=True)
    with rasterio_open(out) as src:
        assert not src.read(masked=True).mask.all()


def test_custom_grid_points(custom_grid_points):
    mp = custom_grid_points.process_mp(tile=(3, 1245, 37))
    with mp.open("inp") as points:
        assert points.read()
