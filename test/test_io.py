"""Test Mapchete io module."""

import pytest
import rasterio
import os
from shapely.errors import TopologicalError

import mapchete
from mapchete.io import (
    get_best_zoom_level,
    path_exists,
    absolute_path,
    read_json,
    tile_to_zoom_level,
    tiles_exist,
    copy,
)
from mapchete.tile import BufferedTilePyramid


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


def test_best_zoom_level(dummy1_tif):
    """Test best zoom level determination."""
    assert get_best_zoom_level(dummy1_tif, "geodetic")
    assert get_best_zoom_level(dummy1_tif, "mercator")


@pytest.mark.remote
def test_s3_path_exists(s2_band_remote):
    assert path_exists(s2_band_remote)


@pytest.mark.remote
def test_remote_path_exists(http_raster):
    assert path_exists(http_raster)
    assert not path_exists("http://ungarj.github.io/invalid_file.tif")


def test_absolute_path():
    assert absolute_path(path="file.tif", base_dir="/mnt/data") == "/mnt/data/file.tif"
    assert (
        absolute_path(path="/mnt/data/file.tif", base_dir="/mnt/other_data")
        == "/mnt/data/file.tif"
    )
    with pytest.raises(TypeError):
        absolute_path(path="file.tif", base_dir=None)
    with pytest.raises(TypeError):
        absolute_path(path="file.tif", base_dir="no/abs/dir")
    assert (
        absolute_path(path="https://file.tif", base_dir="/mnt/data")
        == "https://file.tif"
    )


@pytest.mark.remote
def test_read_remote_json(s3_metadata_json, http_metadata_json):
    assert isinstance(read_json(s3_metadata_json), dict)
    assert isinstance(read_json(http_metadata_json), dict)
    with pytest.raises(FileNotFoundError):
        read_json("s3://mapchete-test/invalid_metadata.json")
    with pytest.raises(FileNotFoundError):
        read_json(
            "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/invalid_metadata.json"
        )


def test_tile_to_zoom_level():
    tp_merc = BufferedTilePyramid("mercator")
    tp_geod = BufferedTilePyramid("geodetic")
    zoom = 9
    col = 0

    # mercator from geodetic
    # at Northern boundary
    assert tile_to_zoom_level(tp_merc.tile(zoom, 0, col), tp_geod) == 9
    assert (
        tile_to_zoom_level(tp_merc.tile(zoom, 0, col), tp_geod, matching_method="min")
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
            matching_method="min",
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
            matching_method="min",
        )
        == 12
    )
    assert (
        tile_to_zoom_level(
            BufferedTilePyramid("mercator", metatiling=2, pixelbuffer=20).tile(4, 0, 7),
            BufferedTilePyramid("geodetic", metatiling=8, pixelbuffer=20),
            matching_method="gdal",
        )
        == 4
    )

    # geodetic from mercator
    # at Northern boundary
    # NOTE: using a newer proj version (8.2.0) will yield different results
    assert tile_to_zoom_level(tp_geod.tile(zoom, 0, col), tp_merc) in [0, 2]
    with pytest.raises(TopologicalError):
        tile_to_zoom_level(tp_geod.tile(zoom, 0, col), tp_merc, matching_method="min")
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
            matching_method="min",
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
            matching_method="min",
        )

    # check wrong method
    with pytest.raises(ValueError):
        tile_to_zoom_level(
            tp_geod.tile(zoom, tp_geod.matrix_height(zoom) - 1, col),
            tp_merc,
            matching_method="invalid_method",
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
        mp.batch_process(zoom=zoom)
        process_tiles = list(mp.config.process_pyramid.tiles_from_bounds(bounds, zoom))
        output_tiles = list(mp.config.output_pyramid.tiles_from_bounds(bounds, zoom))

        # see which files were written and create set for output_tiles and process_tiles
        out_path = os.path.join(
            SCRIPTDIR, example_mapchete.dict["output"]["path"], str(zoom)
        )
        written_output_tiles = set()
        for root, dirs, files in os.walk(out_path):
            for file in files:
                zoom, row = map(int, root.split("/")[-2:])
                col = int(file.split(".")[0])
                written_output_tiles.add(mp.config.output_pyramid.tile(zoom, row, col))
        written_process_tiles = set(
            [mp.config.process_pyramid.intersecting(t)[0] for t in written_output_tiles]
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
        assert existing == written_process_tiles
        assert not_existing
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
        assert not_existing
        assert set(output_tiles) == existing.union(not_existing)


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
        mp.batch_process(zoom=zoom)
        process_tiles = list(mp.config.process_pyramid.tiles_from_bounds(bounds, zoom))
        output_tiles = list(mp.config.output_pyramid.tiles_from_bounds(bounds, zoom))

        # manually check which tiles exist
        written_output_tiles = set()
        for t in output_tiles:
            if mp.config.output_reader.tiles_exist(output_tile=t):
                written_output_tiles.add(t)
        written_process_tiles = set(
            [mp.config.process_pyramid.intersecting(t)[0] for t in written_output_tiles]
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
        assert existing == written_process_tiles
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
    with rasterio.open(out) as src:
        assert not src.read(masked=True).mask.all()

    # try to copy again, catching the IOError
    with pytest.raises(IOError):
        copy(cleantopo_br_tif, out)

    # copy again but overwrite
    copy(cleantopo_br_tif, out, overwrite=True)
    with rasterio.open(out) as src:
        assert not src.read(masked=True).mask.all()


def test_custom_grid_points(custom_grid_points):
    mp = custom_grid_points.process_mp(tile=(3, 1245, 37))
    with mp.open("inp") as points:
        assert points.read()
