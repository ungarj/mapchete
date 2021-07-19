import os
import rasterio
from tilematrix import TilePyramid

import mapchete
from mapchete.commands import convert, cp, execute, index, rm


SCRIPTDIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(SCRIPTDIR, "testdata")


def test_cp(mp_tmpdir, cleantopo_br, wkt_geom):
    # generate TileDirectory
    with mapchete.open(
        cleantopo_br.path, bounds=[169.19251592399996, -90, 180, -80.18582802550002]
    ) as mp:
        mp.batch_process(zoom=5)
    out_path = os.path.join(TESTDATA_DIR, cleantopo_br.dict["output"]["path"])

    # copy tiles and subset by bounds
    tiles = cp(
        out_path,
        os.path.join(mp_tmpdir, "bounds"),
        zoom=5,
        bounds=[169.19251592399996, -90, 180, -80.18582802550002],
    )
    assert len(tiles)

    # copy all tiles
    tiles = cp(
        out_path,
        os.path.join(mp_tmpdir, "all"),
        zoom=5,
    )
    assert len(tiles)

    # copy tiles and subset by area
    tiles = cp(out_path, os.path.join(mp_tmpdir, "area"), zoom=5, area=wkt_geom)
    assert len(tiles)

    # copy local tiles without using threads
    tiles = cp(out_path, os.path.join(mp_tmpdir, "nothreads"), zoom=5, multi=1)
    assert len(tiles)


def test_cp_http(mp_tmpdir, http_tiledir):
    # copy tiles and subset by bounds
    tiles = cp(
        http_tiledir, os.path.join(mp_tmpdir, "http"), zoom=1, bounds=[3, 1, 4, 2]
    )
    assert len(tiles)


def test_rm(mp_tmpdir, cleantopo_br):
    # generate TileDirectory
    with mapchete.open(
        cleantopo_br.path, bounds=[169.19251592399996, -90, 180, -80.18582802550002]
    ) as mp:
        mp.batch_process(zoom=5)
    out_path = os.path.join(TESTDATA_DIR, cleantopo_br.dict["output"]["path"])

    # remove tiles
    tiles = rm(out_path, zoom=5)
    assert len(tiles) > 0

    # remove tiles but this time they should already have been removed
    tiles = rm(out_path, zoom=5)
    assert len(tiles) == 0


def test_execute(mp_tmpdir, cleantopo_br, cleantopo_br_tif):
    zoom = 5
    config = cleantopo_br.dict
    config["pyramid"].update(metatiling=1)
    tp = TilePyramid("geodetic")
    tiles = list(tp.tiles_from_bounds(rasterio.open(cleantopo_br_tif).bounds, zoom))
    job = execute(config, zoom=zoom)
    assert len(tiles) == len(job)
    with mapchete.open(config) as mp:
        for t in tiles:
            with rasterio.open(mp.config.output.get_path(t)) as src:
                assert not src.read(masked=True).mask.all()


def test_execute_tile(mp_tmpdir, cleantopo_br):
    tile = (5, 30, 63)

    config = cleantopo_br.dict
    config["pyramid"].update(metatiling=1)
    job = execute(config, tile=tile)

    assert len(job) == 1

    with mapchete.open(config) as mp:
        with rasterio.open(
            mp.config.output.get_path(mp.config.output_pyramid.tile(*tile))
        ) as src:
            assert not src.read(masked=True).mask.all()


def test_execute_vrt(mp_tmpdir, cleantopo_br):
    """Using debug output."""
    execute(cleantopo_br.path, zoom=5, vrt=True)
    with mapchete.open(cleantopo_br.dict) as mp:
        vrt_path = os.path.join(mp.config.output.path, "5.vrt")
        with rasterio.open(vrt_path) as src:
            assert src.read().any()

    # run again, this time with custom output directory
    execute(cleantopo_br.path, zoom=5, vrt=True, idx_out_dir=mp_tmpdir)
    with mapchete.open(cleantopo_br.dict) as mp:
        vrt_path = os.path.join(mp_tmpdir, "5.vrt")
        with rasterio.open(vrt_path) as src:
            assert src.read().any()

    # run with single tile
    execute(cleantopo_br.path, tile=(5, 3, 7), vrt=True)

    # no new entries
    execute(cleantopo_br.path, tile=(5, 0, 0), vrt=True)
