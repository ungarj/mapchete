import os

import mapchete
from mapchete.commands import cp, rm


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
