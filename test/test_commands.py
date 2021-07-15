import os

import mapchete
from mapchete.commands import cp


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
    cp(
        out_path,
        os.path.join(mp_tmpdir, "all"),
        zoom=5,
        bounds=[169.19251592399996, -90, 180, -80.18582802550002],
    )

    # copy all tiles
    cp(
        out_path,
        os.path.join(mp_tmpdir, "all"),
        zoom=5,
    )

    # copy tiles and subset by area
    cp(out_path, os.path.join(mp_tmpdir, "all"), zoom=5, area=wkt_geom)

    # copy local tiles without using threads
    cp(out_path, os.path.join(mp_tmpdir, "all"), zoom=5, multi=1)


def test_cp_http(mp_tmpdir, http_tiledir):
    # copy tiles and subset by bounds
    cp(http_tiledir, os.path.join(mp_tmpdir, "http"), zoom=1, bounds=[3, 1, 4, 2])
