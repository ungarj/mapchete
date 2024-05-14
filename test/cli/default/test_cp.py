from test.cli.default import run_cli

import pytest


def test_cp(mp_tmpdir, cleantopo_br, wkt_geom, testdata_dir):
    """Using debug output."""
    # generate TileDirectory
    run_cli(
        [
            "execute",
            cleantopo_br.path,
            "-z",
            "5",
            "-b",
            "169.19251592399996",
            "-90",
            "180",
            "-80.18582802550002",
            "--concurrency",
            "none",
        ]
    )
    out_path = testdata_dir / cleantopo_br.dict["output"]["path"]

    # copy tiles and subset by point
    run_cli(
        [
            "cp",
            out_path,
            mp_tmpdir / "all",
            "-z",
            "5",
            "-p",
            "170",
            "-85",
            "--concurrency",
            "none",
        ]
    )
    # copy tiles and subset by bounds
    run_cli(
        [
            "cp",
            out_path,
            mp_tmpdir / "all",
            "-z",
            "5",
            "-b",
            "169.19251592399996",
            "-90",
            "180",
            "-80.18582802550002",
            "--concurrency",
            "none",
        ]
    )
    # copy all tiles
    run_cli(
        [
            "cp",
            out_path,
            mp_tmpdir / "all",
            "-z",
            "5",
            "--concurrency",
            "none",
        ]
    )
    # copy tiles and subset by area
    run_cli(
        [
            "cp",
            out_path,
            mp_tmpdir / "all",
            "-z",
            "5",
            "--area",
            wkt_geom,
            "--concurrency",
            "none",
        ]
    )
    # copy local tiles wit using threads
    run_cli(
        [
            "cp",
            out_path,
            mp_tmpdir / "all",
            "-z",
            "5",
            "--concurrency",
            "threads",
        ]
    )


@pytest.mark.integration
def test_cp_http(mp_tmpdir, http_tiledir):
    # copy tiles and subset by bounds
    run_cli(
        [
            "cp",
            http_tiledir,
            mp_tmpdir / "http",
            "-z",
            "1",
            "-b",
            "3.0",
            "1.0",
            "4.0",
            "2.0",
            "--concurrency",
            "none",
        ]
    )
