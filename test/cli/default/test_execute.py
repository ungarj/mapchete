"""Test Mapchete main module and processing."""

import logging

from shapely import wkt

import mapchete
from mapchete.io import rasterio_open

from . import run_cli

logger = logging.getLogger(__name__)


def test_concurrent_processes(cleantopo_br_metatiling_1):
    # """Run mapchete execute with multiple workers."""
    run_cli(
        [
            "execute",
            cleantopo_br_metatiling_1.path,
            "--zoom",
            "5",
            "--workers",
            "2",
            "-d",
            "--concurrency",
            "processes",
        ]
    )


def test_concurrent_threads(cleantopo_br_metatiling_1):
    """Run mapchete execute with multiple workers."""
    run_cli(
        [
            "execute",
            cleantopo_br_metatiling_1.path,
            "--zoom",
            "5",
            "--workers",
            "2",
            "-d",
            "--concurrency",
            "threads",
        ],
    )


def test_concurrent_dask(cleantopo_br_metatiling_1):
    """Run mapchete execute with multiple workers."""
    run_cli(
        [
            "execute",
            cleantopo_br_metatiling_1.path,
            "--zoom",
            "5",
            "--workers",
            "2",
            "-d",
            "--concurrency",
            "dask",
        ],
    )


def test_debug(example_mapchete):
    """Using debug output."""
    run_cli(
        [
            "execute",
            example_mapchete.path,
            "-t",
            "10",
            "500",
            "1040",
            "--debug",
            "--concurrency",
            "none",
        ]
    )


def test_vrt(mp_tmpdir, cleantopo_br):
    """Using debug output."""
    run_cli(["execute", cleantopo_br.path, "-z", "5", "--vrt"])
    with mapchete.open(cleantopo_br.dict) as mp:
        vrt_path = mp.config.output.path / "5.vrt"
        with rasterio_open(vrt_path) as src:
            assert src.read().any()

    # run again, this time with custom output directory
    run_cli(
        [
            "execute",
            cleantopo_br.path,
            "-z",
            "5",
            "--vrt",
            "--idx-out-dir",
            mp_tmpdir,
            "--concurrency",
            "none",
        ]
    )
    with mapchete.open(cleantopo_br.dict) as mp:
        vrt_path = mp_tmpdir / "5.vrt"
        with rasterio_open(vrt_path) as src:
            assert src.read().any()

    # run with single tile
    run_cli(
        [
            "execute",
            cleantopo_br.path,
            "-t",
            "5",
            "3",
            "7",
            "--vrt",
            "--concurrency",
            "none",
        ]
    )

    # no new entries
    run_cli(
        [
            "execute",
            cleantopo_br.path,
            "-t",
            "5",
            "0",
            "0",
            "--vrt",
            "--concurrency",
            "none",
        ]
    )


def test_verbose(example_mapchete):
    """Using verbose output."""
    run_cli(
        [
            "execute",
            example_mapchete.path,
            "-t",
            "10",
            "500",
            "1040",
            "--verbose",
            "--concurrency",
            "none",
        ]
    )


def test_logfile(mp_tmpdir, example_mapchete):
    """Using logfile."""
    logfile = mp_tmpdir / "temp.log"
    run_cli(
        [
            "execute",
            example_mapchete.path,
            "-t",
            "10",
            "500",
            "1040",
            "--logfile",
            logfile,
            "--concurrency",
            "none",
        ]
    )
    assert logfile.exists()
    with open(logfile) as log:
        assert "DEBUG" in log.read()


def test_wkt_area(example_mapchete, wkt_geom):
    """Using area from WKT."""
    run_cli(
        ["execute", example_mapchete.path, "--area", wkt_geom, "--concurrency", "none"]
    )


def test_point(example_mapchete, wkt_geom):
    """Using bounds from WKT."""
    g = wkt.loads(wkt_geom)
    run_cli(
        [
            "execute",
            example_mapchete.path,
            "--point",
            str(g.centroid.x),
            str(g.centroid.y),
            "--concurrency",
            "none",
        ]
    )


def test_callback_errors(cleantopo_tl):
    run_cli(
        ["execute", cleantopo_tl.path, "--zoom", "4,5,7", "--concurrency", "none"],
        expected_exit_code=2,
        raise_exc=False,
        output_contains="zooms can be maximum two items",
    )
    run_cli(
        ["execute", cleantopo_tl.path, "--zoom", "invalid", "--concurrency", "none"],
        expected_exit_code=2,
        raise_exc=False,
        output_contains="zoom levels must be integer values",
    )
