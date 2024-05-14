from test.cli.default import run_cli

import pytest

import mapchete
from mapchete.io.vector import fiona_open


def test_geojson(cleantopo_br):
    # execute process at zoom 3
    run_cli(
        ["execute", cleantopo_br.path, "-z", "3", "--debug", "--concurrency", "none"]
    )

    # generate index for zoom 3
    run_cli(["index", cleantopo_br.path, "-z", "3", "--geojson", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert len(files) == 4
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_geojson_fieldname(cleantopo_br):
    # execute process at zoom 3
    run_cli(
        ["execute", cleantopo_br.path, "-z", "3", "--debug", "--concurrency", "none"]
    )

    # index and rename "location" to "new_fieldname"
    run_cli(
        [
            "index",
            cleantopo_br.path,
            "-z",
            "3",
            "--geojson",
            "--debug",
            "--fieldname",
            "new_fieldname",
        ]
    )
    with mapchete.open(cleantopo_br.dict) as mp:
        with fiona_open(mp.config.output.path / "3.geojson") as src:
            for f in src:
                assert "new_fieldname" in f["properties"]
            assert len(list(src)) == 1


def test_geojson_basepath(cleantopo_br):
    # execute process at zoom 3
    run_cli(
        ["execute", cleantopo_br.path, "-z", "3", "--debug", "--concurrency", "none"]
    )

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    run_cli(
        [
            "index",
            cleantopo_br.path,
            "-z",
            "3",
            "--geojson",
            "--debug",
            "--basepath",
            basepath,
        ]
    )
    with mapchete.open(cleantopo_br.dict) as mp:
        with fiona_open(mp.config.output.path / "3.geojson") as src:
            for f in src:
                assert f["properties"]["location"].startswith(basepath)
            assert len(list(src)) == 1


def test_geojson_for_gdal(cleantopo_br):
    # execute process at zoom 3
    run_cli(["execute", cleantopo_br.path, "-z", "3", "--debug"])

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    run_cli(
        [
            "index",
            cleantopo_br.path,
            "-z",
            "3",
            "--geojson",
            "--debug",
            "--basepath",
            basepath,
            "--for-gdal",
        ]
    )
    with mapchete.open(cleantopo_br.dict) as mp:
        with fiona_open(mp.config.output.path / "3.geojson") as src:
            for f in src:
                assert f["properties"]["location"].startswith("/vsicurl/" + basepath)
            assert len(list(src)) == 1


def test_geojson_tile(cleantopo_tl):
    # execute process for single tile
    run_cli(
        [
            "execute",
            cleantopo_tl.path,
            "-t",
            "3",
            "0",
            "0",
            "--debug",
            "--concurrency",
            "none",
        ]
    )
    # generate index
    run_cli(["index", cleantopo_tl.path, "-t", "3", "0", "0", "--geojson", "--debug"])
    with mapchete.open(cleantopo_tl.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert len(files) == 4
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        assert len(list(src)) == 1


def test_geojson_wkt_area(cleantopo_br, wkt_geom):
    # execute process at zoom 3
    run_cli(
        [
            "execute",
            cleantopo_br.path,
            "--debug",
            "--area",
            wkt_geom,
            "--concurrency",
            "none",
        ]
    )

    # generate index for zoom 3
    run_cli(["index", cleantopo_br.path, "--geojson", "--debug", "--area", wkt_geom])

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert len(files) == 8  # was 7 before doing the observer pattern
        assert "3.geojson" in files


def test_gpkg(cleantopo_br):
    # execute process
    run_cli(
        ["execute", cleantopo_br.path, "-z", "5", "--debug", "--concurrency", "none"]
    )

    # generate index
    run_cli(["index", cleantopo_br.path, "-z", "5", "--gpkg", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.gpkg" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    run_cli(["index", cleantopo_br.path, "-z", "5", "--gpkg", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.gpkg" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_shp(cleantopo_br):
    # execute process
    run_cli(
        ["execute", cleantopo_br.path, "-z", "5", "--debug", "--concurrency", "none"]
    )

    # generate index
    run_cli(["index", cleantopo_br.path, "-z", "5", "--shp", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.shp" in files
    with fiona_open(mp.config.output.path / "5.shp") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    run_cli(["index", cleantopo_br.path, "-z", "5", "--shp", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.shp" in files
    with fiona_open(mp.config.output.path / "5.shp") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_fgb(cleantopo_br):
    # execute process
    run_cli(
        ["execute", cleantopo_br.path, "-z", "5", "--debug", "--concurrency", "none"]
    )

    # generate index
    run_cli(["index", cleantopo_br.path, "-z", "5", "--fgb", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.fgb" in files
    with fiona_open(mp.config.output.path / "5.fgb") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    run_cli(["index", cleantopo_br.path, "-z", "5", "--fgb", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.fgb" in files
    with fiona_open(mp.config.output.path / "5.fgb") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_text(cleantopo_br):
    # execute process
    run_cli(
        ["execute", cleantopo_br.path, "-z", "5", "--debug", "--concurrency", "none"]
    )

    # generate index
    run_cli(["index", cleantopo_br.path, "-z", "5", "--txt", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.txt" in files
    with open(mp.config.output.path / "5.txt") as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")

    # write again and assert there is no new entry because there is already one
    run_cli(["index", cleantopo_br.path, "-z", "5", "--txt", "--debug"])
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.txt" in files
    with open(mp.config.output.path / "5.txt") as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")


def test_errors(cleantopo_br):
    with pytest.raises(ValueError):
        run_cli(["index", cleantopo_br.path, "-z", "5", "--debug"])

    with pytest.raises(SystemExit):
        run_cli(["index", "-z", "5", "--debug"])
