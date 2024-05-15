import pytest

import mapchete
from mapchete.commands import execute, index
from mapchete.io.vector import fiona_open


def test_index_geojson(cleantopo_br):
    # execute process at zoom 3
    execute(cleantopo_br.dict, zoom=3)

    # generate index for zoom 3
    index(cleantopo_br.dict, zoom=3, geojson=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert len(files) == 4
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_geojson_fieldname(cleantopo_br):
    # execute process at zoom 3
    execute(cleantopo_br.dict, zoom=3)

    # index and rename "location" to "new_fieldname"
    index(
        cleantopo_br.dict,
        zoom=3,
        geojson=True,
        fieldname="new_fieldname",
    )
    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        for f in src:
            assert "new_fieldname" in f["properties"]
        assert len(list(src)) == 1


def test_index_geojson_basepath(cleantopo_br):
    # execute process at zoom 3
    execute(cleantopo_br.dict, zoom=3)

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    index(cleantopo_br.dict, zoom=3, geojson=True, basepath=basepath)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        for f in src:
            assert f["properties"]["location"].startswith(basepath)
        assert len(list(src)) == 1


def test_index_geojson_for_gdal(cleantopo_br):
    # execute process at zoom 3
    execute(cleantopo_br.dict, zoom=3)

    basepath = "http://localhost"
    # index and rename "location" to "new_fieldname"
    index(cleantopo_br.dict, zoom=3, geojson=True, basepath=basepath, for_gdal=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        for f in src:
            assert f["properties"]["location"].startswith("/vsicurl/" + basepath)
        assert len(list(src)) == 1


def test_index_geojson_tile(cleantopo_tl):
    # execute process at zoom 3
    execute(cleantopo_tl.dict, zoom=3)

    # generate index
    index(cleantopo_tl.dict, tile=(3, 0, 0), geojson=True)

    with mapchete.open(cleantopo_tl.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert len(files) == 4
        assert "3.geojson" in files
    with fiona_open(mp.config.output.path / "3.geojson") as src:
        assert len(list(src)) == 1


def test_index_geojson_wkt_area(cleantopo_tl, wkt_geom_tl):
    # execute process at zoom 3
    execute(cleantopo_tl.dict, area=wkt_geom_tl)

    # generate index for zoom 3
    index(cleantopo_tl.dict, geojson=True, area=wkt_geom_tl)

    with mapchete.open(cleantopo_tl.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert len(files) == 14
        assert "3.geojson" in files


def test_index_gpkg(cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.dict, zoom=5, gpkg=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.gpkg" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, gpkg=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.gpkg" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_shp(cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.dict, zoom=5, shp=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.shp" in files
    with fiona_open(mp.config.output.path / "5.shp") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, shp=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.shp" in files
    with fiona_open(mp.config.output.path / "5.shp") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_fgb(cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.dict, zoom=5, fgb=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.fgb" in files
    with fiona_open(mp.config.output.path / "5.fgb") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, fgb=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.fgb" in files
    with fiona_open(mp.config.output.path / "5.fgb") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_text(cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.dict, zoom=5, txt=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.txt" in files
    with open(mp.config.output.path / "5.txt") as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, txt=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.txt" in files
    with open(mp.config.output.path / "5.txt") as src:
        lines = list(src)
        assert len(lines) == 1
        for l in lines:
            assert l.endswith("7.tif\n")


def test_index_tiledir(cleantopo_br):
    # execute process
    execute(cleantopo_br.dict, zoom=5)

    # generate index
    index(cleantopo_br.output_path, zoom=5, gpkg=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.gpkg" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1

    # write again and assert there is no new entry because there is already one
    index(cleantopo_br.dict, zoom=5, txt=True)

    with mapchete.open(cleantopo_br.dict) as mp:
        files = mp.config.output.path.ls(absolute_paths=False)
        assert "5.txt" in files
    with fiona_open(mp.config.output.path / "5.gpkg") as src:
        for f in src:
            assert "location" in f["properties"]
        assert len(list(src)) == 1


def test_index_errors(cleantopo_br):
    with pytest.raises(ValueError):
        index(cleantopo_br.dict, zoom=5)
