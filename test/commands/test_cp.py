from test.commands import TaskCounter

import pytest

import mapchete
from mapchete.commands import cp


def test_cp(mp_tmpdir, cleantopo_br, wkt_geom, testdata_dir):
    # generate TileDirectory
    with mapchete.open(
        cleantopo_br.dict, bounds=[169.19251592399996, -90, 180, -80.18582802550002]
    ) as mp:
        list(mp.execute(zoom=5))
    out_path = testdata_dir / cleantopo_br.dict["output"]["path"]
    # copy tiles and subset by bounds
    task_counter = TaskCounter()
    cp(
        out_path,
        mp_tmpdir / "bounds",
        zoom=5,
        bounds=[169.19251592399996, -90, 180, -80.18582802550002],
        observers=[task_counter],
    )
    assert task_counter.tasks

    # copy all tiles
    task_counter = TaskCounter()
    cp(out_path, mp_tmpdir / "all", zoom=5, observers=[task_counter])
    assert task_counter.tasks

    # copy tiles and subset by area
    task_counter = TaskCounter()
    cp(
        out_path,
        mp_tmpdir / "area",
        zoom=5,
        area=wkt_geom,
        observers=[task_counter],
    )
    assert task_counter.tasks

    # copy local tiles without using threads
    task_counter = TaskCounter()
    cp(
        out_path,
        mp_tmpdir / "nothreads",
        zoom=5,
        workers=1,
        observers=[task_counter],
    )
    assert task_counter.tasks


@pytest.mark.integration
def test_cp_http(mp_tmpdir, http_tiledir):
    # copy tiles and subset by bounds
    task_counter = TaskCounter()
    cp(
        http_tiledir,
        mp_tmpdir / "http",
        zoom=1,
        bounds=[3, 1, 4, 2],
        observers=[task_counter],
    )
    assert task_counter.tasks
