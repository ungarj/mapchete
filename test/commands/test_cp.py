from test.commands import TaskCounter

import pytest
from shapely import wkt

import mapchete
from mapchete.commands import cp


def test_cp_bounds(mp_tmpdir, local_tiledirectory):
    # copy tiles and subset by bounds
    task_counter = TaskCounter()
    cp(
        local_tiledirectory,
        mp_tmpdir / "bounds",
        zoom=5,
        bounds=[169.19251592399996, -90, 180, -80.18582802550002],
        observers=[task_counter],
    )
    assert task_counter.tasks


def test_cp_all(mp_tmpdir, local_tiledirectory):
    # copy all tiles
    task_counter = TaskCounter()
    cp(local_tiledirectory, mp_tmpdir / "all", zoom=5, observers=[task_counter])
    assert task_counter.tasks


def test_cp_area(mp_tmpdir, wkt_geom, local_tiledirectory):
    # copy tiles and subset by area
    task_counter = TaskCounter()
    cp(
        local_tiledirectory,
        mp_tmpdir / "area",
        zoom=5,
        area=wkt_geom,
        observers=[task_counter],
    )
    assert task_counter.tasks


def test_cp_point(mp_tmpdir, wkt_geom, local_tiledirectory):
    # copy tiles and subset by point
    task_counter = TaskCounter()
    cp(
        local_tiledirectory,
        mp_tmpdir / "point",
        zoom=5,
        point=wkt.loads(wkt_geom).centroid,
        observers=[task_counter],
    )
    assert task_counter.tasks


def test_cp_existing(mp_tmpdir, local_tiledirectory):
    point = (169.19251592399996, -89.99)
    # copy two times
    task_counter = TaskCounter()
    cp(
        local_tiledirectory,
        mp_tmpdir / "point",
        zoom=5,
        point=point,
        observers=[task_counter],
    )
    assert task_counter.tasks
    assert task_counter.text_in_messages("1 tiles copied")
    task_counter = TaskCounter()
    cp(
        local_tiledirectory,
        mp_tmpdir / "point",
        zoom=5,
        point=point,
        observers=[task_counter],
        workers=1,
    )
    assert task_counter.tasks
    assert task_counter.text_in_messages("destination tile exists")


def test_cp_nothreads(mp_tmpdir, local_tiledirectory):
    # copy local tiles without using threads
    task_counter = TaskCounter()
    cp(
        local_tiledirectory,
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
