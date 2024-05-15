from test.commands import TaskCounter

import pytest

import mapchete
from mapchete.commands import rm


def test_rm(cleantopo_br, testdata_dir):
    # generate TileDirectory
    with mapchete.open(
        cleantopo_br.dict, bounds=[169.19251592399996, -90, 180, -80.18582802550002]
    ) as mp:
        list(mp.execute(zoom=5))
    out_path = testdata_dir / cleantopo_br.dict["output"]["path"]

    # remove tiles
    task_counter = TaskCounter()
    rm(out_path, zoom=5, observers=[task_counter])
    assert task_counter.tasks

    # remove tiles but this time they should already have been removed
    task_counter = TaskCounter()
    rm(out_path, zoom=5, observers=[task_counter])
    assert task_counter.tasks == 0


def test_rm_path_list(mp_tmpdir):
    out_path = mp_tmpdir / "some_file.txt"
    with out_path.open("w") as dst:
        dst.write("foo")

    assert out_path.exists()
    rm(paths=[out_path])
    assert not out_path.exists()


@pytest.mark.integration
def test_rm_path_list_s3(s3_testdata_dir):
    out_path = s3_testdata_dir / "some_file.txt"
    with out_path.open("w") as dst:
        dst.write("foo")

    assert out_path.exists()
    rm(paths=[out_path])
    assert not out_path.exists()
