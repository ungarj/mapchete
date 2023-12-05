import pytest

import mapchete
from mapchete.io import fs_from_path, rasterio_open
from mapchete.processing.tasks import Task


def _trivial_func(arg, kwarg=None):
    return arg + kwarg + "bar"


def test_preprocessing_empty_tasks_per_input(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        assert len(mp.config.input) == len(mp.config.preprocessing_tasks_per_input())


def test_preprocessing_empty_tasks(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        assert len(mp.config.preprocessing_tasks()) == 0


def test_preprocessing_empty_tasks_count(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        assert mp.config.preprocessing_tasks_count() == 0


def test_preprocessing_set_empty_preprocessing_task_result(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        with pytest.raises(KeyError):
            mp.config.set_preprocessing_task_result("foo", "bar")


def test_add_preprocessing_task(example_mapchete):
    with mapchete.open(example_mapchete.dict) as mp:
        # get input object
        inp = mp.config.input_at_zoom("file1", 10)
        tasks = mp.count_tasks()
        # add a preprocessing task and make sure it is registered
        inp.add_preprocessing_task(_trivial_func, fargs=("foo"))
        assert len(mp.config.preprocessing_tasks()) == 1
        assert mp.config.preprocessing_tasks_count() == 1
        assert mp.count_tasks() == tasks + 1


@pytest.mark.parametrize("concurrency", ["threads", "processes", "dask", None])
def test_run_preprocessing_task(cleantopo_br, concurrency):
    zoom = 5
    with mapchete.open(cleantopo_br.dict) as mp:
        # get input object
        inp = mp.config.input_at_zoom("file1", zoom)
        with pytest.raises(KeyError):
            inp.get_preprocessing_task_result("test_task")
        # add a preprocessing task and make sure it is registered
        inp.add_preprocessing_task(
            _trivial_func, key="test_task", fargs="foo", fkwargs={"kwarg": "foo"}
        )
        with pytest.raises(ValueError):
            inp.get_preprocessing_task_result("test_task")
        list(
            mp.execute(
                concurrency=concurrency, zoom=zoom, remember_preprocessing_results=True
            )
        )
        assert inp.get_preprocessing_task_result("test_task") == "foofoobar"


@pytest.mark.parametrize("concurrency", ["threads", "processes", "dask", None])
def test_run_preprocessing_tasks(cleantopo_br, concurrency):
    zoom = 3
    with mapchete.open(cleantopo_br.dict) as mp:
        inp1 = mp.config.input_at_zoom("file1", zoom)
        inp1.add_preprocessing_task(
            _trivial_func, key="test_task", fargs="foo", fkwargs={"kwarg": "foo"}
        )
        inp1.add_preprocessing_task(
            _trivial_func, key="test_other_task", fargs="bar", fkwargs={"kwarg": "foo"}
        )
        # total number of tasks should be 3
        assert (
            sum(
                [
                    len(tasks)
                    for tasks in mp.config.preprocessing_tasks_per_input().values()
                ]
            )
            == 2
        )

        assert mp.config.preprocessing_tasks_count() == 2

        list(
            mp.execute(
                concurrency=concurrency, zoom=zoom, remember_preprocessing_results=True
            )
        )
        assert mp.config.preprocessing_task_finished(f"{inp1.input_key}:test_task")
        assert inp1.get_preprocessing_task_result("test_task") == "foofoobar"
        assert mp.config.preprocessing_task_finished(
            f"{inp1.input_key}:test_other_task"
        )
        assert inp1.get_preprocessing_task_result("test_other_task") == "barfoobar"


def test_preprocess_cache_raster_vector(preprocess_cache_raster_vector):
    # tile containing only features: 5 29 62
    mp = preprocess_cache_raster_vector.process_mp((5, 29, 62))
    with mp.open("clip") as clip:
        assert str(clip.path).endswith("cache/aoi_br.fgb")
        assert clip.read()
    with mp.open("inp") as raster:
        assert str(raster.path).endswith("cache/cleantopo_br.tif")
        assert raster.read().mask.all()

    # tile containing features and raster: 5 30 62
    mp = preprocess_cache_raster_vector.process_mp((5, 30, 62))
    with mp.open("clip") as clip:
        assert str(clip.path).endswith("cache/aoi_br.fgb")
        assert clip.read()
    with mp.open("inp") as raster:
        assert str(raster.path).endswith("cache/cleantopo_br.tif")
        assert not raster.read().mask.all()


def test_preprocess_cache_raster_vector_tasks(preprocess_cache_raster_vector):
    with preprocess_cache_raster_vector.mp() as mp:
        for i in ["clip", "inp"]:
            input_data = mp.config.input_at_zoom(key=i, zoom=5)
            for task in input_data.preprocessing_tasks.values():
                assert isinstance(task, Task)
                assert task.has_geometry()


def test_preprocessing_tasks_dependencies(preprocess_cache_memory):
    with preprocess_cache_memory.mp() as mp:
        for i in ["clip", "inp"]:
            input_data = mp.config.input_at_zoom(key=i, zoom=5)
            for task in input_data.preprocessing_tasks.values():
                assert isinstance(task, Task)
                assert task.has_geometry()
        list(mp.execute())

        out_path = mp.config.output_reader.path
        total_tifs = len(
            [
                file
                for directory in fs_from_path(out_path).walk(out_path)
                for file in directory[2]
                if file.endswith(".tif")
            ]
        )
        assert total_tifs == 9


def test_preprocessing_tasks_dependencies_dask(preprocess_cache_memory):
    with preprocess_cache_memory.mp(batch_preprocess=False) as mp:
        for i in ["clip", "inp"]:
            input_data = mp.config.input_at_zoom(key=i, zoom=5)
            for task in input_data.preprocessing_tasks.values():
                assert isinstance(task, Task)
                assert task.has_geometry()

        list(mp.execute(concurrency="dask"))

        out_path = mp.config.output_reader.path
        total_tifs = [
            f"{directory[0]}/{file}"
            for directory in fs_from_path(out_path).walk(out_path)
            for file in directory[2]
            if file.endswith(".tif")
        ]
        assert len(total_tifs) == 9


@pytest.mark.parametrize("concurrency", ["threads", "processes", "dask", None])
def test_preprocessing_tasks_dependencies_single_tile(
    preprocess_cache_memory, concurrency
):
    with preprocess_cache_memory.mp(batch_preprocess=False) as mp:
        for i in ["clip", "inp"]:
            input_data = mp.config.input_at_zoom(key=i, zoom=5)
            for task in input_data.preprocessing_tasks.values():
                assert isinstance(task, Task)
                assert task.has_geometry()

        tile = (5, 31, 63)
        for task_info in mp.execute(
            tile=tile, concurrency=concurrency, remember_preprocessing_results=True
        ):
            pass

        out_path = mp.config.output_reader.get_path(
            mp.config.process_pyramid.tile(*tile)
        )
        with rasterio_open(out_path) as src:
            assert not src.read(masked=True).mask.all()

        out_path = mp.config.output_reader.path
        total_tifs = [
            f"{directory[0]}/{file}"
            for directory in fs_from_path(out_path).walk(out_path)
            for file in directory[2]
            if file.endswith(".tif")
        ]
        assert len(total_tifs) == 1
