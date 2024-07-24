from itertools import chain

import pytest
from shapely.geometry import shape

from mapchete.errors import NoTaskGeometry
from mapchete.processing.tasks import Task, TaskBatch, Tasks, TileTask, TileTaskBatch
from mapchete.testing import ProcessFixture


def dummy_func(*args, **kwargs):
    return


def test_task_geo_interface():
    task = Task(dummy_func, bounds=(0, 1, 2, 3))
    assert not shape(task).is_empty


def test_task_errors():
    # provide geometry and bounds at the same time
    with pytest.raises(ValueError):
        Task(dummy_func, geometry="foo", bounds="bar")

    # task has no geo information
    with pytest.raises(NoTaskGeometry):
        Task(dummy_func).__geo_interface__

    # invalid dependencies
    with pytest.raises(TypeError):
        Task(dummy_func).add_dependencies("invalid")


def test_task_dict():
    task_dict = Task(dummy_func, bounds=(0, 1, 2, 3)).to_dict()
    assert "geometry" in task_dict
    assert "properties" in task_dict
    assert "id" in task_dict
    assert "bounds" in task_dict


def test_task_dependencies():
    task = Task(dummy_func)
    task.add_dependencies({"foo": "bar"})
    assert "foo" in task.dependencies


def test_tile_task_geo_interface(example_mapchete):
    task = TileTask(
        tile=example_mapchete.first_process_tile(), config=example_mapchete.mp().config
    )
    assert not shape(task).is_empty


def test_task_batches():
    batch = TaskBatch(
        (Task(func=str, fargs=(i,), bounds=(0, 1, 2, 3)) for i in range(10))
    )
    assert batch.items()
    assert batch.keys()
    assert batch.values()

    for task in batch:
        assert isinstance(task, Task)

    other_task = Task(dummy_func, bounds=(0, 1, 2, 3))
    assert len(batch.intersection(other_task)) == 10
    assert len(batch.intersection((0, 1, 2, 3))) == 10


def test_task_batches_errors():
    with pytest.raises(TypeError):
        TaskBatch()

    with pytest.raises(TypeError):
        TaskBatch(["invalid"])

    batch = TaskBatch(
        (Task(func=str, fargs=(i,), bounds=(0, 1, 2, 3)) for i in range(10))
    )
    with pytest.raises(TypeError):
        batch.intersection("invalid")


def test_tile_task_batch(dem_to_hillshade):
    tile_task_batch = TileTaskBatch(
        (
            TileTask(tile=process_tile, config=dem_to_hillshade.mp().config)
            for process_tile in dem_to_hillshade.mp().get_process_tiles(zoom=5)
        )
    )

    for task in tile_task_batch:
        assert isinstance(task, TileTask)

    other_tile = dem_to_hillshade.first_process_tile().get_parent()
    tile_task = TileTask(tile=other_tile, config=dem_to_hillshade.mp().config)
    assert len(tile_task_batch.intersection(tile_task)) == 4

    task = Task(dummy_func, bounds=other_tile.bounds)
    assert len(tile_task_batch.intersection(task)) == 4

    assert len(tile_task_batch.intersection(task.bounds)) == 4

    with pytest.raises(TypeError):
        tile_task_batch.intersection("invalid")


def test_task_batches_to_dask_graph(dem_to_hillshade):
    preprocessing_batch = TaskBatch(
        (Task(func=str, fargs=(i,), bounds=(0, 1, 2, 3)) for i in range(10))
    )
    assert preprocessing_batch.items()
    assert preprocessing_batch.keys()
    assert preprocessing_batch.values()
    zoom_batches = (
        TileTaskBatch(
            (
                TileTask(tile=process_tile, config=dem_to_hillshade.mp().config)
                for process_tile in dem_to_hillshade.mp().get_process_tiles(zoom=zoom)
            )
        )
        for zoom in dem_to_hillshade.mp().config.zoom_levels.descending()
    )
    collection = Tasks((preprocessing_batch, *zoom_batches)).to_dask_graph()
    assert collection
    # deactivated this because it stalls GitHub action
    # import dask
    # dask.compute(collection, scheduler=dask_executor._executor_client)


def test_task_batches_mixed_geometries():
    batch = TaskBatch(
        chain(
            (Task(func=str, fargs=(i,), bounds=(0, 1, 2, 3)) for i in range(10)),
            (Task(func=str, fargs=(i,)) for i in range(10)),
        )
    )
    assert len(batch.items()) == 20
    assert len(batch.keys()) == 20
    assert len(batch.values()) == 20

    for task in batch:
        assert isinstance(task, Task)

    # other task intersecting with all tasks
    other_task = Task(dummy_func, bounds=(0, 1, 2, 3))
    assert len(batch.intersection(other_task)) == 20
    assert len(batch.intersection((0, 1, 2, 3))) == 20

    # other task not intersecting with spatial tasks
    other_task = Task(dummy_func, bounds=(3, 4, 5, 6))
    assert len(batch.intersection(other_task)) == 10
    assert len(batch.intersection((3, 4, 5, 6))) == 10


def test_task_batch_geometry():
    batch = TaskBatch(
        [Task(func=str, bounds=(0, 1, 2, 3)), Task(func=str, bounds=(2, 3, 4, 5))]
    )
    assert shape(batch).bounds == (0, 1, 4, 5)

    # return empty geometry
    assert shape(TaskBatch([])).is_empty


def task_batches_generator(process: ProcessFixture, preprocessing_tasks_count=10):
    if preprocessing_tasks_count:
        input_key = list(process.mp().config.inputs.keys())[0]
        yield TaskBatch(
            (
                Task(
                    func=str,
                    fargs=(i,),
                    bounds=process.mp().config.bounds,
                    id=f"{input_key}:{i}",
                )
                for i in range(preprocessing_tasks_count)
            )
        )
    for zoom in process.mp().config.zoom_levels.descending():
        yield TileTaskBatch(
            (
                TileTask(tile=process_tile, config=process.mp().config)
                for process_tile in process.mp().get_process_tiles(zoom=zoom)
            ),
            id=f"zoom-{zoom}",
        )


def test_task_batches_as_dask_graph(dem_to_hillshade):
    task_batches = Tasks(task_batches_generator(dem_to_hillshade))
    assert len(task_batches.preprocessing_batches) == 1
    assert len(task_batches.tile_batches) == len(
        dem_to_hillshade.mp().config.zoom_levels
    )
    graph = task_batches.to_dask_graph()
    assert graph

    # deactivated this because it stalls GitHub action
    # import dask
    # dask.compute(graph, scheduler=dask_executor._executor_client)


def test_task_batches_as_layered_batches(dem_to_hillshade):
    task_batches = Tasks(task_batches_generator(dem_to_hillshade))
    assert len(task_batches.preprocessing_batches) == 1
    assert len(task_batches.tile_batches) == len(
        dem_to_hillshade.mp().config.zoom_levels
    )
    batches = [list(batch) for batch in task_batches.to_batches()]
    assert batches

    for batch in batches:
        assert batch
        for tile_task in batch:
            assert isinstance(tile_task, Task)


def test_task_batches_as_single_batch(dem_to_hillshade):
    task_batches = Tasks(task_batches_generator(dem_to_hillshade))
    assert len(task_batches.preprocessing_batches) == 1
    assert len(task_batches.tile_batches) == len(
        dem_to_hillshade.mp().config.zoom_levels
    )
    batch = list(task_batches.to_batch())
    assert batch
    for tile_task in batch:
        assert isinstance(tile_task, Task)
