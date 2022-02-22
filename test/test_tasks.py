from itertools import chain
from typing import Type
import pytest
from shapely.geometry import shape


from mapchete.errors import NoTaskGeometry
from mapchete._tasks import Task, TileTask, TaskBatch, TileTaskBatch, to_dask_collection


def test_task_geo_interface():
    task = Task(bounds=(0, 1, 2, 3))
    assert not shape(task).is_empty


def test_task_errors():
    # provide geometry and bounds at the same time
    with pytest.raises(ValueError):
        Task(geometry="foo", bounds="bar")

    # task has no geo information
    with pytest.raises(NoTaskGeometry):
        Task().__geo_interface__

    # invalid dependencies
    with pytest.raises(TypeError):
        Task().add_dependencies("invalid")


def test_task_dict():
    task_dict = Task(bounds=(0, 1, 2, 3)).to_dict()
    assert "geometry" in task_dict
    assert "properties" in task_dict
    assert "id" in task_dict
    assert "bounds" in task_dict


def test_task_dependencies():
    task = Task()
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

    other_task = Task(bounds=(0, 1, 2, 3))
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

    task = Task(bounds=other_tile.bounds)
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
        for zoom in reversed(dem_to_hillshade.mp().config.zoom_levels)
    )
    collection = to_dask_collection((preprocessing_batch, *zoom_batches))
    import dask

    dask.compute(collection)


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
    other_task = Task(bounds=(0, 1, 2, 3))
    assert len(batch.intersection(other_task)) == 20
    assert len(batch.intersection((0, 1, 2, 3))) == 20

    # other task not intersecting with spatial tasks
    other_task = Task(bounds=(3, 4, 5, 6))
    assert len(batch.intersection(other_task)) == 10
    assert len(batch.intersection((3, 4, 5, 6))) == 10
