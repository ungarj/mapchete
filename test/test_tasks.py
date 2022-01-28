from mapchete._tasks import Task, TileTask, TaskBatch, TileTaskBatch, to_dask_collection
from mapchete.tile import BufferedTilePyramid

from shapely.geometry import shape


def test_task_geo_interface():
    task = Task(bounds=(0, 1, 2, 3))
    assert not shape(task).is_empty


def test_tile_task_geo_interface(example_mapchete):
    task = TileTask(
        tile=example_mapchete.first_process_tile(), config=example_mapchete.mp().config
    )
    assert not shape(task).is_empty


def test_task_batches(dem_to_hillshade):
    preprocessing_batch = TaskBatch(
        (Task(func=str, fargs=(i,), bounds=(0, 1, 2, 3)) for i in range(10))
    )
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
