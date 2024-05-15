from test.commands import TaskCounter

import pytest
from shapely.geometry import box
from tilematrix import TilePyramid

import mapchete
from mapchete.commands import convert, cp, execute, index, rm
from mapchete.config import DaskSettings
from mapchete.enums import Concurrency, Status
from mapchete.errors import JobCancelledError
from mapchete.io import fiona_open, rasterio_open
from mapchete.processing.types import TaskInfo
from mapchete.protocols import ObserverProtocol


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        ("threads", None),
        ("dask", True),
        ("dask", False),
        ("processes", None),
        (None, None),
    ],
)
def test_execute(
    cleantopo_br_metatiling_1, cleantopo_br_tif, concurrency, process_graph
):
    execute_kwargs = dict(concurrency=concurrency)
    if concurrency == "dask":
        execute_kwargs.update(dask_settings=DaskSettings(process_graph=process_graph))

    zoom = 5
    tp = TilePyramid("geodetic")
    with rasterio_open(cleantopo_br_tif) as src:
        tiles = list(tp.tiles_from_bounds(src.bounds, zoom))
    execute(cleantopo_br_metatiling_1.dict, zoom=zoom, **execute_kwargs)
    mp = cleantopo_br_metatiling_1.mp()
    for t in tiles:
        with rasterio_open(mp.config.output.get_path(t)) as src:
            assert not src.read(masked=True).mask.all()


def test_execute_retry(example_mapchete):
    zoom = 10
    retries = 2

    class ExceptionRaiser:
        """Makes the job fail during progress."""

        def update(*args, progress=None, **kwargs):
            if progress and progress.current > 2:
                raise RuntimeError("This job just raised an exception!")

    class RetryCounter:
        """Count retry attempts."""

        retries = 0

        def update(self, *args, status=None, **kwargs):
            if status and status == Status.retrying:
                self.retries += 1

    exception_raiser = ExceptionRaiser()
    retry_counter = RetryCounter()

    # this job should fail
    with pytest.raises(RuntimeError):
        execute(
            example_mapchete.dict,
            zoom=zoom,
            retries=retries,
            observers=[exception_raiser, retry_counter],
            concurrency=Concurrency.none,
        )

    # make sure job has been retried
    assert retry_counter.retries == retries


def test_execute_cancel(cleantopo_br_metatiling_1):
    zoom = 5

    class CancelObserver:
        """Cancels job when running."""

        def update(*args, progress=None, **kwargs):
            if progress and progress.current > 0:
                raise JobCancelledError

    class StatusObserver:
        """Observes job state."""

        status = None

        def update(self, *args, status=None, **kwargs):
            if status:
                self.status = status

    state_observer = StatusObserver()
    execute(
        cleantopo_br_metatiling_1.dict,
        zoom=zoom,
        observers=[CancelObserver(), state_observer],
        concurrency=Concurrency.none,
    )
    assert state_observer.status == Status.cancelled


def test_execute_tile(mp_tmpdir, cleantopo_br_metatiling_1):
    tile = (5, 30, 63)

    task_counter = TaskCounter()
    execute(cleantopo_br_metatiling_1.dict, tile=tile, observers=[task_counter])

    assert task_counter.tasks == 1

    mp = cleantopo_br_metatiling_1.mp()
    with rasterio_open(
        mp.config.output.get_path(mp.config.output_pyramid.tile(*tile))
    ) as src:
        assert not src.read(masked=True).mask.all()


def test_execute_point(mp_tmpdir, example_mapchete, dummy2_tif):
    """Using bounds from WKT."""
    with rasterio_open(dummy2_tif) as src:
        g = box(*src.bounds)

    task_counter = TaskCounter()
    execute(
        example_mapchete.dict,
        point=[g.centroid.x, g.centroid.y],
        zoom=10,
        observers=[task_counter],
    )
    assert task_counter.tasks == 1


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        ("threads", None),
        ("dask", True),
        ("dask", False),
        ("processes", None),
        (None, None),
    ],
)
def test_execute_preprocessing_tasks(
    concurrency, preprocess_cache_raster_vector, process_graph
):
    execute_kwargs = dict(concurrency=concurrency)
    if concurrency == "dask":
        execute_kwargs.update(dask_settings=DaskSettings(process_graph=process_graph))

    task_counter = TaskCounter()
    execute(
        preprocess_cache_raster_vector.path, observers=[task_counter], **execute_kwargs
    )
    assert task_counter.tasks


@pytest.mark.parametrize(
    "concurrency,process_graph",
    [
        # ("threads", False),  # profiling does not work with threads
        ("dask", False),
        ("dask", True),
        ("processes", False),
        (None, False),
    ],
)
def test_execute_profiling(cleantopo_br_metatiling_1, concurrency, process_graph):
    execute_kwargs = dict(concurrency=concurrency)
    if concurrency == "dask":
        execute_kwargs.update(dask_settings=DaskSettings(process_graph=process_graph))

    zoom = 5

    class TaskResultObserver(ObserverProtocol):
        def update(self, *args, task_result=None, **kwargs):
            if task_result:
                assert isinstance(task_result, TaskInfo)
                assert task_result.profiling
                for profiler in ["time", "memory"]:
                    assert profiler in task_result.profiling

                assert task_result.profiling["time"].elapsed > 0

                assert task_result.profiling["memory"].max_allocated > 0
                assert task_result.profiling["memory"].total_allocated > 0
                assert task_result.profiling["memory"].allocations > 0

    execute(
        cleantopo_br_metatiling_1.dict,
        zoom=zoom,
        profiling=True,
        observers=[TaskResultObserver()],
        **execute_kwargs
    )


def test_convert_empty_gpkg(empty_gpkg, mp_tmpdir):
    convert(
        empty_gpkg,
        mp_tmpdir,
        output_pyramid="geodetic",
        zoom=5,
        output_format="GeoJSON",
    )
