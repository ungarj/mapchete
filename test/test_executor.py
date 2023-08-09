import time
from concurrent.futures._base import CancelledError

import pytest

import mapchete
from mapchete import Executor, SkippedFuture
from mapchete._executor import FakeFuture
from mapchete.errors import MapcheteTaskFailed


def _dummy_process(i, sleep=0):
    time.sleep(sleep)
    return i + 1


@pytest.mark.parametrize(
    "executor_fixture",
    ["sequential_executor", "dask_executor", "processes_executor", "threads_executor"],
)
def test_as_completed(executor_fixture, request, items=10):
    executor = request.getfixturevalue(executor_fixture)

    count = 0
    # process all
    for future in executor.as_completed(_dummy_process, range(items)):
        count += 1
        assert future.result()
    assert items == count
    assert not executor.running_futures


@pytest.mark.parametrize(
    "executor_fixture",
    ["sequential_executor", "dask_executor", "processes_executor", "threads_executor"],
)
def test_as_completed_cancel(executor_fixture, request, items=10):
    executor = request.getfixturevalue(executor_fixture)

    # abort
    for future in executor.as_completed(_dummy_process, range(items)):
        assert future.result()
        executor.cancel()
    assert not executor.running_futures


@pytest.mark.parametrize(
    "executor_fixture",
    ["sequential_executor", "dask_executor", "processes_executor", "threads_executor"],
)
def test_as_completed_skip(executor_fixture, request, items=10):
    executor = request.getfixturevalue(executor_fixture)

    count = 0
    skip_info = "foo"
    # process all
    for future in executor.as_completed(
        _dummy_process,
        [(i, True, skip_info) for i in range(items)],
        item_skip_bool=True,
    ):
        assert isinstance(future, SkippedFuture)
        assert future.skip_info == skip_info
        count += 1
    assert not executor.running_futures
    assert items == count


@pytest.mark.parametrize(
    "executor_fixture",
    ["sequential_executor", "dask_executor", "processes_executor", "threads_executor"],
)
@pytest.mark.parametrize(
    "max_submitted_tasks",
    [1, 2, 10],
)
def test_as_completed_max_tasks(
    executor_fixture, max_submitted_tasks, request, items=100
):
    executor = request.getfixturevalue(executor_fixture)

    count = 0
    for future in executor.as_completed(
        _dummy_process,
        range(items),
        max_submitted_tasks=max_submitted_tasks,
        chunksize=items // 10,
    ):
        assert future.result()
        count += 1

    assert count == items
    assert not executor.running_futures


@pytest.mark.parametrize(
    "executor_fixture",
    ["sequential_executor", "dask_executor", "processes_executor", "threads_executor"],
)
def test_map(executor_fixture, request):
    executor = request.getfixturevalue(executor_fixture)

    items = list(range(10))
    result = executor.map(_dummy_process, items)
    assert [i + 1 for i in items] == result


def test_fake_future():
    def task(*args, **kwargs):
        return True

    def failing_task(*args, **kwargs):
        raise RuntimeError()

    future = FakeFuture(task, fargs=[1, True], fkwargs=dict(foo="bar"))
    assert future.result()
    assert not future.exception()

    future = FakeFuture(failing_task, fargs=[1, True], fkwargs=dict(foo="bar"))
    with pytest.raises(RuntimeError):
        future.result()
    assert future.exception()


def test_process_exception_tile(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(mp.compute(tile=(5, 0, 0), concurrency="processes"))


def test_process_exception_tile_dask(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(
                mp.compute(tile=(5, 0, 0), concurrency="dask", dask_compute_graph=True)
            )


def test_process_exception_tile_dask_nograph(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(
                mp.compute(tile=(5, 0, 0), concurrency="dask", dask_compute_graph=False)
            )


def test_process_exception_zoom(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(mp.compute(zoom=5, concurrency="processes"))


def test_process_exception_zoom_dask(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(mp.compute(zoom=5, concurrency="dask", dask_compute_graph=True))


def test_process_exception_zoom_dask_nograph(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(mp.compute(zoom=5, concurrency="dask", dask_compute_graph=False))


def test_dask_cancellederror(dask_executor, items=10):
    def raise_cancellederror(*args, **kwargs):
        raise CancelledError()

    with pytest.raises(CancelledError):
        list(dask_executor.as_completed(raise_cancellederror, range(items)))
