import pytest
import time

import mapchete
from mapchete import Executor, SkippedFuture
from mapchete.errors import MapcheteTaskFailed
from mapchete._executor import FakeFuture


def _dummy_process(i, sleep=0):
    time.sleep(sleep)
    return i + 1


def test_sequential_executor_as_completed():
    items = 10
    count = 0
    with Executor(concurrency=None) as executor:
        # process all
        for future in executor.as_completed(_dummy_process, range(items)):
            count += 1
            assert future.result()
        assert items == count

        # abort
        cancelled = False
        for future in executor.as_completed(_dummy_process, range(items)):
            if cancelled:
                raise RuntimeError()
            assert future.result()
            cancelled = True
            executor.cancel()


def test_sequential_executor_as_completed_skip():
    items = 10
    count = 0
    skip_info = "foo"
    with Executor(concurrency=None) as executor:
        # process all
        for future in executor.as_completed(
            _dummy_process,
            [(i, True, skip_info) for i in range(items)],
            item_skip_bool=True,
        ):
            assert isinstance(future, SkippedFuture)
            assert future.skip_info == skip_info
            count += 1
        assert items == count


def test_sequential_executor_map():
    items = list(range(10))
    with Executor(concurrency=None) as executor:
        result = executor.map(_dummy_process, items)
        assert [i + 1 for i in items] == result


def test_concurrent_futures_processes_executor_as_completed():
    items = 10
    with Executor(concurrency="processes") as executor:
        # process all
        count = 0
        for future in executor.as_completed(_dummy_process, range(items)):
            count += 1
            assert future.result()
        assert not executor.running_futures


def test_concurrent_futures_processes_executor_as_completed_max_tasks():
    items = 100
    with Executor(concurrency="processes") as executor:
        # abort
        for future in executor.as_completed(
            _dummy_process, range(items), max_submitted_tasks=1
        ):
            assert future.result()

        assert not executor.running_futures


def test_concurrent_futures_processes_executor_as_completed_skip():
    items = 10
    skip_info = "foo"
    with Executor(concurrency="processes") as executor:
        # process all
        count = 0
        for future in executor.as_completed(
            _dummy_process,
            [(i, True, skip_info) for i in range(items)],
            item_skip_bool=True,
        ):
            count += 1
            assert isinstance(future, SkippedFuture)
            assert future.skip_info == skip_info
        assert items == count


def test_concurrent_futures_processes_executor_cancel_as_completed():
    items = 100
    with Executor(concurrency="processes", max_workers=2) as executor:
        # abort
        for future in executor.as_completed(
            _dummy_process, range(items), fkwargs=dict(sleep=2)
        ):
            assert future.result()
            executor.cancel()
            break

        assert not executor.running_futures


def test_concurrent_futures_processes_executor_map():
    items = list(range(10))
    with Executor(concurrency="processes") as executor:
        result = executor.map(_dummy_process, items)
        assert [i + 1 for i in items] == result


def test_concurrent_futures_threads_executor_as_completed():
    items = 100
    with Executor(concurrency="threads", max_workers=2) as executor:
        # abort
        for future in executor.as_completed(
            _dummy_process, range(items), fkwargs=dict(sleep=2)
        ):
            assert future.result()
            executor.cancel()
            break

        assert not executor.running_futures


def test_concurrent_futures_threads_executor_as_completed_skip():
    items = 100
    skip_info = "foo"
    with Executor(concurrency="threads", max_workers=2) as executor:
        count = 0
        for future in executor.as_completed(
            _dummy_process,
            [(i, True, skip_info) for i in range(items)],
            item_skip_bool=True,
            fkwargs=dict(sleep=2),
        ):
            count += 1
            assert isinstance(future, SkippedFuture)
            assert future.skip_info == skip_info
        assert items == count


def test_concurrent_futures_threads_executor_map():
    items = list(range(10))
    with Executor(concurrency="threads") as executor:
        result = executor.map(_dummy_process, items)
        assert [i + 1 for i in items] == result


def test_dask_executor_as_completed():
    items = 100
    with Executor(concurrency="dask", max_workers=2) as executor:
        # abort
        for future in executor.as_completed(
            _dummy_process, range(items), fkwargs=dict(sleep=2)
        ):
            assert future.result()
            executor.cancel()
            break

        assert not executor.running_futures


def test_dask_executor_as_completed_skip():
    items = 100
    skip_info = "foo"
    with Executor(concurrency="dask", max_workers=2) as executor:
        count = 0
        for future in executor.as_completed(
            _dummy_process,
            [(i, True, skip_info) for i in range(items)],
            item_skip_bool=True,
            fkwargs=dict(sleep=2),
        ):
            count += 1
            assert isinstance(future, SkippedFuture)
            assert future.skip_info == skip_info
        assert items == count


@pytest.mark.parametrize("max_submitted_tasks", [1, None])
def test_dask_executor_as_completed_max_tasks(max_submitted_tasks):
    items = 100
    with Executor(concurrency="dask") as executor:
        # abort
        for future in executor.as_completed(
            _dummy_process, range(items), max_submitted_tasks=max_submitted_tasks
        ):
            assert future.result()

        assert not executor.running_futures


def test_concurrent_futures_dask_executor_map():
    items = list(range(10))
    with Executor(concurrency="dask") as executor:
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
