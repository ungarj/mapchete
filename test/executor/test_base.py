import time

import numpy.ma as ma
import pytest

import mapchete
from mapchete.enums import Concurrency
from mapchete.errors import MapcheteTaskFailed
from mapchete.executor.base import run_func_with_profilers
from mapchete.executor.future import MFuture
from mapchete.executor.types import Profiler, Result
from mapchete.io.raster.read import read_raster_no_crs
from mapchete.processing.profilers.memory import measure_memory
from mapchete.processing.profilers.requests import measure_requests
from mapchete.processing.profilers.time import measure_time


def _dummy_process(i, sleep=0):
    list(range(1_000_000))
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
    assert not executor.futures


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
    assert not executor.futures


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
        assert future.skipped
        assert future.skip_info == skip_info
        count += 1
    assert not executor.futures
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
    assert not executor.futures


@pytest.mark.parametrize(
    "executor_fixture",
    ["sequential_executor", "dask_executor", "processes_executor", "threads_executor"],
)
def test_map(executor_fixture, request):
    executor = request.getfixturevalue(executor_fixture)

    items = list(range(10))
    result = executor.map(_dummy_process, items)
    assert [i + 1 for i in items] == result


def test_process_exception_tile(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(mp.execute(tile=(5, 0, 0), concurrency=Concurrency.processes))


@pytest.mark.parametrize(
    "path_fixture",
    ["raster_4band"],
)
def test_profile_wrapper(request, path_fixture):
    path = request.getfixturevalue(path_fixture)
    result = run_func_with_profilers(
        read_raster_no_crs,
        path,
        profilers=[
            Profiler(name="time", decorator=measure_time),
            Profiler(name="memory", decorator=measure_memory),
        ],
    )
    assert isinstance(result, Result)
    assert isinstance(result.output, ma.MaskedArray)
    assert isinstance(result.profiling, dict)
    assert len(result.profiling) == 2
    assert result.profiling["time"].elapsed > 0
    assert result.profiling["memory"].max_allocated > 0
    assert result.profiling["memory"].total_allocated > 0


@pytest.mark.skip(
    reason="this test is flaky and the feature is also tested in "
    "test_processing_profilers.py::test_requests_return_result"
)
@pytest.mark.integration
@pytest.mark.parametrize(
    "path_fixture",
    [
        "raster_4band_s3",
        "raster_4band_aws_s3",
        "raster_4band_http",
        "raster_4band_secure_http",
    ],
)
def test_profile_wrapper_requests(request, path_fixture):
    path = request.getfixturevalue(path_fixture)

    # setting this is important, otherwise GDAL will cache the file
    # and thus measure_requests will not be able to count requests
    # if the file has already been opened in a prior tests
    with path.rio_env(opts=dict(CPL_VSIL_CURL_NON_CACHED=path.as_gdal_str())):
        result = run_func_with_profilers(
            read_raster_no_crs,
            path,
            profilers=[
                Profiler(name="time", decorator=measure_time),
                Profiler(name="requests", decorator=measure_requests),
                Profiler(name="memory", decorator=measure_memory),
            ],
        )
    assert isinstance(result, Result)
    assert isinstance(result.output, ma.MaskedArray)
    assert isinstance(result.profiling, dict)
    assert len(result.profiling) == 3
    assert result.profiling["time"].elapsed > 0
    assert result.profiling["memory"].max_allocated > 0
    assert result.profiling["memory"].total_allocated > 0
    assert result.profiling["requests"].get_count > 0
    assert result.profiling["requests"].get_bytes > 0


@pytest.mark.parametrize(
    "executor_fixture",
    ["sequential_executor", "dask_executor", "processes_executor", "threads_executor"],
)
def test_profiling(executor_fixture, request):
    executor = request.getfixturevalue(executor_fixture)

    # add profiler
    executor.add_profiler(name="time", decorator=measure_time)

    items = list(range(10))
    for future in executor.as_completed(_dummy_process, items):
        assert isinstance(future, MFuture)
        output = future.result()
        assert not isinstance(output, Result)
        assert "time" in future.profiling
