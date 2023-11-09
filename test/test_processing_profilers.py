import numpy.ma as ma
import pytest

from mapchete.io.raster import read_raster_no_crs
from mapchete.processing.profilers.memory import (
    MeasuredMemory,
    MemoryTracker,
    measure_memory,
)
from mapchete.processing.profilers.requests import MeasuredRequests, measure_requests


def test_memory_return_result(raster_4band):
    @measure_memory()
    def _decorated(path):
        return read_raster_no_crs(path)

    retval, result = _decorated(raster_4band)
    assert isinstance(retval, ma.MaskedArray)
    assert isinstance(result, MeasuredMemory)
    assert result.allocations > 0
    assert result.max_allocated > 0
    assert result.total_allocated > 0


def test_memory_not_return_result(raster_4band):
    @measure_memory(add_to_return=False)
    def _decorated(path):
        return read_raster_no_crs(path)

    retval = _decorated(raster_4band)
    assert isinstance(retval, ma.MaskedArray)


def test_memory_context_manager(raster_4band, mp_tmpdir):
    result_file = mp_tmpdir / "memtracker_result.bin"

    with MemoryTracker(output_file=result_file) as memory_tracker:
        output = read_raster_no_crs(raster_4band)

    assert isinstance(output, ma.MaskedArray)

    assert memory_tracker.allocations > 0
    assert memory_tracker.max_allocated > 0
    assert memory_tracker.total_allocated > 0
    assert result_file.exists()
    assert result_file.open("rb").read()


@pytest.mark.integration
def test_requests_return_result(raster_4band_s3):
    @measure_requests()
    def _decorated(path):
        return read_raster_no_crs(path)

    retval, result = _decorated(raster_4band_s3)
    assert isinstance(retval, ma.MaskedArray)
    assert isinstance(result, MeasuredRequests)

    # no heads given?
    # assert result.head_count > 0
    assert result.get_count > 0
    assert result.get_bytes > 0


@pytest.mark.integration
def test_requests_not_return_result(raster_4band_s3):
    @measure_requests(add_to_return=False)
    def _decorated(path):
        return read_raster_no_crs(path)

    retval = _decorated(raster_4band_s3)
    assert isinstance(retval, ma.MaskedArray)
