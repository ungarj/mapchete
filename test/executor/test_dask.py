from concurrent.futures._base import CancelledError

from dask.delayed import delayed
import pytest

import mapchete
from mapchete.config.models import DaskSettings
from mapchete.enums import Concurrency
from mapchete.errors import MapcheteTaskFailed


@pytest.mark.parametrize("process_graph", [True, False])
def test_process_exception_tile_dask(cleantopo_br, process_error_py, process_graph):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(
                mp.execute(
                    tile=(5, 0, 0),
                    concurrency=Concurrency.dask,
                    dask_settings=DaskSettings(process_graph=process_graph),
                )
            )


@pytest.mark.parametrize("process_graph", [True, False])
def test_process_exception_zoom_dask(cleantopo_br, process_error_py, process_graph):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(
                mp.execute(
                    zoom=5,
                    concurrency=Concurrency.dask,
                    dask_settings=DaskSettings(process_graph=process_graph),
                )
            )


def test_dask_cancellederror(dask_executor, items=10):
    def raise_cancellederror(*args, **kwargs):
        raise CancelledError()

    with pytest.raises(CancelledError):
        list(dask_executor.as_completed(raise_cancellederror, range(items)))


def test_compute_task_graph(dask_executor):
    for future in dask_executor.compute_task_graph(
        dask_collection=[delayed(str)(number) for number in range(10)]
    ):
        assert future.result()
