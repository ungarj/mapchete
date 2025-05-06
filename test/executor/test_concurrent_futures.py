import pytest
import mapchete
from mapchete.enums import Concurrency
from mapchete.errors import MapcheteTaskFailed


def test_process_exception_zoom(mp_tmpdir, cleantopo_br, process_error_py):
    """Assert process exception is raised."""
    config = cleantopo_br.dict
    config.update(process=process_error_py)
    with mapchete.open(config) as mp:
        with pytest.raises(MapcheteTaskFailed):
            list(mp.execute(zoom=5, concurrency=Concurrency.processes))
