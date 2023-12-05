import pickle

import numpy.ma as ma

from mapchete.processing.profilers.memory import MeasuredMemory
from mapchete.processing.profilers.requests import MeasuredRequests
from mapchete.processing.profilers.time import MeasuredTime
from mapchete.processing.types import TaskInfo
from mapchete.tile import BufferedTilePyramid

task_info_kwargs = dict(
    id="foo",
    output=ma.ones((1, 1, 1)),
    processed=True,
    process_msg="foo",
    written=True,
    write_msg="bar",
    profiling={
        "memory": MeasuredMemory(),
        "requests": MeasuredRequests(),
        "time": MeasuredTime(),
    },
    tile=BufferedTilePyramid("geodetic").tile(5, 5, 5),
)


def test_task_info_pickle():
    task_info = TaskInfo(**task_info_kwargs)
    # pickle and unpickle
    reloaded = pickle.loads(pickle.dumps(task_info))
    assert reloaded == task_info
