from typing import Optional

from tqdm import tqdm

from mapchete.pretty import pretty_bytes, pretty_seconds
from mapchete.processing.types import TaskInfo
from mapchete.protocols import ObserverProtocol
from mapchete.types import Progress


class PBar(ObserverProtocol):
    """Custom progress bar which is used as an observer for commands."""

    print_messages: bool = True
    _pbar = tqdm

    def __init__(self, *args, print_messages: bool = True, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self.print_messages = print_messages

    def __enter__(self):
        self._pbar = tqdm(*self._args, **self._kwargs)
        return self

    def __exit__(self, *args):
        self._pbar.__exit__(*args)

    def update(
        self,
        *args,
        progress: Optional[Progress] = None,
        message: Optional[str] = None,
        task_info: Optional[TaskInfo] = None,
        **kwargs,
    ):
        if progress:
            if self._pbar.total is None or self._pbar.total != progress.total:
                self._pbar.reset(progress.total)

            self._pbar.update(progress.current - self._pbar.n)

        if self.print_messages:
            if task_info:
                msg = f"task {task_info.id}: {task_info.process_msg}"

                if task_info.profiling:
                    msg += profiling_info(task_info)

                tqdm.write(msg)

            if message:
                tqdm.write(message)


def profiling_info(task_info: TaskInfo) -> str:
    profiling_info = []
    if task_info.profiling.get("time"):
        elapsed = task_info.profiling["time"].elapsed
        profiling_info.append(f"time: {pretty_seconds(elapsed)}")
    if task_info.profiling.get("memory"):  # pragma: no cover
        max_allocated = task_info.profiling["memory"].max_allocated
        profiling_info.append(f"max memory usage: {pretty_bytes(max_allocated)}")
    if task_info.profiling.get("memory"):  # pragma: no cover
        head_requests = task_info.profiling["requests"].head_count
        get_requests = task_info.profiling["requests"].get_count
        requests = head_requests + get_requests
        transferred = task_info.profiling["requests"].get_bytes
        profiling_info.append(f"{requests} GET and HEAD requests")
        profiling_info.append(f"{pretty_bytes(transferred)} transferred")
    return f" ({', '.join(profiling_info)})"
