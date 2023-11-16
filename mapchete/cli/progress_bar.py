from typing import Optional

from tqdm import tqdm

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
        **kwargs
    ):
        if progress:
            if self._pbar.total is None or self._pbar.total != progress.total:
                self._pbar.reset(progress.total)

            self._pbar.update(progress.current - self._pbar.n)

        if self.print_messages and message:
            tqdm.write(message)
