from typing import Generator


class Job:
    """Wraps the output of a processing function into a generator with known length."""

    def __init__(
        self,
        func: Generator,
        *fargs: dict,
        as_iterator: bool = False,
        total: int = None,
        **fkwargs: dict
    ):
        self.func = func
        self.fargs = fargs
        self.fkwargs = fkwargs
        self._total = total
        self._as_iterator = as_iterator
        if not as_iterator:
            list(self.func(*self.fargs, **self.fkwargs))

    def __len__(self):
        return self._total

    def __iter__(self):
        if not self._as_iterator:
            raise TypeError("initialize with 'as_iterator=True'")
        return self.func(*self.fargs, **self.fkwargs)


def empty_callback(*args, **kwargs):
    pass
