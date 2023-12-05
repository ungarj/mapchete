import time

from mapchete.pretty import pretty_seconds


class Timer:
    """
    Context manager to facilitate timing code.

    Examples
    --------
    >>> with Timer() as t:
            ...  # some longer running code
    >>> print(t)  # prints elapsed time

    based on http://preshing.com/20110924/timing-your-code-using-pythons-with-statement/
    """

    def __init__(self, elapsed=0.0, str_round=3):
        self._elapsed = elapsed
        self._str_round = str_round
        self.start = None
        self.end = None

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self._elapsed = self.end - self.start

    def __lt__(self, other):
        return self._elapsed < other._elapsed

    def __le__(self, other):
        return self._elapsed <= other._elapsed

    def __eq__(self, other):
        return self._elapsed == other._elapsed

    def __ne__(self, other):
        return self._elapsed != other._elapsed

    def __ge__(self, other):
        return self._elapsed >= other._elapsed

    def __gt__(self, other):
        return self._elapsed > other._elapsed

    def __add__(self, other):
        return Timer(elapsed=self._elapsed + other._elapsed)

    def __sub__(self, other):
        return Timer(elapsed=self._elapsed - other._elapsed)

    def __repr__(self):
        return "Timer(start=%s, end=%s, elapsed=%s)" % (
            self.start,
            self.end,
            self.__str__(),
        )

    def __str__(self):
        return pretty_seconds(self.elapsed, self._str_round)

    @property
    def elapsed(self):
        return (
            time.time() - self.start if self.start and not self.end else self._elapsed
        )
