import pytest

from mapchete.executor import MFuture


def test_mfuture():
    def task(*args, **kwargs):
        return True

    def failing_task(*args, **kwargs):
        raise RuntimeError()

    future = MFuture.from_func(task, fargs=(1, True), fkwargs=dict(foo="bar"))
    assert future.result()
    assert not future.exception()

    future = MFuture.from_func(failing_task, fargs=(1, True), fkwargs=dict(foo="bar"))
    with pytest.raises(RuntimeError):
        future.result()
    assert future.exception()
