import pytest

from mapchete.pretty import pretty_bytes, pretty_seconds


@pytest.mark.parametrize(
    "bytes,string",
    [
        (2_000, "KiB"),
        (2_000_000, "MiB"),
        (2_000_000_000, "GiB"),
    ],
)
def test_pretty_bytes(bytes, string):
    assert string in pretty_bytes(bytes)


@pytest.mark.parametrize(
    "seconds,string",
    [
        (61, "m"),
        (3601, "h"),
    ],
)
def test_pretty_seconds(seconds, string):
    assert string in pretty_seconds(seconds)
