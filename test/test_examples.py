import pytest
from pytest import lazy_fixture

from mapchete.enums import Concurrency
from mapchete.testing import ProcessFixture


@pytest.mark.parametrize(
    "process_fixture",
    [
        lazy_fixture("example_clip"),
        lazy_fixture("example_contours"),
        lazy_fixture("example_custom_grid"),
        lazy_fixture("example_file_groups"),
        lazy_fixture("example_hillshade"),
    ],
)
def test_example(process_fixture: ProcessFixture):
    """Runs configuration only on first process tile"""
    with process_fixture.mp() as mp:
        assert list(
            mp.execute(
                tile=process_fixture.first_process_tile(), concurrency=Concurrency.none
            )
        )
