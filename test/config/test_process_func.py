import pickle

import pytest
from pytest_lazyfixture import lazy_fixture

from mapchete.config.process_func import ProcessFunc


@pytest.mark.parametrize(
    "process_src",
    [
        lazy_fixture("example_process_module"),
        lazy_fixture("example_process_py"),
        lazy_fixture("example_process_text"),
    ],
)
def test_process(process_src, example_custom_process_mapchete):
    mp = example_custom_process_mapchete.process_mp()
    process = ProcessFunc(process_src)
    assert process.name
    assert process(mp) is not None


@pytest.mark.parametrize(
    "process_src",
    [
        lazy_fixture("example_process_module"),
        lazy_fixture("example_process_py"),
        lazy_fixture("example_process_text"),
    ],
)
def test_process_pickle(process_src, example_custom_process_mapchete):
    mp = example_custom_process_mapchete.process_mp()
    process = ProcessFunc(process_src)
    # pickle and unpickle
    reloaded = pickle.loads(pickle.dumps(process))
    assert reloaded(mp) is not None
