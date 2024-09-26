import pytest
from pytest_lazyfixture import lazy_fixture

from mapchete.io.vector import fiona_open


@pytest.mark.parametrize("path", [lazy_fixture("mp_tmpdir")])
@pytest.mark.parametrize("in_memory", [True, False])
def test_fiona_open_write(path, in_memory, landpoly):
    path = path / f"test_fiona_write-{in_memory}.tif"
    with fiona_open(landpoly) as src:
        with fiona_open(path, "w", in_memory=in_memory, **src.profile) as dst:
            dst.writerecords(src)
    assert path.exists()
    with fiona_open(path) as src:
        written = list(src)
        assert written


@pytest.mark.integration
@pytest.mark.parametrize("path", [lazy_fixture("mp_s3_tmpdir")])
@pytest.mark.parametrize("in_memory", [True, False])
def test_fiona_open_write_remote(path, in_memory, landpoly):
    test_fiona_open_write(path, in_memory, landpoly)
