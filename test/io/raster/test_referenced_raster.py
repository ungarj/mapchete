import numpy as np
import numpy.ma as ma
import pytest
from pytest_lazyfixture import lazy_fixture

from mapchete.io.raster.open import rasterio_open
from mapchete.io.raster.referenced_raster import ReferencedRaster


def test_referencedraster_meta(s2_band):
    rr = ReferencedRaster.from_file(s2_band)
    meta = rr.meta
    for k in [
        "driver",
        "dtype",
        "nodata",
        "width",
        "height",
        "count",
        "crs",
        "transform",
    ]:
        assert k in meta


@pytest.mark.parametrize("masked", [True, False])
@pytest.mark.parametrize("grid", [lazy_fixture("s2_band_tile")])
def test_referencedraster_from_file(s2_band, masked, grid):
    rr = ReferencedRaster.from_file(s2_band, grid=grid, masked=masked)
    if masked:
        assert isinstance(rr.array, ma.MaskedArray)
    else:
        assert not isinstance(rr.array, ma.MaskedArray)
        assert isinstance(rr.array, np.ndarray)
    if grid:
        assert rr.array.shape[1:] == grid.shape


def test_referencedraster_from_array_like(s2_band):
    rr = ReferencedRaster.from_file(s2_band)
    assert ReferencedRaster.from_array_like(rr)
    assert ReferencedRaster.from_array_like(rr.data, transform=rr.transform, crs=rr.crs)


def test_referencedraster_from_array_like_errors(s2_band):
    with pytest.raises(TypeError):
        ReferencedRaster.from_array_like("foo")  # type: ignore

    rr = ReferencedRaster.from_file(s2_band)
    with pytest.raises(ValueError):
        ReferencedRaster.from_array_like(rr.data)
    with pytest.raises(ValueError):
        ReferencedRaster.from_array_like(rr.data, transform=rr.transform)
    with pytest.raises(ValueError):
        ReferencedRaster.from_array_like(rr.data, crs=rr.crs)


def test_referencedraster_array_interface(s2_band):
    rr = ReferencedRaster.from_file(s2_band)
    assert isinstance(ma.array(rr), ma.MaskedArray)


@pytest.mark.parametrize("indexes", [None, 1, [1]])
def test_referencedraster_get_band_indexes(s2_band, indexes):
    rr = ReferencedRaster.from_file(s2_band)
    assert rr.get_band_indexes(indexes) == [1]


@pytest.mark.parametrize("indexes", [None, 1, [1]])
def test_referencedraster_read_band(s2_band, indexes):
    rr = ReferencedRaster.from_file(s2_band)
    assert rr.read(indexes).any()


@pytest.mark.parametrize("indexes", [None, 1, [1]])
def test_referencedraster_read_tile_band(s2_band, indexes, s2_band_tile):
    rr = ReferencedRaster.from_file(s2_band)
    assert rr.read(indexes, grid=s2_band_tile).any()


@pytest.mark.parametrize("dims", [2, 3])
def test_referencedraster_to_file(s2_band, mp_tmpdir, dims):
    rr = ReferencedRaster.from_file(s2_band)
    if dims == 2:
        rr.data = rr.data[0]
    out_file = mp_tmpdir / "test.tif"
    rr.to_file(out_file)
    with rasterio_open(out_file) as src:
        assert src.read(masked=True).any()
