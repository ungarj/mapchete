import numpy as np
import numpy.ma as ma
import pytest
from pytest_lazyfixture import lazy_fixture

from mapchete.io.profiles import COGDeflateProfile
from mapchete.io.raster.open import rasterio_open
from mapchete.io.raster.write import rasterio_write, write_raster_window
from mapchete.path import path_exists
from mapchete.tile import BufferedTilePyramid


def test_write_raster_window_errors():
    """Basic output format writing."""
    tile = BufferedTilePyramid("geodetic").tile(5, 5, 5)
    data = ma.masked_array(np.ndarray((1, 1)))
    profile = {}
    path = ""
    # in_tile
    with pytest.raises(TypeError):
        write_raster_window(
            in_grid="invalid tile",  # type: ignore
            in_data=data,
            out_profile=profile,
            out_grid=tile,
            out_path=path,
        )
    # out_tile
    with pytest.raises(TypeError):
        write_raster_window(
            in_grid=tile,
            in_data=data,
            out_profile=profile,
            out_grid="invalid tile",  # type: ignore
            out_path=path,
        )
    # in_data
    with pytest.raises(TypeError):
        write_raster_window(
            in_grid=tile,
            in_data="invalid data",  # type: ignore
            out_profile=profile,
            out_grid=tile,
            out_path=path,
        )
    # out_profile
    with pytest.raises(TypeError):
        write_raster_window(
            in_grid=tile,
            in_data=data,
            out_profile="invalid profile",  # type: ignore
            out_grid=tile,
            out_path=path,
        )
    # out_path
    with pytest.raises(TypeError):
        write_raster_window(
            in_grid=tile,
            in_data=data,
            out_profile=profile,
            out_grid=tile,
            out_path=999,  # type: ignore
        )
    # cannot write
    with pytest.raises(ValueError):
        write_raster_window(
            in_grid=tile,
            in_data=data,
            out_profile=profile,
            out_grid=tile,
            out_path="/invalid_path",
        )


@pytest.mark.parametrize("path", [lazy_fixture("mp_tmpdir")])
@pytest.mark.parametrize("dtype", [np.uint8, np.float32])
@pytest.mark.parametrize("in_memory", [True, False])
def test_rasterio_write(path, dtype, in_memory):
    arr = np.ones((1, 256, 256)).astype(dtype)
    count, width, height = arr.shape
    path = path / f"test_rasterio_write-{str(dtype)}-{in_memory}.tif"
    with rasterio_open(
        path,
        "w",
        in_memory=in_memory,
        count=count,
        width=width,
        height=height,
        crs="EPSG:4326",
        **COGDeflateProfile(dtype=dtype),
    ) as dst:
        dst.write(arr)
    assert path_exists(path)
    with rasterio_open(path) as src:
        written = src.read()
        assert np.array_equal(arr, written)


@pytest.mark.integration
@pytest.mark.parametrize("path", [lazy_fixture("mp_s3_tmpdir")])
@pytest.mark.parametrize("dtype", [np.uint8, np.float32])
@pytest.mark.parametrize("in_memory", [True, False])
def test_rasterio_write_remote(path, dtype, in_memory):
    test_rasterio_write(path, dtype, in_memory)


@pytest.mark.integration
@pytest.mark.parametrize("in_memory", [True, False])
def test_rasterio_write_remote_exception(mp_s3_tmpdir, in_memory):
    path = mp_s3_tmpdir / "temp.tif"
    with pytest.raises(ValueError):
        # raise exception on purpose
        with rasterio_write(
            path,
            "w",
            in_memory=in_memory,
            count=3,
            width=256,
            height=256,
            crs="EPSG:4326",
            **COGDeflateProfile(dtype="uint8"),
        ):
            raise ValueError()
