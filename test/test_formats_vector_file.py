from typing import Union

import pytest

from mapchete import VectorInput
from mapchete.testing import ProcessFixture


@pytest.mark.parametrize(
    "validity_check",
    [True, False],
)
@pytest.mark.parametrize(
    "clip_to_crs_bounds",
    [True, False],
)
@pytest.mark.parametrize(
    "pixelbuffer",
    [0, 10],
)
def test_read_union_geometry(
    flatgeobuf: ProcessFixture,
    validity_check: bool,
    clip_to_crs_bounds: bool,
    pixelbuffer: int,
):
    vector_input: VectorInput = flatgeobuf.process_mp().open("file1")  # type: ignore
    assert vector_input.read_union_geometry(
        validity_check=validity_check,
        clip_to_crs_bounds=clip_to_crs_bounds,
        pixelbuffer=pixelbuffer,
    ).is_valid


def test_read_union_geometry_empty(flatgeobuf: ProcessFixture):
    vector_input: VectorInput = flatgeobuf.process_mp(tile=(4, 0, 0)).open("file1")  # type: ignore
    assert vector_input.read_union_geometry().is_empty


@pytest.mark.parametrize(
    "validity_check",
    [True, False],
)
def test_read_as_raster_mask_validity_check(
    flatgeobuf: ProcessFixture, validity_check: bool
):
    vector_input: VectorInput = flatgeobuf.process_mp().open("file1")  # type: ignore
    mask = vector_input.read_as_raster_mask(validity_check=validity_check)
    assert not mask.all()


@pytest.mark.parametrize(
    "clip_to_crs_bounds",
    [True, False],
)
def test_read_as_raster_mask_clip_to_crs_bounds(
    flatgeobuf: ProcessFixture, clip_to_crs_bounds: bool
):
    vector_input: VectorInput = flatgeobuf.process_mp().open("file1")  # type: ignore
    mask = vector_input.read_as_raster_mask(clip_to_crs_bounds=clip_to_crs_bounds)
    assert not mask.all()


@pytest.mark.parametrize(
    "pixelbuffer",
    [0, 100],
)
def test_read_as_raster_mask_pixelbuffer(flatgeobuf: ProcessFixture, pixelbuffer: bool):
    vector_input: VectorInput = flatgeobuf.process_mp().open("file1")  # type: ignore
    mask = vector_input.read_as_raster_mask(pixelbuffer=pixelbuffer)
    assert not mask.all()


@pytest.mark.parametrize(
    "all_touched",
    [True, False],
)
def test_read_as_raster_mask_all_touched(flatgeobuf: ProcessFixture, all_touched: bool):
    vector_input: VectorInput = flatgeobuf.process_mp().open("file1")  # type: ignore
    mask = vector_input.read_as_raster_mask(all_touched=all_touched)
    assert not mask.all()


@pytest.mark.parametrize(
    "invert",
    [True, False],
)
def test_read_as_raster_mask_invert(flatgeobuf: ProcessFixture, invert: bool):
    vector_input: VectorInput = flatgeobuf.process_mp().open("file1")  # type: ignore
    mask = vector_input.read_as_raster_mask(invert=invert)
    assert not mask.all()


@pytest.mark.parametrize(
    "band_count",
    [None, 1, 3],
)
def test_read_as_raster_mask_band_count(
    flatgeobuf: ProcessFixture, band_count: Union[int, None]
):
    vector_input: VectorInput = flatgeobuf.process_mp().open("file1")  # type: ignore
    mask = vector_input.read_as_raster_mask(band_count=band_count)
    assert not mask.all()
    if band_count:
        assert mask.ndim > 2


@pytest.mark.parametrize(
    "invert",
    [True, False],
)
def test_read_as_raster_mask_invert_empty(flatgeobuf: ProcessFixture, invert: bool):
    vector_input: VectorInput = flatgeobuf.process_mp(tile=(4, 0, 0)).open("file1")  # type: ignore
    mask = vector_input.read_as_raster_mask(invert=invert)
    assert (mask.all()) == invert
