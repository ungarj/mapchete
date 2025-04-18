import numpy as np
import numpy.ma as ma
import pytest
from shapely import GeometryCollection

from mapchete.io.raster.array import (
    clip_array_with_vector,
    extract_from_array,
    prepare_array,
    resample_from_array,
)
from mapchete.io.raster.referenced_raster import ReferencedRaster
from mapchete.tile import BufferedTilePyramid


def test_extract_from_array():
    """Extract subdata from array."""
    in_tile = BufferedTilePyramid("geodetic", metatiling=4).tile(5, 5, 5)
    shape = (in_tile.shape[0] // 2, in_tile.shape[1])
    data = ma.masked_array(np.concatenate([np.ones(shape), np.ones(shape) * 2]))
    # intersecting at top
    out_tile = BufferedTilePyramid("geodetic").tile(5, 20, 20)
    out_array = extract_from_array(
        array=data, in_affine=in_tile.affine, out_tile=out_tile
    )
    assert isinstance(out_array, np.ndarray)
    assert np.all(np.where(out_array == 1, True, False))
    # intersecting at bottom
    out_tile = BufferedTilePyramid("geodetic").tile(5, 22, 20)
    out_array = extract_from_array(
        array=data, in_affine=in_tile.affine, out_tile=out_tile
    )
    assert isinstance(out_array, np.ndarray)
    assert np.all(np.where(out_array == 2, True, False))
    # not intersecting
    out_tile = BufferedTilePyramid("geodetic").tile(5, 15, 20)
    with pytest.raises(ValueError):
        out_array = extract_from_array(
            array=data, in_affine=in_tile.affine, out_tile=out_tile
        )


def test_resample_from_array():
    """Resample array using rasterio reproject()."""
    in_tile = BufferedTilePyramid("geodetic").tile(5, 5, 5)
    in_data = np.ones(in_tile.shape)
    # tile from next toom level
    out_tile = BufferedTilePyramid("geodetic").tile(6, 10, 10)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile)
    assert isinstance(out_array, ma.masked_array)
    assert np.all(np.where(out_array == 1, True, False))
    # not intersecting tile
    out_tile = BufferedTilePyramid("geodetic").tile(7, 0, 0)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile)
    assert isinstance(out_array, ma.masked_array)
    assert out_array.mask.all()
    # data as tuple
    in_data = (np.ones(in_tile.shape[1:]),)
    out_tile = BufferedTilePyramid("geodetic").tile(6, 10, 10)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile)
    # deprecated
    resample_from_array(in_data, in_tile.affine, out_tile, nodata=-9999)
    # keep 2D
    in_data = (np.ones(in_tile.shape[1:]),)
    out_tile = BufferedTilePyramid("geodetic").tile(6, 10, 10)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile, keep_2d=True)
    assert out_array.shape == (256, 256)
    # not keep 2D
    in_data = (np.ones(in_tile.shape[1:]),)
    out_tile = BufferedTilePyramid("geodetic").tile(6, 10, 10)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile, keep_2d=False)
    assert out_array.shape == (1, 256, 256)
    # Test ma.MaskedArray input
    in_data = (
        ma.MaskedArray(
            data=np.ones(in_tile.shape[1:]),
            mask=np.invert(np.ones(in_tile.shape[1:]), dtype="bool", casting="unsafe"),
        ),
    )
    out_tile = BufferedTilePyramid("geodetic").tile(6, 10, 10)
    out_array = resample_from_array(in_data, in_tile.affine, out_tile)
    assert out_array.shape == (1, 256, 256)
    # errors
    with pytest.raises(TypeError):
        in_data = "invalid_type"
        resample_from_array(in_data, in_tile.affine, out_tile)  # type: ignore
    with pytest.raises(TypeError):
        in_data = np.ones(in_tile.shape[0])
        resample_from_array(in_data, in_tile.affine, out_tile)


def test_prepare_array_iterables():
    """Convert iterable data into a proper array."""
    # input is iterable
    # iterable contains arrays
    data = [np.zeros((1, 1))]
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # iterable contains masked arrays
    data = [ma.empty((1, 1))]
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # iterable contains masked arrays with full mask
    data = [ma.masked_array(data=np.ones((1, 1)), mask=np.ones((1, 1)))]
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)


def test_prepare_array_maskedarrays():
    """Convert masked array data into a proper array."""
    # input is ma.masked_array
    data = ma.empty((1, 1, 1))
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # input is ma.masked_array with full mask
    data = ma.masked_array(data=np.ones((1, 1, 1)), mask=np.ones((1, 1, 1)))
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)


def test_prepare_array_ndarrays():
    """Convert ndarray data into a proper array."""
    # input is np.ndarray
    data = np.zeros((1, 1, 1))
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # input is 2D np.ndarray
    data = np.zeros((1, 1))
    # output ndarray
    output = prepare_array(data, masked=False)
    assert isinstance(output, np.ndarray)
    assert not isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)
    # output masked array
    output = prepare_array(data)
    assert isinstance(output, ma.masked_array)
    assert output.shape == (1, 1, 1)


def test_prepare_array_errors():
    """Convert ndarray data into a proper array."""
    # input is iterable
    data = [None]
    with pytest.raises(ValueError):
        prepare_array(data)  # type: ignore

    # input is not array
    data = 5
    with pytest.raises(ValueError):
        prepare_array(data)  # type: ignore


def test_clip_array_with_vector(s2_band, s2_band_tile):
    rr = ReferencedRaster.from_file(s2_band)

    geometries = [dict(geometry=s2_band_tile.bbox)]
    out = clip_array_with_vector(rr.data, rr.affine, geometries)
    assert out.mask.all()


def test_clip_array_with_vector_geometrycollection(s2_band, s2_band_tile):
    rr = ReferencedRaster.from_file(s2_band)

    geometries = [dict(geometry=GeometryCollection([s2_band_tile.bbox]))]
    out = clip_array_with_vector(rr.data, rr.affine, geometries)
    assert out.mask.all()


def test_clip_array_with_vector_2dim(s2_band, s2_band_tile):
    rr = ReferencedRaster.from_file(s2_band)

    geometries = [dict(geometry=s2_band_tile.bbox)]
    out = clip_array_with_vector(rr.data[0], rr.affine, geometries)
    assert out.mask.all()


@pytest.mark.parametrize("inverted", [True, False])
@pytest.mark.parametrize("clip_buffer", [0, 0.1])
def test_clip_array_with_vector_empty_geometries(s2_band, inverted, clip_buffer):
    rr = ReferencedRaster.from_file(s2_band)

    geometries = [dict(geometry=GeometryCollection())]
    out = clip_array_with_vector(
        rr.data, rr.affine, geometries, inverted=inverted, clip_buffer=clip_buffer
    )
    if inverted:
        assert not out.mask.all()
    else:
        assert out.mask.all()
