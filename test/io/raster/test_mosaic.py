from itertools import product
import numpy as np
import pytest
from shapely import box, unary_union

from mapchete.bounds import Bounds
from mapchete.io.raster.mosaic import create_mosaic
from mapchete.io.raster.referenced_raster import ReferencedRaster
from mapchete.tile import BufferedTilePyramid


def test_create_mosaic_errors():
    """Check error handling of create_mosaic()."""
    tp_geo = BufferedTilePyramid("geodetic")
    tp_mer = BufferedTilePyramid("mercator")
    geo_tile = tp_geo.tile(1, 0, 0)
    geo_tile_data = np.ndarray(geo_tile.shape)
    mer_tile = tp_mer.tile(1, 1, 0)
    mer_tile_data = np.ndarray(mer_tile.shape)
    # tiles error
    with pytest.raises(TypeError):
        create_mosaic("invalid tiles")  # type: ignore
    with pytest.raises(TypeError):
        create_mosaic(["invalid tiles"])  # type: ignore
    # CRS error
    with pytest.raises(ValueError):
        create_mosaic([(geo_tile, geo_tile_data), (mer_tile, mer_tile_data)])  # type: ignore
    # zoom error
    with pytest.raises(ValueError):
        diff_zoom = tp_geo.tile(2, 1, 0)
        diff_zoom_data = np.ndarray(diff_zoom.shape)
        create_mosaic([(geo_tile, geo_tile_data), (diff_zoom, diff_zoom_data)])  # type: ignore
    # tile data error
    with pytest.raises(TypeError):
        # for one tile
        create_mosaic([(geo_tile, None)])  # type: ignore
    with pytest.raises(TypeError):
        # for multiple tiles
        create_mosaic([(geo_tile, None), (geo_tile, None)])  # type: ignore
    # tile data type error
    with pytest.raises(TypeError):
        diff_type = tp_geo.tile(1, 1, 0)
        diff_type_data = np.ndarray(diff_zoom.shape).astype("int")
        create_mosaic([(geo_tile, geo_tile_data), (diff_type, diff_type_data)])  # type: ignore
    # no tiles
    with pytest.raises(ValueError):
        create_mosaic(tiles=[])  # type: ignore


@pytest.mark.parametrize("pixelbuffer", [0, 10])
def test_create_mosaic(pixelbuffer):
    """Create mosaic from tiles."""
    tp = BufferedTilePyramid("geodetic")
    # quick return mosaic if there is just one tile
    tile = tp.tile(3, 3, 3)
    data = np.ones(tile.shape)
    mosaic = create_mosaic([(tile, data)])
    assert isinstance(mosaic, ReferencedRaster)
    assert np.array_equal(data, mosaic.data)
    assert tile.affine == mosaic.affine
    zoom = 5
    # multiple tiles on top left corner of tile matrix
    tp = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles_arrays = [
        (tp.tile(zoom, row, col), np.ones(tp.tile(zoom, row, col).shape))
        for row, col in product(range(4), range(4))
    ]
    # 4x4 top left tiles from zoom 5 equal top left tile from zoom 3
    # also use tile generator
    mosaic = create_mosaic((tile_array for tile_array in tiles_arrays))
    assert isinstance(mosaic, ReferencedRaster)
    assert np.all(np.where(mosaic.data == 1, True, False))
    mosaic_bbox = box(
        mosaic.affine[2],
        mosaic.affine[5] + mosaic.data.shape[1] * mosaic.affine[4],
        mosaic.affine[2] + mosaic.data.shape[2] * mosaic.affine[0],
        mosaic.affine[5],
    )
    control_bbox = box(*unary_union([t.bbox for t, _ in tiles_arrays]).bounds)
    assert mosaic_bbox.equals(control_bbox)
    # multiple tiles on bottom right corner of tile matrix
    tp = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    tiles_arrays = [
        (tp.tile(zoom, row, col), np.ones(tp.tile(zoom, row, col).shape))
        for row, col in product(
            range(tp.matrix_height(zoom) - 4, tp.matrix_height(zoom)),
            range(tp.matrix_width(zoom) - 4, tp.matrix_width(zoom)),
        )
    ]
    # 4x4 top left tiles from zoom 5 equal top left tile from zoom 3
    # also use tile generator
    mosaic = create_mosaic((t for t in tiles_arrays))
    assert isinstance(mosaic, ReferencedRaster)
    assert np.all(np.where(mosaic.data == 1, True, False))
    mosaic_bbox = box(
        mosaic.affine[2],
        mosaic.affine[5] + mosaic.data.shape[1] * mosaic.affine[4],
        mosaic.affine[2] + mosaic.data.shape[2] * mosaic.affine[0],
        mosaic.affine[5],
    )
    control_bbox = box(*unary_union([t.bbox for t, _ in tiles_arrays]).bounds)
    assert mosaic_bbox.equals(control_bbox)


def test_create_mosaic_antimeridian():
    """Create mosaic using tiles on opposing antimeridian sides."""
    zoom = 5
    row = 0
    pixelbuffer = 5
    tp = BufferedTilePyramid("geodetic", pixelbuffer=pixelbuffer)
    west = tp.tile(zoom, row, 0)
    east = tp.tile(zoom, row, tp.matrix_width(zoom) - 1)
    mosaic = create_mosaic(
        [
            (west, np.ones(west.shape).astype("uint8")),
            (east, np.ones(east.shape).astype("uint8") * 2),
        ]
    )
    assert isinstance(mosaic, ReferencedRaster)

    # Huge array gets initialized because the two tiles are on opposing sides of the
    # projection area. The below test should pass if the tiles are stitched together next
    # to each other.
    assert mosaic.data.shape == (1, west.height, west.width * 2 - 2 * pixelbuffer)
    assert mosaic.data[0][0][0] == 2
    assert mosaic.data[0][0][-1] == 1

    # If tiles from opposing sides from Antimeridian are mosaicked it will happen that the
    # output mosaic exceeds the CRS bounds (obviously). In such a case the mosaicking
    # function shall make sure that the larger part of the output mosaic shall be inside
    # the CRS bounds.

    # (1) mosaic crosses Antimeridian in the West, larger part is on Western hemisphere:
    tiles_ids = [
        # Western hemisphere tiles
        (zoom, row, 0),
        (zoom, row, 1),
        # Eastern hemisphere tile
        (zoom, row, tp.matrix_width(zoom) - 1),
    ]
    tiles = [
        (tp.tile(*tile_id), np.ones(tp.tile(*tile_id).shape)) for tile_id in tiles_ids
    ]
    mosaic = create_mosaic(tiles)
    control_bounds = Bounds(
        # Eastern tile has to be shifted
        -(360 - tp.tile(*tiles_ids[2]).left),
        tp.tile(*tiles_ids[2]).bottom,
        tp.tile(*tiles_ids[1]).right,
        tp.tile(*tiles_ids[1]).top,
    )
    assert mosaic.bounds == control_bounds

    # (2) mosaic crosses Antimeridian in the West, larger part is on Eastern hemisphere:
    tiles_ids = [
        # Western hemisphere tile
        (zoom, row, 0),
        # Eastern hemisphere tiles
        (zoom, row, tp.matrix_width(zoom) - 1),
        (zoom, row, tp.matrix_width(zoom) - 2),
    ]
    tiles = [
        (tp.tile(*tile_id), np.ones(tp.tile(*tile_id).shape)) for tile_id in tiles_ids
    ]
    mosaic = create_mosaic(tiles)
    control_bounds = Bounds(
        tp.tile(*tiles_ids[2]).left,
        tp.tile(*tiles_ids[2]).bottom,
        # Western tile has to be shifted
        360 + tp.tile(*tiles_ids[0]).right,
        tp.tile(*tiles_ids[0]).top,
    )
    assert mosaic.bounds == control_bounds
