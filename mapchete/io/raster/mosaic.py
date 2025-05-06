import logging
from types import GeneratorType
from typing import Iterable, List, Tuple

import numpy as np
import numpy.ma as ma
from affine import Affine

from mapchete.bounds import Bounds
from mapchete.io.raster.array import bounds_to_ranges, prepare_array
from mapchete.io.raster.referenced_raster import ReferencedRaster
from mapchete.tile import BufferedTile
from mapchete.types import NodataVal

logger = logging.getLogger(__name__)


def create_mosaic(
    tiles_arrays: Iterable[Tuple[BufferedTile, np.ndarray]], nodata: NodataVal = 0
) -> ReferencedRaster:
    """
    Create a mosaic from tiles.

    Tiles must be connected (also possible over Antimeridian), otherwise strange things
    can happen!

    Parameters
    ----------
    tiles : iterable
        an iterable containing tuples of a BufferedTile and an array
    nodata : integer or float
        raster nodata value to initialize the mosaic with (default: 0)

    Returns
    -------
    mosaic : ReferencedRaster
    """
    if isinstance(tiles_arrays, GeneratorType):
        tiles_arrays_list = list(tiles_arrays)
    elif isinstance(tiles_arrays, list):
        tiles_arrays_list = tiles_arrays
    else:
        raise TypeError("tiles must be either a list or generator")
    if not all([isinstance(pair, tuple) for pair in tiles_arrays_list]):
        raise TypeError("tiles items must be tuples")
    if not all(
        [
            all([isinstance(tile, BufferedTile), isinstance(data, np.ndarray)])
            for tile, data in tiles_arrays_list
        ]
    ):
        raise TypeError("tuples must be pairs of BufferedTile and array")
    if len(tiles_arrays_list) == 0:
        raise ValueError("tiles list is empty")

    logger.debug("create mosaic from %s tile(s)", len(tiles_arrays_list))
    # quick return if there is just one tile
    if len(tiles_arrays_list) == 1:
        tile, data = tiles_arrays_list[0]
        return ReferencedRaster(
            data=data, transform=tile.affine, bounds=tile.bounds, crs=tile.crs
        )

    # assert all tiles have same properties
    pyramid, resolution, dtype = _get_tiles_properties(tiles_arrays_list)
    # just handle antimeridian on global pyramid types
    shift = _shift_required(tiles_arrays_list)
    logger.debug("shift: %s" % shift)

    tiles_arrays_iter = iter(tiles_arrays_list)
    tile, data = next(tiles_arrays_iter)
    num_bands = data.shape[0] if data.ndim > 2 else 1

    def _shift(left, bottom, right, top) -> Tuple[float, float, float, float]:
        if shift:
            # shift by half of the grid width
            left += pyramid.x_size / 2
            right += pyramid.x_size / 2
            # if tile is now shifted outside pyramid bounds, move within
            if right > pyramid.right:
                right -= pyramid.x_size
                left -= pyramid.x_size
        return (left, bottom, right, top)

    mosaic_left, mosaic_bottom, mosaic_right, mosaic_top = _shift(*tile.bounds)

    for tile, data in tiles_arrays_iter:
        left, bottom, right, top = _shift(*tile.bounds)
        mosaic_left = min([left, mosaic_left])
        mosaic_bottom = min([bottom, mosaic_bottom])
        mosaic_right = max([right, mosaic_right])
        mosaic_top = max([top, mosaic_top])
    height = int(round((mosaic_top - mosaic_bottom) / resolution))
    width = int(round((mosaic_right - mosaic_left) / resolution))
    # initialize empty mosaic
    mosaic = ma.MaskedArray(
        data=np.full((num_bands, height, width), dtype=dtype, fill_value=nodata),
        mask=np.ones((num_bands, height, width)),
    )
    # create Affine
    affine = Affine(resolution, 0, mosaic_left, 0, -resolution, mosaic_top)
    # fill mosaic array with tile data
    for tile, data in tiles_arrays_list:
        masked_data: ma.MaskedArray = prepare_array(data, nodata=nodata, dtype=dtype)  # type: ignore
        t_left, t_bottom, t_right, t_top = _shift(*tile.bounds)
        minrow, maxrow, mincol, maxcol = bounds_to_ranges(
            bounds=(t_left, t_bottom, t_right, t_top),
            transform=affine,
        )
        existing_data = mosaic[:, minrow:maxrow, mincol:maxcol]
        existing_mask = mosaic.mask[:, minrow:maxrow, mincol:maxcol]
        mosaic[:, minrow:maxrow, mincol:maxcol] = np.where(
            masked_data.mask, existing_data, masked_data
        )
        mosaic.mask[:, minrow:maxrow, mincol:maxcol] = np.where(
            masked_data.mask, existing_mask, masked_data.mask
        )

    if shift:
        # shift back output mosaic
        mosaic_left -= pyramid.x_size / 2
        mosaic_right -= pyramid.x_size / 2

    # if mosaic crosses Antimeridian, make sure the mosaic output bounds are based on the
    # hemisphere of the Antimeridian with the larger mosaic intersection
    if mosaic_left < pyramid.left or mosaic_right > pyramid.right:
        # mosaic crosses Antimeridian
        logger.debug("mosaic crosses Antimeridian")
        left_distance = abs(pyramid.left - mosaic_left)
        right_distance = abs(pyramid.left - mosaic_right)
        # per default, the mosaic is placed on the right side of the Antimeridian, so we
        # only need to move the bounds in case the larger part of the mosaic is on the
        # left side
        if left_distance > right_distance:
            mosaic_left += pyramid.x_size
            mosaic_right += pyramid.x_size

    return ReferencedRaster(
        data=mosaic,
        transform=Affine(resolution, 0, mosaic_left, 0, -resolution, mosaic_top),
        bounds=Bounds(mosaic_left, mosaic_bottom, mosaic_right, mosaic_top),
        crs=tile.crs,
    )


def _get_tiles_properties(tiles_arrays_list: List[Tuple[BufferedTile, np.ndarray]]):
    tiles_data_iter = iter(tiles_arrays_list)

    first_tile, first_array = next(tiles_data_iter)
    tile_pyramid = first_tile.tile_pyramid
    pixel_x_size = first_tile.pixel_x_size
    dtype = first_array[0].dtype

    for tile, data in tiles_data_iter:
        if tile.zoom != tiles_arrays_list[0][0].zoom:
            raise ValueError("all tiles must be from same zoom level")
        if tile.crs != tiles_arrays_list[0][0].crs:
            raise ValueError("all tiles must have the same CRS")
        if isinstance(data, np.ndarray):
            if data[0].dtype != tiles_arrays_list[0][1][0].dtype:
                raise TypeError(
                    f"all tile data must have the same dtype: {data[0].dtype} != {tiles_arrays_list[0][1][0].dtype}"
                )
    return tile_pyramid, pixel_x_size, dtype


def _shift_required(tiles):
    """Determine if distance over antimeridian is shorter than normal distance."""
    if tiles[0][0].tile_pyramid.is_global:
        # get set of tile columns
        tile_cols = sorted(list(set([t[0].col for t in tiles])))
        # if tile columns are an unbroken sequence, tiles are connected and are not
        # passing the Antimeridian
        if tile_cols == list(range(min(tile_cols), max(tile_cols) + 1)):
            return False
        else:
            # look at column gaps and try to determine the smallest distance
            def gen_groups(items):
                """Group tile columns by sequence."""
                j = items[0]
                group = [j]
                for i in items[1:]:
                    # item is next in expected sequence
                    if i == j + 1:
                        group.append(i)
                    # gap occured, so yield existing group and create new one
                    else:
                        yield group
                        group = [i]
                    j = i
                yield group

            groups = list(gen_groups(tile_cols))
            # in case there is only one group, don't shift
            if len(groups) == 1:  # pragma: no cover
                return False
            # distance between first column of first group and last column of last group
            normal_distance = groups[-1][-1] - groups[0][0]
            # distance between last column of first group and last column of first group
            # but crossing the antimeridian
            antimeridian_distance = (
                groups[0][-1] + tiles[0][0].tile_pyramid.matrix_width(tiles[0][0].zoom)
            ) - groups[-1][0]
            # return whether distance over antimeridian is shorter
            return antimeridian_distance < normal_distance
    else:  # pragma: no cover
        return False
