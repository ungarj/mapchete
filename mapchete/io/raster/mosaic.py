import logging
from types import GeneratorType
from typing import Iterator, Tuple

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
    tiles: Iterator[Tuple[BufferedTile, np.ndarray]], nodata: NodataVal = 0
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
    if isinstance(tiles, GeneratorType):
        tiles = list(tiles)
    elif not isinstance(tiles, list):
        raise TypeError("tiles must be either a list or generator")
    if not all([isinstance(pair, tuple) for pair in tiles]):
        raise TypeError("tiles items must be tuples")
    if not all(
        [
            all([isinstance(tile, BufferedTile), isinstance(data, np.ndarray)])
            for tile, data in tiles
        ]
    ):
        raise TypeError("tuples must be pairs of BufferedTile and array")
    if len(tiles) == 0:
        raise ValueError("tiles list is empty")

    logger.debug("create mosaic from %s tile(s)", len(tiles))
    # quick return if there is just one tile
    if len(tiles) == 1:
        tile, data = tiles[0]
        return ReferencedRaster(
            data=data, transform=tile.affine, bounds=tile.bounds, crs=tile.crs
        )

    # assert all tiles have same properties
    pyramid, resolution, dtype = _get_tiles_properties(tiles)
    # just handle antimeridian on global pyramid types
    shift = _shift_required(tiles)
    logger.debug("shift: %s" % shift)
    # determine mosaic shape and reference
    m_left, m_bottom, m_right, m_top = None, None, None, None
    for tile, data in tiles:
        num_bands = data.shape[0] if data.ndim > 2 else 1
        left, bottom, right, top = tile.bounds
        if shift:
            # shift by half of the grid width
            left += pyramid.x_size / 2
            right += pyramid.x_size / 2
            # if tile is now shifted outside pyramid bounds, move within
            if right > pyramid.right:
                right -= pyramid.x_size
                left -= pyramid.x_size
        m_left = min([left, m_left]) if m_left is not None else left
        m_bottom = min([bottom, m_bottom]) if m_bottom is not None else bottom
        m_right = max([right, m_right]) if m_right is not None else right
        m_top = max([top, m_top]) if m_top is not None else top
    height = int(round((m_top - m_bottom) / resolution))
    width = int(round((m_right - m_left) / resolution))
    # initialize empty mosaic
    mosaic = ma.MaskedArray(
        data=np.full((num_bands, height, width), dtype=dtype, fill_value=nodata),
        mask=np.ones((num_bands, height, width)),
    )
    # create Affine
    affine = Affine(resolution, 0, m_left, 0, -resolution, m_top)
    # fill mosaic array with tile data
    for tile, data in tiles:
        data = prepare_array(data, nodata=nodata, dtype=dtype)
        t_left, t_bottom, t_right, t_top = tile.bounds
        if shift:
            t_left += pyramid.x_size / 2
            t_right += pyramid.x_size / 2
            # if tile is now shifted outside pyramid bounds, move within
            if t_right > pyramid.right:
                t_right -= pyramid.x_size
                t_left -= pyramid.x_size
        minrow, maxrow, mincol, maxcol = bounds_to_ranges(
            bounds=(t_left, t_bottom, t_right, t_top),
            transform=affine,
        )
        existing_data = mosaic[:, minrow:maxrow, mincol:maxcol]
        existing_mask = mosaic.mask[:, minrow:maxrow, mincol:maxcol]
        mosaic[:, minrow:maxrow, mincol:maxcol] = np.where(
            data.mask, existing_data, data
        )
        mosaic.mask[:, minrow:maxrow, mincol:maxcol] = np.where(
            data.mask, existing_mask, data.mask
        )

    if shift:
        # shift back output mosaic
        m_left -= pyramid.x_size / 2
        m_right -= pyramid.x_size / 2

    # if mosaic crosses Antimeridian, make sure the mosaic output bounds are based on the
    # hemisphere of the Antimeridian with the larger mosaic intersection
    if m_left < pyramid.left or m_right > pyramid.right:
        # mosaic crosses Antimeridian
        logger.debug("mosaic crosses Antimeridian")
        left_distance = abs(pyramid.left - m_left)
        right_distance = abs(pyramid.left - m_right)
        # per default, the mosaic is placed on the right side of the Antimeridian, so we
        # only need to move the bounds in case the larger part of the mosaic is on the
        # left side
        if left_distance > right_distance:
            m_left += pyramid.x_size
            m_right += pyramid.x_size
    logger.debug(Bounds(m_left, m_bottom, m_right, m_top))
    return ReferencedRaster(
        data=mosaic,
        transform=Affine(resolution, 0, m_left, 0, -resolution, m_top),
        bounds=Bounds(m_left, m_bottom, m_right, m_top),
        crs=tile.crs,
    )


def _get_tiles_properties(tiles):
    for tile, data in tiles:
        if tile.zoom != tiles[0][0].zoom:
            raise ValueError("all tiles must be from same zoom level")
        if tile.crs != tiles[0][0].crs:
            raise ValueError("all tiles must have the same CRS")
        if isinstance(data, np.ndarray):
            if data[0].dtype != tiles[0][1][0].dtype:
                raise TypeError(
                    f"all tile data must have the same dtype: {data[0].dtype} != {tiles[0][1][0].dtype}"
                )
    return tile.tile_pyramid, tile.pixel_x_size, data[0].dtype


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
