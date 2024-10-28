"""Mapchtete handling tiles."""

from __future__ import annotations

from enum import Enum
import logging
from itertools import product
from typing import Generator, List, Literal, TypedDict, Union

import numpy as np
from affine import Affine
from pydantic import NonNegativeInt
from rasterio.enums import Resampling
from rasterio.features import rasterize, shapes
from rasterio.transform import from_bounds
from rasterio.warp import reproject
from shapely import clip_by_rect
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
from tilematrix import Shape, Tile, TileIndex, TilePyramid, GridDefinition
from tilematrix._conf import ROUND

from mapchete.bounds import Bounds
from mapchete.protocols import GridProtocol
from mapchete.types import (
    BoundsLike,
    CRSLike,
    Geometry,
    ShapeLike,
    Polygon,
    MultiPolygon,
)

logger = logging.getLogger(__name__)

GridDefinitionDict = TypedDict(
    "GridDefinitionDict",
    {
        "grid": Literal["custom", "geodetic", "mercator"],
        "shape": ShapeLike,
        "bounds": BoundsLike,
        "srs": CRSLike,
        "is_global": bool,
    },
)
MetatilingValue = Literal[1, 2, 4, 8, 16, 32, 64, 128, 256]
PyramidDefinitionDict = TypedDict(
    "PyramidDefinitionDict",
    {
        "grid": GridDefinitionDict,
        "metatiling": MetatilingValue,
        "tile_size": NonNegativeInt,
        "pixelbuffer": NonNegativeInt,
    },
)


class BatchBy(str, Enum):
    row = "row"
    col = "col"


class BufferedTilePyramid(TilePyramid):
    """
    A tile pyramid with fixed pixelbuffer and metatiling.
    """

    metatiling: MetatilingValue
    pixelbuffer: NonNegativeInt

    def __init__(
        self,
        grid: Union[
            Literal["geodetic", "mercator"], GridDefinitionDict, GridDefinition
        ],
        metatiling: MetatilingValue = 1,
        tile_size: NonNegativeInt = 256,
        pixelbuffer: NonNegativeInt = 0,
    ):
        TilePyramid.__init__(self, grid, metatiling=metatiling, tile_size=tile_size)
        self.tile_pyramid = TilePyramid(
            grid, metatiling=metatiling, tile_size=tile_size
        )
        self.metatiling = metatiling
        if isinstance(pixelbuffer, int) and pixelbuffer >= 0:
            self.pixelbuffer = pixelbuffer
        else:  # pragma: no cover
            raise ValueError("pixelbuffer has to be a non-negative int")

    def tile(self, zoom: int, row: int, col: int) -> BufferedTile:
        """
        Return ``BufferedTile`` object of this ``BufferedTilePyramid``.
        """
        return BufferedTile(
            self.tile_pyramid.tile(zoom, row, col), pixelbuffer=self.pixelbuffer
        )

    def tiles_from_bounds(
        self, bounds: BoundsLike, zoom: int
    ) -> Generator[BufferedTile, None, None]:
        """
        Yield BufferedTiles intersecting with bounds.
        """
        yield from self.tiles_from_bbox(Bounds.from_inp(bounds).geometry, zoom=zoom)

    def tiles_from_bounds_batches(
        self, bounds: BoundsLike, zoom: int, batch_by: BatchBy = BatchBy.row
    ) -> Generator[Generator[BufferedTile, None, None], None, None]:
        """
        Yield batches of BufferedTiles intersecting with bounds.
        """
        yield from self.tiles_from_bbox_batches(
            Bounds.from_inp(bounds).geometry, zoom=zoom, batch_by=batch_by
        )

    def tiles_from_bbox(
        self, geometry: Geometry, zoom: int
    ) -> Generator[BufferedTile, None, None]:
        """
        Yield BufferedTiles intersecting with geometry bounds.
        """
        for tile in self.tile_pyramid.tiles_from_bbox(geometry, zoom=zoom):
            if isinstance(tile, Tile):
                yield self.tile(*tile.id)

    def tiles_from_bbox_batches(
        self, geometry: Geometry, zoom: int, batch_by: BatchBy = BatchBy.row
    ) -> Generator[Generator[BufferedTile, None, None], None, None]:
        """
        Yield batches of BufferedTiles intersecting with geometry bounds.
        """
        for batch in self.tile_pyramid.tiles_from_bbox(
            geometry,
            zoom=zoom,
            batch_by="column" if batch_by.value == "col" else batch_by.value,
        ):
            yield (self.tile(*tile.id) for tile in batch if isinstance(tile, Tile))

    def tiles_from_geom(
        self, geometry: Geometry, zoom: int, exact: bool = False
    ) -> Generator[BufferedTile, None, None]:
        """
        Yield BufferedTiles intersecting with geometry.
        """
        for tile in self.tile_pyramid.tiles_from_geom(geometry, zoom=zoom, exact=exact):
            if isinstance(tile, Tile):
                yield self.tile(*tile.id)

    def tiles_from_geom_batches(
        self,
        geometry: Geometry,
        zoom: int,
        batch_by: BatchBy = BatchBy.row,
        exact: bool = False,
    ) -> Generator[Generator[BufferedTile, None, None], None, None]:
        """
        Yield batches of BufferedTiles intersecting with geometry.
        """
        for batch in self.tile_pyramid.tiles_from_geom(
            geometry,
            zoom=zoom,
            batch_by="column" if batch_by.value == "col" else batch_by.value,
            exact=exact,
        ):
            yield (self.tile(*tile.id) for tile in batch if isinstance(tile, Tile))

    def intersecting(self, tile: BufferedTile) -> List[BufferedTile]:
        """
        Return all BufferedTiles intersecting with tile.
        """
        return [
            self.tile(*intersecting_tile.id)
            for intersecting_tile in self.tile_pyramid.intersecting(tile)
        ]

    def matrix_affine(self, zoom: int) -> Affine:
        """
        Return Affine object for zoom level assuming tiles are cells.
        """
        if self.pixelbuffer:  # pragma: no cover
            raise ValueError(
                "Matrix Affine can only be created for pyramid without pixelbuffer."
            )
        return Affine(
            round(self.x_size / self.matrix_width(zoom), ROUND),
            0,
            self.bounds.left,
            0,
            -round(self.y_size / self.matrix_height(zoom), ROUND),
            self.bounds.top,
        )

    def to_dict(self) -> PyramidDefinitionDict:
        """
        Return dictionary representation of pyramid parameters.
        """
        return dict(
            grid=self.grid.to_dict(),
            metatiling=self.metatiling,
            tile_size=self.tile_size,
            pixelbuffer=self.pixelbuffer,
        )  # type: ignore

    def without_pixelbuffer(self) -> BufferedTilePyramid:
        config_dict = self.to_dict()
        config_dict.update(pixelbuffer=0)
        return BufferedTilePyramid(**config_dict)

    @staticmethod
    def from_dict(config_dict) -> BufferedTilePyramid:
        """
        Initialize TilePyramid from configuration dictionary.
        """
        return BufferedTilePyramid(**config_dict)

    def __repr__(self):
        return (
            "BufferedTilePyramid(%s, tile_size=%s, metatiling=%s, pixelbuffer=%s)"
            % (self.grid, self.tile_size, self.metatiling, self.pixelbuffer)
        )


class BufferedTile(GridProtocol):
    """
    A Tile member of a BufferedTilePyramid.
    """

    zoom: int
    row: int
    col: int
    id: TileIndex
    pixelbuffer: NonNegativeInt
    bounds: Bounds
    bbox: Union[Polygon, MultiPolygon]
    left: float
    bottom: float
    right: float
    top: float
    shape: Shape
    height: int
    width: int
    affine: Affine
    transform: Affine
    crs: CRSLike
    tp: TilePyramid
    pixel_x_size: float
    pixel_y_size: float

    def __init__(self, tile: Tile, pixelbuffer: NonNegativeInt = 0):
        """Initialize."""
        if isinstance(tile, BufferedTile):
            tile = TilePyramid(
                tile.tp.grid, tile_size=tile.tp.tile_size, metatiling=tile.tp.metatiling
            ).tile(*tile.id)
        self._tile = tile
        self.pixelbuffer = pixelbuffer
        self.buffered_tp = BufferedTilePyramid(
            tile.tp.to_dict(), pixelbuffer=pixelbuffer
        )
        bounds = Bounds.from_inp(self._tile.bounds(pixelbuffer=self.pixelbuffer))
        self.left = bounds.left
        self.bottom = bounds.bottom
        self.right = bounds.right
        self.top = bounds.top
        self.bounds = bounds
        self.bbox = self._tile.bbox(pixelbuffer=self.pixelbuffer)
        self.__geo_interface__ = mapping(self.bbox)
        self.shape = self._tile.shape(pixelbuffer=self.pixelbuffer)
        self.height = self.shape.height
        self.width = self.shape.width
        self.affine = self._tile.affine(pixelbuffer=self.pixelbuffer)
        self.transform = self._tile.affine(pixelbuffer=self.pixelbuffer)
        self.zoom = tile.zoom
        self.row = tile.row
        self.col = tile.col
        self.id = tile.id
        self.crs = tile.crs
        self.tp = self.tile_pyramid = tile.tp
        self.pixel_x_size = tile.pixel_x_size
        self.pixel_y_size = tile.pixel_y_size

    def is_valid(self) -> bool:  # pragma: no cover
        return self._tile.is_valid()

    def get_children(self) -> List[BufferedTile]:
        """
        Get tile children (intersecting tiles in next zoom level).
        """
        return [BufferedTile(t, self.pixelbuffer) for t in self._tile.get_children()]

    def get_parent(self) -> BufferedTile:
        """
        Get tile parent (intersecting tile in previous zoom level).
        """
        return BufferedTile(self._tile.get_parent(), self.pixelbuffer)  # type: ignore

    def get_neighbors(self, connectedness: Literal[4, 8] = 8) -> List[BufferedTile]:
        """
        Return tile neighbors.

        Tile neighbors are unique, i.e. in some edge cases, where both the left
        and right neighbor wrapped around the antimeridian is the same. Also,
        neighbors ouside the northern and southern TilePyramid boundaries are
        excluded, because they are invalid.

        connectedness : int
            [4 or 8] return four direct neighbors or all eight.

                    # -------------
        # | 8 | 1 | 5 |
        # -------------
        # | 4 | x | 2 |
        # -------------
        # | 7 | 3 | 6 |
        # -------------
        """
        return [
            BufferedTile(t, self.pixelbuffer)
            for t in self._tile.get_neighbors(connectedness=connectedness)
        ]

    def is_on_edge(self) -> bool:
        """Determine whether tile touches or goes over pyramid edge."""
        return (
            self.left <= self.tile_pyramid.left
            or self.bottom <= self.tile_pyramid.bottom  # touches_left
            or self.right >= self.tile_pyramid.right  # touches_bottom
            or self.top >= self.tile_pyramid.top  # touches_right  # touches_top
        )

    def __eq__(self, other: BufferedTile):
        return (
            isinstance(other, self.__class__)
            and self.pixelbuffer == other.pixelbuffer
            and self.buffered_tp == other.buffered_tp
            and self.id == other.id
        )

    def __ne__(self, other: BufferedTile):
        return not self.__eq__(other)

    def __repr__(self):
        return f"BufferedTile(zoom={self.zoom}, row={self.row}, col={self.col})"

    def __hash__(self):
        return hash(repr(self))

    def __iter__(self):
        yield self.zoom
        yield self.row
        yield self.col


def count_tiles(
    geometry: Geometry,
    pyramid: BufferedTilePyramid,
    minzoom: int,
    maxzoom: int,
    init_zoom: int = 0,
    rasterize_threshold: int = 0,
) -> int:
    """
    Count number of tiles intersecting with geometry.
    """

    def _count_tiles(tiles, geometry, minzoom, maxzoom):
        count = 0
        for tile in tiles:
            # determine data covered by tile
            tile_intersection = geometry.intersection(tile.bbox)

            # skip if there is no intersection
            if not tile_intersection.area:
                continue

            # increase counter as tile contains data
            if tile.zoom >= minzoom:
                count += 1

            # if there are further zoom levels, analyze descendants
            if tile.zoom < maxzoom:
                # if tile is half full, analyze each descendant
                # also do this if the tile children are not four in which case we cannot use
                # the count formula below
                if (
                    tile_intersection.area < tile.bbox.area
                    or len(tile.get_children()) != 4
                ):
                    count += _count_tiles(
                        tile.get_children(), tile_intersection, minzoom, maxzoom
                    )

                # if tile is full, all of its descendants will be full as well
                else:
                    # sum up tiles for each remaining zoom level
                    count += sum(
                        [
                            4**z
                            for z in range(
                                # only count zoom levels which are greater than minzoom or
                                # count all zoom levels from tile zoom level to maxzoom
                                minzoom - tile.zoom if tile.zoom < minzoom else 1,
                                maxzoom - tile.zoom + 1,
                            )
                        ]
                    )

        return count

    def _count_cells(pyramid, geometry, minzoom, maxzoom):
        if geometry.is_empty:  # pragma: no cover
            return 0

        # for the rasterization algorithm we need to keep the all_touched flag True
        # but slightly reduce the geometry area in order to get the same results as
        # with the tiles/vector algorithm.
        left, bottom, right, top = geometry.bounds
        width = right - left
        height = top - bottom
        buffer_distance = ((width + height) / 2) * -0.0000001
        # geometry will be reduced by a tiny fraction of the average from bounds width & height
        geometry_reduced = geometry.buffer(buffer_distance)

        logger.debug(
            "rasterize polygon on %s x %s cells",
            pyramid.matrix_height(maxzoom),
            pyramid.matrix_width(maxzoom),
        )
        transform = pyramid.matrix_affine(maxzoom)
        raster = rasterize(
            [(geometry_reduced, 1)],
            out_shape=(pyramid.matrix_height(maxzoom), pyramid.matrix_width(maxzoom)),
            fill=0,
            transform=transform,
            dtype=np.uint8,
            all_touched=True,
        )

        # count cells
        count = raster.sum()

        # resample raster up until minzoom using "max" resampling and count cells
        for zoom in reversed(range(minzoom, maxzoom)):
            raster, transform = reproject(
                raster,
                np.zeros(
                    (pyramid.matrix_height(zoom), pyramid.matrix_width(zoom)),
                    dtype=np.uint8,
                ),
                src_transform=transform,
                src_crs=pyramid.crs,
                dst_transform=pyramid.matrix_affine(zoom),
                dst_crs=pyramid.crs,
                resampling=Resampling.max,
            )
            count += raster.sum()

        # return cell sum
        return int(count)

    if not 0 <= init_zoom <= minzoom <= maxzoom:  # pragma: no cover
        raise ValueError("invalid zoom levels given")
    # tile buffers are not being taken into account
    unbuffered_pyramid = BufferedTilePyramid(
        pyramid.grid, tile_size=pyramid.tile_size, metatiling=pyramid.metatiling
    )
    height = pyramid.matrix_height(init_zoom)
    width = pyramid.matrix_width(init_zoom)
    # rasterize to array and count cells if too many tiles are expected
    if width > rasterize_threshold or height > rasterize_threshold:
        logger.debug("rasterize tiles to count geometry overlap")
        return _count_cells(unbuffered_pyramid, geometry, minzoom, maxzoom)

    logger.debug("count tiles using tile logic")
    return _count_tiles(
        [
            unbuffered_pyramid.tile(*tile_id)
            for tile_id in product(
                [init_zoom],
                range(pyramid.matrix_height(init_zoom)),
                range(pyramid.matrix_width(init_zoom)),
            )
        ],
        geometry,
        minzoom,
        maxzoom,
    )


def snap_geometry_to_tiles(
    geometry: Geometry, pyramid: BufferedTilePyramid, zoom: int
) -> Geometry:
    if geometry.is_empty:
        return geometry
    # calculate everything using an unbuffered pyramid, because otherwise the Affine
    # object cannot be calculated
    unbuffered_pyramid = BufferedTilePyramid.from_dict(
        dict(pyramid.to_dict(), pixelbuffer=0)
    )

    # use subset because otherwise memory usage can crash the program
    left, bottom, right, top = geometry.bounds
    # clip geometry bounds to pyramid bounds
    left = max([left, unbuffered_pyramid.bounds.left])
    bottom = max([bottom, unbuffered_pyramid.bounds.bottom])
    right = min([right, unbuffered_pyramid.bounds.right])
    top = min([top, unbuffered_pyramid.bounds.top])
    lb_tile = unbuffered_pyramid.tile_from_xy(left, bottom, zoom, on_edge_use="rt")
    rt_tile = unbuffered_pyramid.tile_from_xy(right, top, zoom, on_edge_use="lb")
    snapped_left, snapped_south, snapped_east, snapped_north = (
        lb_tile.bounds.left,
        lb_tile.bounds.bottom,
        rt_tile.bounds.right,
        rt_tile.bounds.top,
    )
    width = abs(rt_tile.col - lb_tile.col) + 1
    height = abs(rt_tile.row - lb_tile.row) + 1
    out_shape = (height, width)
    transform = from_bounds(
        west=snapped_left,
        south=snapped_south,
        east=snapped_east,
        north=snapped_north,
        width=width,
        height=height,
    )
    raster = rasterize(
        [(geometry, 1)],
        out_shape=out_shape,
        fill=0,
        transform=transform,
        dtype=np.uint8,
        all_touched=True,
    )
    # recreate geometry again by extracting features from raster
    out_geom = unary_union(
        [
            shape(feature)
            for feature, _ in shapes(raster, mask=raster, transform=transform)
        ]
    )
    # if original pyramid contained a pixelbuffer, add it to the output geometry
    if pyramid.pixelbuffer:
        buffer_distance = pyramid.pixelbuffer * pyramid.pixel_x_size(zoom)
        return clip_by_rect(
            out_geom.buffer(buffer_distance, join_style="mitre"),
            pyramid.left - buffer_distance if pyramid.is_global else pyramid.left,
            pyramid.bottom,
            pyramid.right + buffer_distance if pyramid.is_global else pyramid.right,
            pyramid.top,
        )
    return out_geom
