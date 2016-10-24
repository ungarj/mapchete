"""Mapchtete handling tiles."""
from tilematrix import Tile, TilePyramid
from cached_property import cached_property


class BufferedTilePyramid(TilePyramid):
    """A special tile pyramid with fixed pixelbuffer and metatiling."""

    def __init__(self, pyramid_type, metatiling=1, pixelbuffer=0):
        """Initialize."""
        TilePyramid.__init__(self, pyramid_type, metatiling=metatiling)
        self.tile_pyramid = TilePyramid(pyramid_type, metatiling=metatiling)
        self.metatiling = metatiling
        self.pixelbuffer = pixelbuffer

    def tile(self, zoom, row, col):
        """
        Return BufferedTile object of this BufferedTilePyramid.

        - zoom: zoom level
        - row: tile matrix row
        - col: tile matrix column
        """
        tile = self.tile_pyramid.tile(zoom, row, col)
        return BufferedTile(tile, pixelbuffer=self.pixelbuffer)

    def tiles_from_bounds(self, bounds, zoom):
        """
        Return all tiles intersecting with bounds.

        Bounds values will be cleaned if they cross the antimeridian or are
        outside of the Northern or Southern tile pyramid bounds.
        - bounds: tuple of (left, bottom, right, top) bounding values in tile
            pyramid CRS
        - zoom: zoom level
        """
        for tile in self.tile_pyramid.tiles_from_bounds(bounds, zoom):
            yield self.tile(*tile.id)

    def tiles_from_bbox(self, geometry, zoom):
        """
        All metatiles intersecting with given bounding box.

        - geometry: shapely geometry
        - zoom: zoom level
        """
        for tile in self.tile_pyramid.tiles_from_bbox(geometry, zoom):
            yield self.tile(*tile.id)

    def tiles_from_geom(self, geometry, zoom):
        """
        Return all tiles intersecting with input geometry.

        - geometry: shapely geometry
        - zoom: zoom level
        """
        for tile in self.tile_pyramid.tiles_from_geom(geometry, zoom):
            yield self.tile(*tile.id)

    def intersecting(self, tile):
        """
        Return all BufferedTiles intersecting with tile.

        - tile: a BufferedTile
        """
        return [
            self.tile(*intersecting_tile.id)
            for intersecting_tile in self.tile_pyramid.intersecting(tile)
        ]


class BufferedTile(Tile):
    """A special tile with fixed pixelbuffer."""

    def __init__(self, tile, pixelbuffer=0):
        """Initialize."""
        assert not isinstance(tile, BufferedTile)
        Tile.__init__(self, tile.tile_pyramid, tile.zoom, tile.row, tile.col)
        self._tile = tile
        self.pixelbuffer = pixelbuffer
        self.data = None
        self.message = None
        self.error = None

    @cached_property
    def profile(self):
        """Return a rasterio profile dictionary."""
        out_meta = self.output.profile
        out_meta.update(
            width=self.width, height=self.height, transform=None,
            affine=self.affine)
        return out_meta

    @cached_property
    def height(self):
        """Return buffered height."""
        return self._tile.shape(pixelbuffer=self.pixelbuffer)[0]

    @cached_property
    def width(self):
        """Return buffered width."""
        return self._tile.shape(pixelbuffer=self.pixelbuffer)[1]

    @cached_property
    def shape(self):
        """Return buffered shape."""
        return (self.height, self.width)

    @cached_property
    def affine(self):
        """Return buffered Affine."""
        return self._tile.affine(pixelbuffer=self.pixelbuffer)

    @cached_property
    def bounds(self):
        """Return buffered bounds."""
        return self._tile.bounds(pixelbuffer=self.pixelbuffer)

    @cached_property
    def bbox(self):
        """Return buffered bounding box."""
        return self._tile.bbox(pixelbuffer=self.pixelbuffer)
