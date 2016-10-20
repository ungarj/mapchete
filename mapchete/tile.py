"""Mapchtete handling tiles."""
from tilematrix import Tile
from cached_property import cached_property


class BufferedTile(Tile):
    """A special tile with fixed pixelbuffer."""

    def __init__(self, tile, pixelbuffer=0):
        """Initialize."""
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
