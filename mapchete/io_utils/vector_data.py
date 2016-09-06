#!/usr/bin/env python
"""
Classes handling vector data.
"""

import os
from itertools import chain
import fiona
import rasterio
from rasterio.crs import CRS
from copy import deepcopy

from .io_funcs import file_bbox, reproject_geometry
from .vector_io import read_vector_window

class VectorProcessTile(object):
    """
    Class representing a tile (existing or virtual) of target pyramid from a
    Mapchete process output.
    """
    def __init__(
        self,
        input_mapchete,
        tile,
        pixelbuffer=0,
        ):

        try:
            assert os.path.isfile(input_mapchete.config.process_file)
        except:
            raise IOError("input file does not exist: %s" %
                input_mapchete.config.process_file)

        try:
            assert pixelbuffer >= 0
        except:
            raise ValueError("pixelbuffer must be 0 or greater")

        try:
            assert isinstance(pixelbuffer, int)
        except:
            raise ValueError("pixelbuffer must be an integer")

        self.process = input_mapchete
        self.tile_pyramid = self.process.tile_pyramid
        self.tile = tile
        self.input_file = input_mapchete
        self.pixelbuffer = pixelbuffer
        self.schema = self.process.output.schema
        self.driver = self.process.output.driver
        self.crs = self.tile_pyramid.crs

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        # TODO cleanup
        pass

    def read(self, no_neighbors=False):
        """
        Returns features of all underlying tiles. If no_neighbors is set True,
        only the base tile is returned (ATTENTION: won't work if the parent
        mapchete process has a different metatile setting).
        """

        if no_neighbors:
            tile = self.process.tile(self.tile)
            if tile.exists():
                return read_vector_window(
                    tile.path,
                    tile,
                    pixelbuffer=self.pixelbuffer
                )
            else:
                return []
        else:
            dst_tile_bbox = self.tile.bbox(pixelbuffer=self.pixelbuffer)
            src_tiles = [
                self.process.tile(tile)
                for tile in self.process.tile_pyramid.tiles_from_bbox(
                    dst_tile_bbox,
                    self.tile.zoom
                )
                ]

            return chain.from_iterable(
                read_vector_window(
                    tile.path,
                    tile,
                    pixelbuffer=self.pixelbuffer
                )
                for tile in src_tiles
                if tile.exists()
            )

    def is_empty(self):
        """
        Returns true if no tiles are available.
        """
        dst_tile_bbox = self.tile.bbox(pixelbuffer=self.pixelbuffer)
        src_tiles = [
            self.process.tile(tile)
            for tile in self.process.tile_pyramid.tiles_from_bbox(
                dst_tile_bbox,
                self.tile.zoom
            )
        ]

        tile_paths = [
            tile.path
            for tile in src_tiles
            if tile.exists()
            ]

        if tile_paths:
            return False
        else:
            return True

class VectorFileTile(object):
    """
    Class representing a reprojected subset of an input vector dataset clipped
    to the tile boundaries. read() returns a Fiona-like dictionary with a
    "geometry" and a "properties" field.
    """

    def __init__(
        self,
        input_file,
        tile,
        pixelbuffer=0
        ):
        try:
            assert os.path.isfile(input_file)
        except:
            raise IOError("input file does not exist: %s" % input_file)

        try:
            assert pixelbuffer >= 0
        except:
            raise ValueError("pixelbuffer must be 0 or greater")

        try:
            assert isinstance(pixelbuffer, int)
        except:
            raise ValueError("pixelbuffer must be an integer")

        try:
            self.process = tile.process
        except:
            self.process = None
        self.tile_pyramid = tile.tile_pyramid
        self.tile = tile
        self.input_file = input_file
        self.pixelbuffer = pixelbuffer
        self.crs = self.tile_pyramid.crs

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        # TODO cleanup
        pass

    def read(self, validity_check=True):
        """
        This is a wrapper around the read_vector_window function of tilematrix.
        Tilematrix itself uses fiona to read vector data.
        This function returns a generator of GeoJSON-like dictionaries
        containing the clipped vector data and attributes.
        """
        return read_vector_window(
            self.input_file,
            self.tile,
            pixelbuffer=self.pixelbuffer,
            validity_check=validity_check
        )

    def is_empty(self):
        """
        Returns true if input is empty.
        """
        src_bbox = file_bbox(self.input_file, self.tile_pyramid)
        tile_geom = self.tile.bbox(pixelbuffer=self.pixelbuffer)
        if not tile_geom.intersects(src_bbox):
            return True

        try:
            self.read().next()
        except StopIteration:
            return True
        except:
            raise
        return False

    def _read_metadata(self):
        """
        Returns a rasterio-like metadata dictionary adapted to tile.
        """
        with rasterio.open(self.input_file, "r") as src:
            out_meta = deepcopy(src.meta)
        # create geotransform
        px_size = self.tile_pyramid.pixel_x_size(self.tile.zoom)
        left = self.tile.bounds(pixelbuffer=self.pixelbuffer)[0]
        top = self.tile.bounds(pixelbuffer=self.pixelbuffer)[3]
        tile_geotransform = (left, px_size, 0.0, top, 0.0, -px_size)
        out_meta.update(
            width=self.tile_pyramid.tile_size,
            height=self.tile_pyramid.tile_size,
            transform=tile_geotransform,
            affine=self.tile.affine(pixelbuffer=self.pixelbuffer)
        )
        return out_meta
