#!/usr/bin/env python
"""
Classes handling raster data.
"""

import os
from numpy.ma import masked_array, zeros
from tempfile import NamedTemporaryFile
from tilematrix import clip_geometry_to_srs_bounds

from .io_funcs import (
    RESAMPLING_METHODS,
    file_bbox,
    reproject_geometry,
    _read_metadata
    )
from .raster_io import read_raster_window

class RasterProcessTile(object):
    """
    Class representing a tile (existing or virtual) of target pyramid from a
    Mapchete process output.
    """
    def __init__(
        self,
        input_mapchete,
        tile,
        pixelbuffer=0,
        resampling="nearest"
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

        try:
            assert resampling in RESAMPLING_METHODS
        except:
            raise ValueError("resampling method %s not found." % resampling)

        self.process = input_mapchete
        self.tile_pyramid = self.process.tile_pyramid
        self.tile = tile
        self.input_file = input_mapchete
        self.pixelbuffer = pixelbuffer
        self.resampling = resampling
        self.profile = _read_metadata(self, "RasterProcessTile")
        self.affine = self.profile["affine"]
        self.nodata = self.profile["nodata"]
        self.indexes = self.profile["count"]
        self.dtype = self.profile["dtype"]
        self.crs = self.tile_pyramid.crs
        self.shape = (self.profile["height"], self.profile["width"])
        self._np_band_cache = {}

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self._np_band_cache = None

    def read(self, indexes=None):
        """
        Generates reprojected numpy arrays from input process bands.
        """
        band_indexes = _get_band_indexes(self, indexes)

        if len(band_indexes) == 1:
            return _bands_from_cache(self, indexes=band_indexes).next()
        else:
            return _bands_from_cache(self, indexes=band_indexes)

    def is_empty(self, indexes=None):
        """
        Returns true if all items are masked.
        """
        band_indexes = _get_band_indexes(self, indexes)
        src_bbox = self.input_file.config.process_area(self.tile.zoom)
        dst_tile_bbox = self._reproject_tile_bbox(
            out_crs=self.input_file.tile_pyramid.crs
            )

        # empty if tile does not intersect with source process area
        if not dst_tile_bbox.buffer(0).intersects(src_bbox):
            return True

        # empty if no source tiles are available
        tile_paths = self._get_src_tile_paths()
        if not tile_paths:
            return True

        # empty if source band(s) are empty
        all_bands_empty = True
        for band in _bands_from_cache(self, band_indexes):
            if not band.mask.all():
                all_bands_empty = False
                break
        return all_bands_empty

    def _reproject_tile_bbox(self, out_crs=None):
        """
        Returns tile bounding box reprojected to source file CRS. If bounding
        box overlaps with antimeridian, a MultiPolygon is returned.
        """
        return reproject_geometry(
            clip_geometry_to_srs_bounds(
                self.tile.bbox(pixelbuffer=self.pixelbuffer),
                self.tile.tile_pyramid
                ),
            self.tile.crs,
            out_crs
            )

    def _get_src_tile_paths(self):
        """
        Returns existing tile paths from source process.
        """
        dst_tile_bbox = self._reproject_tile_bbox(
            out_crs=self.input_file.tile_pyramid.crs
            )

        src_tiles = [
            self.process.tile(tile)
            for tile in self.process.tile_pyramid.tiles_from_geom(
                dst_tile_bbox,
                self.tile.zoom
            )
        ]

        return [
            tile.path
            for tile in src_tiles
            if tile.exists()
            ]

class RasterFileTile(object):
    """
    Class representing a reprojected and resampled version of an original file
    to a given tile pyramid tile. Properties and functions are inspired by
    rasterio's way of handling datasets.
    """

    def __init__(
        self,
        input_file,
        tile,
        pixelbuffer=0,
        resampling="nearest"
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
            assert resampling in RESAMPLING_METHODS
        except:
            raise ValueError("resampling method %s not found." % resampling)

        try:
            self.process = tile.process
        except:
            self.process = None
        self.tile_pyramid = tile.tile_pyramid
        self.tile = tile
        self.input_file = input_file
        self.pixelbuffer = pixelbuffer
        self.resampling = resampling
        self.profile = _read_metadata(self, "RasterFileTile")
        self.affine = self.profile["affine"]
        self.nodata = self.profile["nodata"]
        self.indexes = self.profile["count"]
        self.dtype = self.profile["dtype"]
        self.crs = self.tile_pyramid.crs
        self.shape = (self.profile["height"], self.profile["width"])
        self._np_band_cache = {}

    def __enter__(self):
        return self

    def __exit__(self, t, value, tb):
        self._np_band_cache = None

    def read(self, indexes=None):
        """
        Generates reprojected numpy arrays from input file bands.
	    """
        band_indexes = _get_band_indexes(self, indexes)

        if len(band_indexes) == 1:
            return _bands_from_cache(self, indexes=band_indexes).next()
        else:
            return _bands_from_cache(self, indexes=band_indexes)

    def is_empty(self, indexes=None):
        """
        Returns true if all items are masked.
        """
        band_indexes = _get_band_indexes(self, indexes)
        src_bbox = file_bbox(self.input_file, self.tile_pyramid)
        tile_geom = self.tile.bbox(pixelbuffer=self.pixelbuffer)

        # empty if tile does not intersect with file bounding box
        if not tile_geom.intersects(src_bbox):
            return True

        # empty if source band(s) are empty
        all_bands_empty = True
        for band in _bands_from_cache(self, band_indexes):
            if not band.mask.all():
                all_bands_empty = False
                break
        return all_bands_empty

def _bands_from_cache(self, indexes=None):
    """
    Caches reprojected source data for multiple usage.
    """
    band_indexes = _get_band_indexes(self, indexes)
    if isinstance(self, RasterProcessTile):
        tile_paths = self._get_src_tile_paths()
        temp_vrt = NamedTemporaryFile()
        raster_file = temp_vrt.name
        build_vrt = "gdalbuildvrt %s %s > /dev/null" %(
            raster_file,
            ' '.join(tile_paths)
            )
        try:
            os.system(build_vrt)
        except:
            raise IOError("build temporary VRT failed")
    elif isinstance(self, RasterFileTile):
        raster_file = self.input_file

    for band_index in band_indexes:
        if not band_index in self._np_band_cache:
            if isinstance(self, RasterProcessTile) and len(tile_paths) == 0:
                band = masked_array(
                    zeros(
                        self.shape,
                        dtype=self.dtype
                    ),
                    mask=True
                    )
            else:
                band = read_raster_window(
                    raster_file,
                    self.tile,
                    indexes=band_index,
                    pixelbuffer=self.pixelbuffer,
                    resampling=self.resampling
                ).next()
            self._np_band_cache[band_index] = band
        yield self._np_band_cache[band_index]

def _get_band_indexes(self, indexes=None):
    """
    Returns valid band indexes.
    """
    if indexes:
        if isinstance(indexes, list):
            return indexes
        else:
            return [indexes]
    else:
        return range(1, self.indexes+1)
