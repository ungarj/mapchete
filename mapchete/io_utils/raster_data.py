#!/usr/bin/env python
"""
Classes handling raster data.
"""

import os
from numpy.ma import masked_array, zeros
from tempfile import NamedTemporaryFile
import rasterio
from rasterio.warp import transform_bounds
from copy import deepcopy
from tilematrix import clip_geometry_to_srs_bounds

from .io_funcs import RESAMPLING_METHODS, file_bbox, reproject_geometry
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
        self.profile = self._read_metadata()
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

    def _read_metadata(self):
        """
        Returns a rasterio-like metadata dictionary adapted to tile.
        """
        out_meta = self.process.output.profile
        # create geotransform
        px_size = self.tile_pyramid.pixel_x_size(self.tile.zoom)
        left = self.tile.bounds(pixelbuffer=self.pixelbuffer)[0]
        top = self.tile.bounds(pixelbuffer=self.pixelbuffer)[3]
        tile_geotransform = (left, px_size, 0.0, top, 0.0, -px_size)
        out_meta.update(
            width=self.tile.width+2*self.pixelbuffer,
            height=self.tile.height+2*self.pixelbuffer,
            transform=tile_geotransform,
            affine=self.tile.affine(pixelbuffer=self.pixelbuffer)
        )
        return out_meta

    def read(self, indexes=None):
        """
        Generates numpy arrays from input process bands.
        - dst_tile: this tile (self.tile)
        - src_tile(s): original MapcheteProcess pyramid tile
        Note: this is a semi-hacky variation as it uses an os.system call to
        generate a temporal mosaic using the gdalbuildvrt command.
        """
        band_indexes = self._band_indexes(indexes)

        if len(band_indexes) == 1:
            return self._bands_from_cache(indexes=band_indexes).next()
        else:
            return self._bands_from_cache(indexes=band_indexes)


    def is_empty(self, indexes=None):
        """
        Returns true if all items are masked.
        """
        band_indexes = self._band_indexes(indexes)
        src_bbox = self.input_file.config.process_area(self.tile.zoom)
        dst_tile_bbox = self._get_tile_bbox_in_file_crs()

        # empty if tile does not intersect with source process area
        if not dst_tile_bbox.buffer(0).intersects(src_bbox):
            return True

        # empty if no source tiles are available
        tile_paths = self._get_src_tile_paths()
        if not tile_paths:
            return True

        # empty if source band(s) are empty
        all_bands_empty = True
        for band_index in band_indexes:
            if not self._bands_from_cache(band_index).mask.all():
                all_bands_empty = False
                break
        return all_bands_empty

    def _bands_from_cache(self, indexes=None):
        """
        Caches reprojected source data for multiple usage.
        """
        band_indexes = self._band_indexes(indexes)
        tile_paths = self._get_src_tile_paths()
        for band_index in band_indexes:
            if not self._np_band_cache[band_index]:
                print "not cached"
                if len(tile_paths) == 0:
                    band = masked_array(
                        zeros(
                            self.shape,
                            dtype=self.dtype
                        ),
                        mask=True
                        )
                else:
                    temp_vrt = NamedTemporaryFile()
                    build_vrt = "gdalbuildvrt %s %s > /dev/null" %(
                        temp_vrt.name,
                        ' '.join(tile_paths)
                        )
                    try:
                        os.system(build_vrt)
                    except:
                        raise IOError("build temporary VRT failed")
                    band = read_raster_window(
                        temp_vrt.name,
                        self.tile,
                        indexes=band_indexes,
                        pixelbuffer=self.pixelbuffer,
                        resampling=self.resampling
                    )
                self._np_band_cache.update(
                    band_index=band
                )
            else:
                print "cached"

            yield self._np_band_cache[band_index]


    def _get_tile_bbox_in_file_crs(self):
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
            self.input_file.tile_pyramid.crs
            )

    def _get_src_tile_paths(self):
        """
        Returns existing tile paths from source process.
        """
        dst_tile_bbox = self._get_tile_bbox_in_file_crs()

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

    def _band_indexes(self, indexes=None):
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
        self.profile = self._read_metadata()
        self.affine = self.profile["affine"]
        self.nodata = self.profile["nodata"]
        self.indexes = self.profile["count"]
        self.dtype = self.profile["dtype"]
        self.crs = self.tile_pyramid.crs
        self.shape = (self.profile["width"], self.profile["height"])

    def __enter__(self):
        return self

    def __exit__(self, t, value, tb):
        # TODO cleanup
        pass

    def read(self, indexes=None):
        """
        Generates numpy arrays from input bands.
        """
        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        return read_raster_window(
            self.input_file,
            self.tile,
            indexes=band_indexes,
            pixelbuffer=self.pixelbuffer,
            resampling=self.resampling
        )


    def is_empty(self, indexes=None):
        """
        Returns true if all items are masked.
        """
        src_bbox = file_bbox(self.input_file, self.tile_pyramid)
        tile_geom = self.tile.bbox(pixelbuffer=self.pixelbuffer)
        if not tile_geom.intersects(src_bbox):
            return True

        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        with rasterio.open(self.input_file, "r") as src:
            # Reproject tile bounds to source file SRS.
            src_left, src_bottom, src_right, src_top = transform_bounds(
                self.tile.crs,
                src.crs,
                *self.tile.bounds(pixelbuffer=self.pixelbuffer),
                densify_pts=21
                )
            minrow, mincol = src.index(src_left, src_top)
            maxrow, maxcol = src.index(src_right, src_bottom)
            window = (minrow, maxrow), (mincol, maxcol)

            # Finally read data per band and store it in tuple.
            bands = (
                src.read(index, window=window, masked=True, boundless=True)
                for index in band_indexes
                )

            all_bands_empty = True
            for band in bands:
                if not band.mask.all():
                    all_bands_empty = False
                    break

            return all_bands_empty


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
