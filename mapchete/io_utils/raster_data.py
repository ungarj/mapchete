#!/usr/bin/env python

import os
from numpy.ma import masked_array, zeros
from tempfile import NamedTemporaryFile
import rasterio
from rasterio.warp import transform_bounds
from copy import deepcopy

from .io_funcs import RESAMPLING_METHODS, file_bbox, _reproject
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

        # try:
        #     assert tile.process
        # except:
        #     raise ValueError("please provide an input process")
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
        self.shape = (self.profile["width"], self.profile["height"])

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        # TODO cleanup
        pass

    def _read_metadata(self):
        """
        Returns a rasterio-like metadata dictionary adapted to tile.
        """
        out_meta = self.process.output.profile
        # create geotransform
        px_size = self.tile_pyramid.pixel_x_size(self.tile.zoom)
        left, bottom, right, top = self.tile.bounds(
            pixelbuffer=self.pixelbuffer
            )
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
        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        dst_tile_bbox = _reproject(
            self.tile.bbox(
                pixelbuffer=self.pixelbuffer
            ),
            self.tile.crs,
            self.input_file.tile_pyramid.crs
            )

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

        if len(tile_paths) == 0:
            # return emtpy array if no input files are given
            empty_array =  masked_array(
                zeros(
                    self.shape,
                    dtype=self.dtype
                ),
                mask=True
                )
            return [
                empty_array
                for index in band_indexes
            ]

        temp_vrt = NamedTemporaryFile()
        build_vrt = "gdalbuildvrt %s %s > /dev/null" %(
            temp_vrt.name,
            ' '.join(tile_paths)
            )
        try:
            os.system(build_vrt)
            return list(read_raster_window(
                temp_vrt.name,
                self.tile,
                indexes=band_indexes,
                pixelbuffer=self.pixelbuffer,
                resampling=self.resampling
            ))
        except:
            raise
        # finally:
            # clean up
            # if os.path.isfile(temp_vrt.name):
            #     os.remove(temp_vrt.name)

    def is_empty(self, indexes=None):
        """
        Returns true if all items are masked.
        """
        src_bbox = self.input_file.config.process_area(self.tile.zoom)
        # reproject tile bounding box to source file CRS
        dst_tile_bbox = _reproject(
            self.tile.bbox(
                pixelbuffer=self.pixelbuffer
            ),
            self.tile.crs,
            self.input_file.tile_pyramid.crs
            )

        if not dst_tile_bbox.intersects(src_bbox):
            return True

        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        src_tiles = [
            self.process.tile(tile)
            for tile in self.process.tile_pyramid.tiles_from_bbox(
                dst_tile_bbox,
                self.tile.zoom
            )
        ]

        temp_vrt = NamedTemporaryFile()
        tile_paths = [
            tile.path
            for tile in src_tiles
            if tile.exists()
            ]

        if len(tile_paths) == 0:
            return True

        build_vrt = "gdalbuildvrt %s %s > /dev/null" %(
            temp_vrt.name,
            ' '.join(tile_paths)
            )
        try:
            os.system(build_vrt)
        except:
            raise IOError((tile.id, "failed", "build temporary VRT failed"))

        all_bands_empty = True
        for band in self.read(band_indexes):
            if not band.mask.all():
                all_bands_empty = False
                break

        return all_bands_empty

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

    def __exit__(self, type, value, tb):
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

            # Calculate new Affine object for read window.
            window = (minrow, maxrow), (mincol, maxcol)
            # window_vector_affine = src.affine.translation(
            #     mincol,
            #     minrow
            #     )
            # window_affine = src.affine * window_vector_affine
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
        left, bottom, right, top = self.tile.bounds(
            pixelbuffer=self.pixelbuffer
            )
        tile_geotransform = (left, px_size, 0.0, top, 0.0, -px_size)
        out_meta.update(
            width=self.tile_pyramid.tile_size,
            height=self.tile_pyramid.tile_size,
            transform=tile_geotransform,
            affine=self.tile.affine(pixelbuffer=self.pixelbuffer)
        )
        return out_meta
