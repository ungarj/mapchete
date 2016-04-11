#!/usr/bin/env python

import numpy as np
import numpy.ma as ma
import os
from copy import deepcopy
from rasterio.warp import calculate_default_transform, transform_bounds
import rasterio
from affine import Affine
import fiona
from tempfile import NamedTemporaryFile

import mapchete
from tilematrix import *


def read_vector(
    process,
    input_file,
    pixelbuffer=0
    ):
    """
    This is a wrapper around the read_vector_window function of tilematrix.
    Tilematrix itself uses fiona to read vector data.
    This function returns a list of GeoJSON-like dictionaries containing the
    clipped vector data and attributes.
    """
    if input_file:
        features = read_vector_window(
            input_file,
            process.tile,
            pixelbuffer=pixelbuffer
        )
    else:
        features = None

    return features


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

    def __exit__( self, type, value, tb ):
        # TODO cleanup
        pass

    def read(self):
        """
        This is a wrapper around the read_vector_window function of tilematrix.
        Tilematrix itself uses fiona to read vector data.
        This function returns a list of GeoJSON-like dictionaries containing the
        clipped vector data and attributes.
        """
        if self.is_empty():
            return []
        else:
            return read_vector_window(
                self.input_file,
                self.tile
            )

    def is_empty(self, indexes=None):
        """
        Returns true if input is empty.
        """
        src_bbox = file_bbox(self.input_file, self.tile_pyramid)
        tile_geom = self.tile.bbox(pixelbuffer=self.pixelbuffer)
        if not tile_geom.intersects(src_bbox):
            return True

        # Reproject tile bounds to source file SRS.
        src_left, src_bottom, src_right, src_top = transform_bounds(
            self.tile.crs,
            self.crs,
            *self.tile.bounds(pixelbuffer=self.pixelbuffer),
            densify_pts=21
            )

        with fiona.open(self.input_file, 'r') as vector:
            features = vector.filter(
                bbox=self.tile.bounds(pixelbuffer=self.pixelbuffer)
            )
            if len(list(features))>0:
                return False
            else:
                return True

    def _read_metadata(self):
        """
        Returns a rasterio-like metadata dictionary adapted to tile.
        """
        with rasterio.open(self.input_file, "r") as src:
            out_meta = src.meta
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

    def __exit__( self, type, value, tb ):
        # TODO cleanup
        pass

    def _read_metadata(self):
        """
        Returns a rasterio-like metadata dictionary adapted to tile.
        """
        out_meta = self.tile_pyramid.format.profile
        # create geotransform
        px_size = self.tile_pyramid.pixel_x_size(self.tile.zoom)
        left, bottom, right, top = self.tile.bounds(
            pixelbuffer=self.pixelbuffer
            )
        tile_geotransform = (left, px_size, 0.0, top, 0.0, -px_size)
        out_meta.update(
            width=self.tile.width,
            height=self.tile.height,
            transform=tile_geotransform,
            affine=self.tile.affine(pixelbuffer=self.pixelbuffer)
        )
        return out_meta

    def read(self, indexes=None, from_baselevel=False):
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

        dst_tile_bbox = self.tile.bbox(pixelbuffer=self.pixelbuffer)
        src_tiles = [
            self.process.tile(tile)
            for tile in self.process.tile_pyramid.tiles_from_bbox(
                dst_tile_bbox,
                self.tile.zoom
            )
        ]
        # TODO flesh out mosaic_tiles() function and reimplement using internal
        # numpy arrays.

        # for tile in src_tiles:
        #     list(tile.read())
        temp_vrt = NamedTemporaryFile()
        tile_paths = [
            tile.path
            for tile in src_tiles
            if tile.exists()
            ]
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
        tile_geom = self.tile.bbox(
            pixelbuffer=self.pixelbuffer
        )
        if not tile_geom.intersects(src_bbox):
            return True

        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

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

    def __exit__( self, type, value, tb ):
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
            window_vector_affine = src.affine.translation(
                mincol,
                minrow
                )
            window_affine = src.affine * window_vector_affine
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
            out_meta = src.meta
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

def mosaic_tiles(
    src_tiles,
    indexes=None
    ):
    """
    Returns a larger numpy array of input tiles.
    """
    if indexes:
        if isinstance(indexes, list):
            band_indexes = indexes
        else:
            band_indexes = [indexes]
    else:
        band_indexes = range(1, self.indexes+1)


def write_raster(
    process,
    metadata,
    bands,
    pixelbuffer=0
    ):
    try:
        assert isinstance(bands, tuple)
    except:
        try:
            assert (
                isinstance(
                bands,
                np.ndarray
                ) or isinstance(
                bands,
                np.ma.core.MaskedArray
                )
            )
            bands = (bands, )
        except:
            raise TypeError("output bands must be stored in a tuple.")

    try:
        for band in bands:
            assert (
                isinstance(
                    band,
                    np.ndarray
                ) or isinstance(
                    band,
                    np.ma.core.MaskedArray
                )
            )
    except:
        raise TypeError(
            "output bands must be numpy ndarrays, not %s" % type(band)
            )

    try:
        for band in bands:
            assert band.ndim == 2
    except:
        raise TypeError(
            "output bands must be 2-dimensional, not %s" % band.ndim
            )

    process.tile_pyramid.format.prepare(
        process.config.output_name,
        process.tile
    )

    out_file = process.tile_pyramid.format.get_tile_name(
        process.config.output_name,
        process.tile
    )

    try:
        write_raster_window(
            out_file,
            process.tile,
            metadata,
            bands,
            pixelbuffer=pixelbuffer
        )
    except:
        raise
