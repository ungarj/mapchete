#!/usr/bin/env python

import numpy as np
import numpy.ma as ma
import os
from copy import deepcopy
from rasterio.warp import calculate_default_transform, transform_bounds
import rasterio
from affine import Affine

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
            process.tile_pyramid,
            process.tile,
            pixelbuffer=pixelbuffer
        )
    else:
        features = None

    return features


def mc_open(
    input_file,
    tile,
    pixelbuffer=0,
    resampling="nearest"):
    """
    Returns either a RasterTile or a VectorTile object.
    """
    if isinstance(input_file, dict):
        raise ValueError("input cannot be dict")
    # TODO add proper check for input type.
    if isinstance(input_file, str):
        pass
        return RasterFileTile(
            input_file,
            tile,
            pixelbuffer=pixelbuffer,
            resampling=resampling
        )

    else:
        return RasterProcessTile(
            input_file,
            tile,
            pixelbuffer=pixelbuffer,
            resampling=resampling
        )

class RasterProcessTile(object):
    """
    Class representing a tile (existing or virtual) of a Mapchete process output
    pyramid.
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
            assert resampling in resampling_methods
        except:
            raise ValueError("resampling method %s not found." % resampling)

        # try:
        #     assert tile.process
        # except:
        #     raise ValueError("please provide an input process")
        try:
            self.process = tile.process
        except:
            self.process = None
        self.tile_pyramid = tile.tile_pyramid
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
        left, bottom, right, top = self.tile.bounds(pixelbuffer=self.pixelbuffer)
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
        Generates numpy arrays from input bands.
        """
        if indexes:
            band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        nodataval = self.nodata
        # Quick fix because None nodata is not allowed.
        if not nodataval:
            nodataval = 0

        shape = (
            self.tile_pyramid.tile_size + 2 * self.pixelbuffer,
            self.tile_pyramid.tile_size + 2 * self.pixelbuffer
            )

        if from_baselevel:
            try:
                assert self.process
            except:
                raise ValueError("this function can only be used within a \
                mapchete process")
            pass
        else:
            pass

        # - get target tile bounds
        tile_bbox = self.tile_pyramid.tile_bbox(
            *self.tile,
            pixelbuffer=self.pixelbuffer
            )

        # - if target tile bounds don't intersect with respective process_area,
        #   return empty array using the source process profile.
        if not tile_bbox.intersects(self.process.process_area):
            for index in band_indexes:
                dst_band = np.ma.zeros(shape=(shape), dtype=src.dtypes[index-1])
                dst_band[:] = nodataval
                dst_band = ma.masked_equal(dst_band, nodataval)
                yield dst_band


        # - determine affected tiles from source pyramid
        src_tile_pyramid = self.process.tile_pyramid
        for index in band_indexes:
            src_tiles = src_tile_pyramid.tiles_from_geom(
                tile_bbox,
                self.tile[0]
            )
            for src_tile in src_tiles:
                self.process.get(src_tile)

        rows = {}
        for src_zoom, src_row, src_col in sorted(src_tiles, key=lambda x:x[2]):
            rows.setdefault(src_row, []).append((src_zoom, src_row, src_col))


        # - check if tiles are available
        # - if not, process these tiles
        # --> use get_tile method
        # do per band:
        # - get tiles
        # - mosaick tiles
        # - crop to output tile boundary
        # return


    def is_empty(self, indexes=None):
        """
        Returns true if all items are masked.
        """
        zoom, row, col = self.tile
        src_bbox = self.input_file.config.process_bounds(zoom)
        tile_geom = self.tile_pyramid.tile_bbox(
            *self.tile,
            pixelbuffer=self.pixelbuffer
        )
        if not tile_geom.intersects(src_bbox):
            return True

        if indexes:
            band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        # Reproject tile bounds to source file SRS.
        src_left, src_bottom, src_right, src_top = transform_bounds(
            self.tile_pyramid.crs,
            self.crs,
            *self.tile_pyramid.tile_bounds(
                *self.tile,
                pixelbuffer=self.pixelbuffer
                ),
            densify_pts=21
            )


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
            assert resampling in resampling_methods
        except:
            raise ValueError("resampling method %s not found." % resampling)

        # try:
        #     assert tile.process
        # except:
        #     raise ValueError("please provide an input process or a tile and\
        #     a tile pyramid.")
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
            band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        # if len(band_indexes) == 1:
        #     return next(read_raster_window(
        #         self.input_file,
        #         self.tile,
        #         indexes=band_indexes,
        #         pixelbuffer=self.pixelbuffer,
        #         resampling=self.resampling
        #     ))
        # else:
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
            band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        # Reproject tile bounds to source file SRS.
        src_left, src_bottom, src_right, src_top = transform_bounds(
            self.tile.crs,
            self.crs,
            *self.tile.bounds(pixelbuffer=self.pixelbuffer),
            densify_pts=21
            )

        with rasterio.open(self.input_file, "r") as src:

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


def read_raster(
    process,
    input_file,
    pixelbuffer=0,
    bands=None,
    resampling="nearest",
    return_empty_mask=False
    ):
    """
    This is a wrapper around the read_raster_window function of tilematrix.
    Tilematrix itself uses rasterio to read raster data.
    This function returns a tuple of metadata and a numpy array containing the
    raster data clipped and resampled to the input tile.
    """
    if input_file and os.path.isfile(input_file):
        metadata, data = read_raster_window(
            input_file,
            process.tile_pyramid,
            process.tile,
            bands=bands,
            pixelbuffer=pixelbuffer,
            resampling=resampling
            )
    else:
        metadata = None
        data = None

    if not return_empty_mask:
        # Return None if bands are empty.
        all_bands_empty = True
        for band_data in data:
            if not band_data.mask.all():
                all_bands_empty = False
        if all_bands_empty:
            metadata = None
            data = None
    return metadata, data


def read_pyramid(
    dst_tile,
    src_output_name,
    src_tile_pyramid,
    src_zoom=None,
    dst_tile_pyramid=None,
    dst_pixelbuffer=0,
    resampling="nearest"
    ):
    """
    This function reads from an existing tile pyramid.
    """
    zoom, row, col = dst_tile
    if not src_zoom:
        src_zoom = zoom
    if not dst_tile_pyramid:
        dst_tile_pyramid = src_tile_pyramid
    tile_bbox = dst_tile_pyramid.tile_bbox(*dst_tile, pixelbuffer=dst_pixelbuffer)
    dst_tile = Tile(dst_tile_pyramid, zoom, row, col)
    tile_bbox = dst_tile.bbox
    src_tiles = src_tile_pyramid.tiles_from_geom(tile_bbox, src_zoom)
    rows = {}
    for src_zoom, src_row, src_col in sorted(src_tiles, key=lambda x:x[2]):
        rows.setdefault(src_row, []).append((src_zoom, src_row, src_col))
    rows_data = {}
    rows_metadata = ()
    temp = type('temp', (object,), {})()
    temp.tile_pyramid = TilePyramid("geodetic", tile_size=128)
    temp.tile = dst_tile

    for row, tiles in rows.iteritems():
        row_data = {}
        for band in range(dst_tile_pyramid.format.profile["count"]):
            row_data[band] = ()
        for tile in tiles:
            filename = src_tile_pyramid.format.get_tile_name(
                src_output_name,
                tile
            )
            tile_metadata, tile_data = read_raster(
                temp,
                filename,
                return_empty_mask=True,
                bands = dst_tile_pyramid.format.profile["count"]
            )
            if not tile_data:
                tile_data = ()
                size = temp.tile_pyramid.tile_size
                for i in range(1, dst_tile_pyramid.format.profile["count"]+1):
                    zeros = np.zeros(
                        shape=((size, size)),
                        dtype=dst_tile_pyramid.format.profile["dtype"]
                    )
                    out_band = ma.masked_array(
                        zeros,
                        mask=True
                    )
                    tile_data += (out_band,)
                tile_metadata = None
            assert len(tile_data) == 3
            for band in tile_data:
                assert isinstance(band, np.ndarray)

                assert band.shape == (128, 128)

            # add tile bands to rows data
            for tile_band, tile_banddata in enumerate(tile_data):
                row_data[tile_band] += (tile_banddata, )
            assert len(row_data) == 3
        for band, banddata in row_data.iteritems():
            assert isinstance(banddata, tuple)
            assert len(banddata) == 2
        rows_data[row] = row_data

    rows_mosaic = {}
    for band in range(dst_tile_pyramid.format.profile["count"]):
        rows_mosaic[band] = ()
    for row, row_data in rows_data.iteritems():
        row_mosaic = {}
        for band, banddata in row_data.iteritems():
            assert len(banddata) == 2
            assert isinstance(banddata, tuple)
            row_mosaic[band] = np.hstack(banddata)
            assert row_mosaic[band].shape == (128, 256)
            rows_mosaic[band] += (row_mosaic[band], )

    mosaic = ()
    assert len(rows_mosaic) == 3
    for band, banddata in rows_mosaic.iteritems():
        assert isinstance(banddata, tuple)
        assert len(banddata) == 2
        band_mosaic = np.vstack(banddata)
        dst_shape = band_mosaic.shape
        mosaic += (band_mosaic, )

    assert isinstance(mosaic, tuple)
    assert len(mosaic) == 3
    for band in mosaic:
        assert isinstance(band, np.ndarray)
        assert band.shape == (256, 256)

    dst_left, dst_bottom, dst_right, dst_top = dst_tile_pyramid.tile_bounds(
        *dst_tile,
        pixelbuffer=dst_pixelbuffer
        )
    dst_width, dst_height = dst_shape


    # Create tile affine
    px_size = src_tile_pyramid.pixel_x_size(zoom)
    tile_geotransform = (dst_left, px_size, 0.0, dst_top, 0.0, -px_size)
    dst_affine = Affine.from_gdal(*tile_geotransform)

    dst_metadata = deepcopy(dst_tile_pyramid.format.profile)
    dst_metadata.pop("transform", None)
    dst_metadata["nodata"] = 255#dst_tile_pyramid.format.profile["nodata"]
    dst_metadata["crs"] = dst_tile_pyramid.crs['init']
    dst_metadata["width"] = dst_width
    dst_metadata["height"] = dst_height
    dst_metadata["affine"] = dst_affine
    dst_metadata["count"] = len(mosaic)
    dst_metadata["dtype"] = dst_tile_pyramid.format.profile["dtype"]

    return dst_metadata, mosaic


def write_raster(
    process,
    metadata,
    bands,
    pixelbuffer=0
    ):
    try:
        assert isinstance(bands, tuple)
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
