#!/usr/bin/env python

import py_compile
import os
import imp
import traceback
from flask import send_file
from PIL import Image
import io
import rasterio
import numpy as np
import numpy.ma as ma
import threading

from tilematrix import TilePyramid, MetaTilePyramid, Tile, read_raster_window
from .io_utils import RasterFileTile, RasterProcessTile, write_raster


class Mapchete(object):
    """
    Class handling MapcheteProcesses and MapcheteConfigs. Main acces point to
    get, retrieve MapcheteTiles or seed entire pyramids.
    """
    def __repr__(self):
        return "<objec 'Mapchete'>"

    def __str__(self):
        return 'Mapchete: %s' % self.config.mapchete_file

    def __init__(
        self,
        config,
        ):
        """
        Initialize with a .mapchete file and optional zoom & bound parameters.
        """
        try:
            self.config = config
            base_tile_pyramid = TilePyramid(self.config.output_type)
            try:
                base_tile_pyramid.set_format(self.config.output_format)
            except:
                raise
            self.tile_pyramid = MetaTilePyramid(
                base_tile_pyramid,
                self.config.metatiling
            )
            self.format = self.tile_pyramid.format
        except:
            raise
        try:
            py_compile.compile(self.config.process_file, doraise=True)
        except:
            raise
        self.process_name = os.path.splitext(
            os.path.basename(self.config.process_file)
        )[0]
        self.tile_cache = {}
        # self.tile_lock = threading.Lock()
        # print "tile_lock initialized", self.process_name, self.tile_lock

    def tile(self, tile):
        """
        Takes a Tile object and adds process specific metadata.
        """
        return MapcheteTile(self, tile)

    def get_work_tiles(self):
        """
        Determines the tiles affected by zoom levels, bounding box and input
        data.
        """
        for zoom in self.config.zoom_levels:
            bbox = self.config.process_area(zoom)
            for tile in self.tile_pyramid.tiles_from_geom(bbox, zoom):
                yield self.tile(tile)

    def execute(self, tile, overwrite=True):
        """
        Processes and saves tile.
        """
        # print "enter execute"
        # with self.tile_lock:
        #     tile_event = self.tile_cache.get(tile.id)
        #     if not tile_event:
        #         tile_cache[tile.id] = threading.Event()
        #
        # if tile_event:
        #     tile_event.wait()

        # TODO tile locking
        # required_tiles = []
        # for tile in required_tiles:
        #     if tile not in subprocess_host.locked_tiles:
        #         subprocess_host.locked_tiles.append
        #         subprocess_host.get_tile(tile)

        if not overwrite and tile.exists():
            return tile.id, "exists", None
        new_process = imp.load_source(
            self.process_name + "Process",
            self.config.process_file
            )
        tile_process = new_process.Process(
            config=self.config,
            tile=tile,
            params=self.config.at_zoom(tile.zoom)
            )
        try:
            result = tile_process.execute()
        except:
            return tile.id, "failed", traceback.print_exc()
            raise
        finally:
            # if not tile_event:
            #     tile_event = tile_cache.get(tile.id)
            #     del tile_cache[tile.id]
            #     tile_event.set()
            tile_process = None
        if result:
            if result == "empty":
                status = "empty"
        else:
            status = "ok"
        return tile.id, status, None

    def get(self, tile, overwrite=False):
        """
        Processes if necessary and gets tile.
        """
        # return empty image if nothing to do at zoom level
        if tile.zoom not in self.config.zoom_levels:
            return self._empty_image()

        # return/process tile or crop/process metatile
        if self.config.metatiling > 1:
            metatile = MapcheteTile(
                self,
                self.tile_pyramid.tiles_from_bbox(
                    tile.bbox(),
                    tile.zoom
                ).next()
            )
            # if overwrite is on or metatile doesn't exist, generate
            if overwrite or not metatile.exists():
                try:
                    messages = self.execute(metatile)
                except:
                    raise
                print messages
                # return empty image if process messaged empty
                if messages[1] == "empty":
                    return self._empty_image()
            # return cropped image
            return send_file(
                self._cropped_metatile(metatile, tile),
                mimetype='image/png'
            )

        # return/process tile with no metatiling
        else:
            # if overwrite is on or metatile doesn't exist, generate
            if overwrite or not tile.exists():
                try:
                    messages = self.execute(tile)
                except:
                    raise
                # return empty image if process messaged empty
                if messages[1] == "empty":
                    return self._empty_image()
            return send_file(tile.path, mimetype='image/png')

    def read(self, tile, indexes=None, pixelbuffer=0, resampling="nearest"):
        """
        Reads source tile to numpy array.
        """
        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = range(1, self.tile_pyramid.profile["count"]+1)

        return tile.read(
            indexes=band_indexes,
            pixelbuffer=pixelbuffer,
            resampling=resampling
            )

    def _empty_image(self):
        """
        Creates transparent PNG
        """
        size = self.tile_pyramid.tilepyramid.tile_size
        empty_image = Image.new('RGBA', (size, size))
        return empty_image.tobytes()

    def _cropped_metatile(self, metatile, tile):
        """
        Crops metatile to tile.
        """
        metatiling = self.tile_pyramid.metatiles
        # calculate pixel boundary
        left = (tile.col % metatiling) * tile.width
        right = left + tile.width
        top = (tile.row % metatiling) * tile.height
        bottom = top + tile.height
        # open buffer image and crop metatile
        img = Image.open(metatile.path)
        cropped = img.crop((left, top, right, bottom))
        out_img = io.BytesIO()
        cropped.save(out_img, 'PNG')
        out_img.seek(0)
        return out_img


class MapcheteTile(Tile):
    """
    Defines a tile object which stores common tile parameters (see
    tilematrix.tile) as well as Mapchete functions such as get(), read() or
    execute().
    """
    def __init__(
        self,
        mapchete,
        tile,
        pixelbuffer=0,
        resampling="nearest"
        ):
        self.tile_pyramid = mapchete.tile_pyramid
        Tile.__init__(self, self.tile_pyramid, tile.zoom, tile.row, tile.col)
        self.process = mapchete
        self.config = mapchete.config
        self.nodata = self.tile_pyramid.format.profile["nodata"]
        self.indexes = self.tile_pyramid.format.profile["count"]
        self.dtype = self.tile_pyramid.format.profile["dtype"]
        self.path = self.tile_pyramid.format.get_tile_name(
            self.config.output_name,
            tile
        )

    def profile(self, pixelbuffer=0):
        """
        Returns a pixelbuffer specific metadata set.
        """
        out_meta = self.tile_pyramid.format.profile
        # create geotransform
        px_size = self.tile_pyramid.pixel_x_size(self.zoom)
        left, bottom, right, top = self.bounds(pixelbuffer=pixelbuffer)
        tile_geotransform = (left, px_size, 0.0, top, 0.0, -px_size)
        out_meta.update(
            width=self.width,
            height=self.height,
            transform=tile_geotransform,
            affine=self.affine(pixelbuffer=pixelbuffer)
        )
        return out_meta

    def exists(self):
        """
        Returns True if file exists or False if not.
        """
        return os.path.isfile(self.path)

    def read(self, indexes=None, pixelbuffer=0, resampling="nearest"):
        """
        Reads input as numpy array. Is the equivalent function of
        read_raster_window in tilematrix.io.
        """
        # TODO fix: function does not always seem to return the correct number of
        # bands.
        if indexes:
            if isinstance(indexes, list):
                band_indexes = indexes
            else:
                band_indexes = [indexes]
        else:
            band_indexes = range(1, self.indexes+1)

        if self.is_empty(pixelbuffer=pixelbuffer):
            out_empty = ()
            for index in band_indexes:
                yield self._empty_band(pixelbuffer=pixelbuffer)

        if pixelbuffer > 0:
            pass
            # determine tiles.
            # check if tiles exist
            # if not, run execute
            # read per band intersecting tiles
            # mosaick tiles
        else:
            # read bare files without buffer
            log_message = self.process.execute(self, overwrite=False)
            with rasterio.open(self.path, "r") as src:
                nodataval = src.nodata
                # Quick fix because None nodata is not allowed.
                if not nodataval:
                    nodataval = 0
                for index in band_indexes:
                    out_band = src.read(index, masked=True)
                    out_band = ma.masked_equal(out_band, nodataval)
                    yield out_band

    def _empty_band(self, pixelbuffer=0):
        """
        Creates empty, masked array.
        """
        shape = (
            self.profile(pixelbuffer)["width"],
            self.profile(pixelbuffer)["height"]
        )
        zeros = np.zeros(
            shape=(shape),
            dtype=self.tile_pyramid.format.profile["dtype"]
        )
        return ma.masked_array(
            zeros,
            mask=True
        )

    def is_empty(self, pixelbuffer=0):
        """
        Returns true if tile bounding box does not intersect with process area.
        Note: This is just a quick test. It could happen, that a tile can be
        empty anyway but this cannot be known at this stage.
        """
        process_area = self.config.process_area(self.zoom)
        if self.bbox(pixelbuffer=pixelbuffer).intersects(process_area):
            return False
        else:
            return True

    def __enter__(self):
        return self

    def __exit__(self):
        # TODO cleanup
        pass


class MapcheteProcess():
    """
    Main process class. Visible as "self" from within a user process file.
    Includes functions reading and writing data.
    """

    def __init__(
        self,
        tile,
        config=None,
        params=None
        ):
        """
        Process initialization.
        """
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""
        self.tile = tile
        self.tile_pyramid = tile.tile_pyramid
        self.params = params
        self.config = config

    def open(
        self,
        input_file,
        pixelbuffer=0,
        resampling="nearest"
        ):
        """
        Returns either a RasterFileTile or a MapcheteTile object.
        """
        if isinstance(input_file, dict):
            raise ValueError("input cannot be dict")
        # TODO add proper check for input type.
        if isinstance(input_file, str):
            return RasterFileTile(
                input_file,
                self.tile,
                pixelbuffer=pixelbuffer,
                resampling=resampling
            )
        else:
            return RasterProcessTile(
                input_file,
                self.tile,
                pixelbuffer=pixelbuffer,
                resampling=resampling
            )

    def write(
        self,
        bands,
        pixelbuffer=0
    ):
        write_raster(
            self,
            self.tile.profile(pixelbuffer=pixelbuffer),
            bands,
            pixelbuffer=pixelbuffer
        )
