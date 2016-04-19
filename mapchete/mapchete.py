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
from tempfile import NamedTemporaryFile
import logging
import logging.config

from tilematrix import (
    TilePyramid,
    MetaTilePyramid,
    Tile,
    read_raster_window,
    write_raster_window
    )
from .io_utils import (
    RasterFileTile,
    RasterProcessTile,
    write_raster,
    VectorFileTile
    )

logger = logging.getLogger("mapchete")


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
            self.tile_pyramid.format.profile.update(
                count=self.config.output_bands,
                dtype=self.config.output_dtype
            )
            if self.config.write_options:
                for option, param in self.config.write_options.iteritems():
                    self.tile_pyramid.format.profile.update(
                        {option: param}
                    )
            self.tile_pyramid.format.profile.update(
                nodata=self.config.output_nodata
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

    def tile(self, tile):
        """
        Takes a Tile object and adds process specific metadata.
        """
        return MapcheteTile(self, tile)

    def get_work_tiles(self, zoom=None):
        """
        Determines the tiles affected by zoom levels, bounding box and input
        data.
        """
        if zoom:
            bbox = self.config.process_area(zoom)
            for tile in self.tile_pyramid.tiles_from_geom(bbox, zoom):
                yield self.tile(tile)
        else:
            for zoom in reversed(self.config.zoom_levels):
                bbox = self.config.process_area(zoom)
                for tile in self.tile_pyramid.tiles_from_geom(bbox, zoom):
                    yield self.tile(tile)

    def execute(self, tile, overwrite=True):
        """
        Processes and saves tile.
        """

        # Do nothing if tile exists or overwrite is turned off.
        if not overwrite and tile.exists():
            return tile.id, "exists", None
        try:
            new_process = imp.load_source(
                self.process_name + "Process",
                self.config.process_file
            )
            tile_process = new_process.Process(
                config=self.config,
                tile=tile,
                params=self.config.at_zoom(tile.zoom)
            )
        except:
            return tile.id, "failed", traceback.print_exc()

        # Generate tile using the user defined process.
        if not self.config.baselevel or tile.zoom == self.config.baselevel["zoom"]:
            try:
                result = tile_process.execute()
            except:
                return tile.id, "failed", traceback.print_exc()
                raise
            finally:
                tile_process = None

            message = None
            if result:
                if result == "empty":
                    status = "empty"
                else:
                    status = "custom"
                    message = result
            else:
                status = "processed"
            return tile.id, status, message

        # If baselevel interpolation is activated, generate from neighbor zooms.
        else:
            if tile.zoom < self.config.baselevel["zoom"]:
                # determine tiles from next zoom level
                process_area = self.config.process_area(tile.zoom+1)
                tile_process_area = process_area.intersection(tile.bbox())
                subtiles = list(
                    MapcheteTile(self, subtile)
                    for subtile in self.tile_pyramid.tiles_from_geom(
                        tile_process_area,
                        tile.zoom+1
                    )
                )
                # TODO create option for on demand processing if subtile is not
                # available.

                # create temporary VRT and create new tile from resampled
                # subtiles.
                subtile_paths = [
                    subtile.path
                    for subtile in subtiles
                    if subtile.exists()
                ]
                if len(subtile_paths) == 0:
                    return tile.id, "empty", None
                temp_vrt = NamedTemporaryFile()
                build_vrt = "gdalbuildvrt %s %s > /dev/null" %(
                    temp_vrt.name,
                    ' '.join(subtile_paths)
                    )
                try:
                    os.system(build_vrt)
                    assert os.path.isfile(temp_vrt.name)
                except:
                    build_vrt = "gdalbuildvrt %s %s" %(
                        temp_vrt.name,
                        ' '.join(subtile_paths)
                        )
                    os.system(build_vrt)
                    return tile.id, "failed", "GDAL VRT building"
                try:
                    assert os.path.isfile(temp_vrt.name)
                    bands = tuple(read_raster_window(
                        temp_vrt.name,
                        tile,
                        resampling=self.config.baselevel["resampling"]
                    ))
                except:
                    return tile.id, "failed", traceback.print_exc()
                try:
                    write_raster(
                        tile_process,
                        self.tile_pyramid.format.profile,
                        bands
                    )
                    return tile.id, "processed", None
                except:
                    return tile.id, "failed", traceback.print_exc()

            elif tile.zoom > self.config.baselevel["zoom"]:
                # determine tiles from previous zoom level
                process_area = self.config.process_area(tile.zoom-1)
                supertile =  list(
                    MapcheteTile(self, supertile)
                    for supertile in self.tile_pyramid.tiles_from_geom(
                        tile.bbox(),
                        tile.zoom-1
                    )
                )[0]
                # check if tiles exist and if not, execute subtiles
                if not overwrite and supertile.exists():
                    # logger.info((tile.id, "exists", None))
                    pass
                else:
                    pass
                if not supertile.exists():
                    # TODO create option for on demand processing
                    return tile.id, "empty", "source tile does not exist"
                try:
                    bands = tuple(read_raster_window(
                        supertile.path,
                        tile,
                        resampling=self.config.baselevel["resampling"]
                    ))
                    write_raster(
                        tile_process,
                        self.tile_pyramid.format.profile,
                        bands
                    )
                    return tile.id, "processed", None
                except:
                    return tile.id, "failed", traceback.print_exc()
                    raise

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
                    logger.error(messages)
                    raise
                logger.info(messages)
                # return empty image if process messaged empty
                if messages[1] == "empty":
                    return self._empty_image()
                if messages[1] == "failed":
                    logger.error(messages)
                    raise IOError(messages)
            else:
                # return cropped image
                assert metatile.exists()
                try:
                    logger.info((metatile.id, tile.id, "return cropped metatile"))
                    return send_file(
                        self._cropped_metatile(metatile, tile),
                        mimetype='image/png'
                    )
                except Exception as e:
                    logger.error(tile.id, "failed", e)
                    raise

        # return/process tile with no metatiling
        else:
            tile = MapcheteTile(self, tile)
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
        assert metatile.exists()
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
        Returns either a fiona vector dictionary, a RasterFileTile or a
        MapcheteTile object.
        """
        if isinstance(input_file, dict):
            raise ValueError("input cannot be dict")
        # TODO add proper check for input type.
        if isinstance(input_file, str):
            extension = os.path.splitext(input_file)[1][1:]
            if extension in ["shp", "geojson"]:
                return VectorFileTile(
                    input_file,
                    self.tile,
                    pixelbuffer=pixelbuffer
                )
            else:
                # if zoom < baselevel, iterate per zoom level until baselevel
                # and resample tiles using their 4 parents.
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
