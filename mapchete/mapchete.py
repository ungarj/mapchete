#!/usr/bin/env python

import py_compile
import os
import imp
import traceback
from flask import send_file
from PIL import Image
import io

from tilematrix import TilePyramid, MetaTilePyramid, Tile


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


    def get_work_tiles(self):
        """
        Determines the tiles affected by zoom levels, bounding box and input
        data.
        """
        for zoom in self.config.zoom_levels:
            bbox = self.config.process_area(5)

            for tile in self.tile_pyramid.tiles_from_geom(bbox, zoom):
                yield tile


    def execute(self, tile, overwrite=True):
        """
        Processes and saves tile.
        """
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
            tile_process = None
        if result:
            if result == "empty":
                status = "empty"
        else:
            status = "ok"
        return tile.id, status, None


    def get(self, tile, overwrite=True):
        """
        Processes if necessary and gets tile.
        """
        zoom, row, col = tile
        # return empty image if nothing to do at zoom level
        if zoom not in self.config.zoom_levels:
            return self._empty_image()

        # return/process tile or crop/process metatile
        if self.config.metatiling > 1:
            metatile = self.tile_pyramid.tiles_from_bbox(
                self.tile_pyramid.tilepyramid.tile_bbox(*tile),
                zoom
                ).next()
            # get image path
            image_path = self.tile_pyramid.format.get_tile_name(
                self.config.output_name,
                metatile
            )
            # if overwrite is on or metatile doesn't exist, generate
            if overwrite or not self.exists(metatile):
                try:
                    messages = self.execute(metatile)
                except:
                    raise
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
            # get image path
            image_path = self.tile_pyramid.format.get_tile_name(
                self.config.output_name,
                tile
            )
            # if overwrite is on or metatile doesn't exist, generate
            if overwrite or not self.exists(tile):
                try:
                    messages = self.execute(tile)
                except:
                    raise
                # return empty image if process messaged empty
                if messages[1] == "empty":
                    return self._empty_image()
            return send_file(image_path, mimetype='image/png')


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
        tile_zoom, tile_row, tile_col = tile
        tile_size = self.tile_pyramid.tilepyramid.tile_size
        metatiling = self.tile_pyramid.metatiles
        # calculate pixel boundary
        left = (tile_col % metatiling) * tile_size
        right = left + tile_size
        top = (tile_row % metatiling) * tile_size
        bottom = top + tile_size
        # open buffer image and crop metatile
        image_path = self.tile_pyramid.format.get_tile_name(
            self.config.output_name,
            metatile
        )
        img = Image.open(image_path)
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

    def __init__(self, zoom, row, col):
        self = self.tile_pyramid.tile(zoom, row, col)

    def exists(self):
        """
        Returns True if file exists or False if not.
        """
        image_path = self.tile_pyramid.format.get_tile_name(
            self.config.output_name,
            tile
        )
        return os.path.isfile(image_path)



class MapcheteProcess():
    """
    Main process class. Needs a Mapchete configuration YAML as input.
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
