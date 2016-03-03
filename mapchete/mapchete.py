#!/usr/bin/env python

import py_compile

from tilematrix import TilePyramid, MetaTilePyramid


class Mapchete(object):
    """
    Class handling MapcheteProcesses and MapcheteConfigs. Main acces point to
    get, retrieve tiles or seed entire pyramids.
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


class MapcheteProcess():
    """
    Main process class. Needs a Mapchete configuration YAML as input.
    """

    def __init__(self, config):
        """
        Process initialization.
        """
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""
        self.tile = config["tile"]
        self.tile_pyramid = config["tile_pyramid"]
        zoom, row, col = self.tile
        self.params = config["zoom_levels"][zoom]
        self.config = config
