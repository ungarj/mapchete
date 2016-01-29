#!/usr/bin/env python

from mapchete import MapcheteProcess

"""
To initialize, the user has to provide:
 - execute(): implement process
 - mapchete_file: a .mapchete file
optional:
 - identifier
 - title
 - version
 - abstract

If the process gets executed, it only runs at a certain zoom level.
"""

class Process(MapcheteProcess):
    """
    Main process class which inherits from MapcheteProcess.
    """
    def __init__(self, config_path):
        MapcheteProcess.__init__(self, config_path)
        self.identifier = "example_process"
        self.title = "example process file",
        self.version = "dirty pre-alpha",
        self.abstract = "used for testing",

    def execute(self, tile, tile_pyramid, params):
        """
        Here, the magic shall happen.
        """
        print tile_pyramid.tile_bounds(*tile)
        return params
