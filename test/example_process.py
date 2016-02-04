#!/usr/bin/env python

from mapchete import MapcheteProcess, read_raster
import numpy as np

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

import time

class Process(MapcheteProcess):
    """
    Main process class which inherits from MapcheteProcess.
    """
    def __init__(self, config):
        MapcheteProcess.__init__(self, config)
        self.identifier = "example_process"
        self.title = "example process file",
        self.version = "dirty pre-alpha",
        self.abstract = "used for testing"

    def execute(self):
        """
        Here, the magic shall happen.
        # print tile_pyramid.tile_bounds(*tile)
        # time.sleep(0.1)
        """
        input_file = self.params["input_files"]['file2']
        metadata, data = read_raster(self, input_file)
        
