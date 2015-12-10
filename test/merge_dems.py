#!/usr/bin/env python

from src.mapchete import MapcheteProcess

"""
User has to:
 - provide identifier
 - provide version
 - provide configuration object (?)
 - implement process

If the process gets executed, it runs at a certain zoom level.
"""

class Process(MapcheteProcess):
    """
    Main process class which inherits from MapcheteProcess.
    """
    def __init__(self, config_path):
        """
        Process initialization from MapcheteProcess.
        """

        MapcheteProcess.__init__(self, config_path)

        self.identifier = "merge_dems"
        self.title = "Merge multiple DEMs",
        self.version = "dirty pre-alpha",
        self.abstract = "Merges multiple DEMs into one.",

    def execute(self, zoom):
        params = self.config.at_zoom(zoom)
        return params
