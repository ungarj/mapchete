#!/usr/bin/env python

from mapchete import MapcheteProcess

"""
User has to:
 - execute(): implement process
 - config_path: YAML configuration file
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

    def execute(self, zoom):
        """
        Here, the magic shall happen.
        """
        params = self.config.at_zoom(zoom)
        return params
