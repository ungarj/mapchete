#!/usr/bin/env python

"""Mapchete process file template."""

from mapchete import MapcheteProcess


class Process(MapcheteProcess):
    """Main process class."""

    def __init__(self, **kwargs):
        """Process initialization."""
        MapcheteProcess.__init__(self, **kwargs)
        self.identifier = "my_process_id",
        self.title = "My long process title",
        self.version = "0.1",
        self.abstract = "Short description on what my process does."

    def execute(self):
        """Insert your python code here."""
        pass
