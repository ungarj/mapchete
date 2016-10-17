#!/usr/bin/env python
"""Example process file."""

from mapchete import MapcheteProcess
from shapely.geometry import shape


class Process(MapcheteProcess):
    """Main process class."""

    def __init__(self, **kwargs):
        """Process initialization."""
        # init process
        MapcheteProcess.__init__(self, **kwargs)
        self.identifier = "my_process_id",
        self.title = "My long process title",
        self.version = "0.1",
        self.abstract = "short description on what my process does"

    def execute(self):
        """User defined process."""
        # Reading and writing data works like this:
        with self.open("file1") as vector_file:
            if vector_file.is_empty():
                return "empty"
                # This assures a transparent tile instead of a pink error tile
                # is returned when using mapchete serve.
            return [
                dict(
                    geometry=feature["geometry"],
                    properties=dict(
                        name=feature["properties"]["NAME_0"],
                        id=feature["properties"]["ID_0"],
                        area=shape(feature["geometry"]).area
                    )
                )
                for feature in vector_file.read()
            ]
