#!/usr/bin/env python

from mapchete import MapcheteProcess

class Process(MapcheteProcess):
    """Main process class"""
    def __init__(self, **kwargs):
        """Process initialization"""
        # init process
        MapcheteProcess.__init__(self, **kwargs)
        self.identifier = "my_process_id",
        self.title="My long process title",
        self.version = "0.1",
        self.abstract="short description on what my process does"

    def execute(self):
        """
        To read data, use this:
        with self.open(
            self.params["input_files"]["raster_file"],
            resampling="bilinear" # other resampling methods are also available
            ) as my_raster_rgb_file:
            if my_raster_rgb_file.is_empty():
                return "empty" # this assures a transparent tile instead of a
                # pink error tile is returned when using mapchete_serve

            r, g, b = my_raster_rgb_file.read()
            # or
            r = my_raster_rgb_file.read(1)

        # or for vector data
        with self.open(
            self.params["input_files"]["vector_file"]
            ) as my_vector_file:
            if my_vector_file.is_empty():
                return "empty"

            for feature in my_vector_file.read():
                # feature["geometry"] --> shapely geometry object
                # feature["properties"] --> attributes dictionary

        Writing the output works like this:
        self.write((r, g, b)) # multiple raster bands
        # or
        self.write(r) # one raster band
        # or
        self.write(features) # vector feature lists
        """
