#!/usr/bin/env python

from mapchete import MapcheteProcess
import numpy as np
import numpy.ma as ma
from rasterio.features import rasterize
from shapely.geometry import shape

class Process(MapcheteProcess):
    """
    Main process class which inherits from MapcheteProcess.
    """
    def __init__(self, **kwargs):
        MapcheteProcess.__init__(self, **kwargs)
        self.identifier = "tilify"
        self.title = "tilifies raster dataset into tile pyramid",
        self.version = "current",
        self.abstract = "used for raster2pyramid CLI"

    def execute(self):
        """
        Rescales raster and clips to coasline.
        """
        resampling = self.params["resampling"]
        pixelbuffer = self.params["pixelbuffer"]

        pixelsize = self.tile.pixel_x_size
        with self.open(
            self.params["input_files"]["raster"],
            pixelbuffer=pixelbuffer,
            resampling=resampling
            ) as raster_file:
            if raster_file.is_empty():
                return "empty"
            bands = self.tile.tile_pyramid.format.profile["count"]
            resampled = tuple(raster_file.read(range(1, bands+1)))
            self.write(resampled)
