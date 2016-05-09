#!/usr/bin/env python

from mapchete import MapcheteProcess
import numpy as np
import numpy.ma as ma
from rasterio.features import rasterize
from shapely.geometry import shape

def stretch_array(a, min, max):
    return ((a.astype("float32")-min)/(max-min)*255).astype("uint8")

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
        if "scale_method" in self.params:
            scale_method = self.params["scale_method"]
        else:
            scale_method = None
        scales_minmax = self.params["scales_minmax"]

        pixelsize = self.tile.pixel_x_size
        with self.open(
            self.params["input_files"]["raster"],
            pixelbuffer=pixelbuffer,
            resampling=resampling
            ) as raster_file:
            if raster_file.is_empty():
                return "empty"
            bands = self.tile.tile_pyramid.format.profile["count"]
            resampled = ()
            for band, scale_minmax in zip(
                raster_file.read(range(1, bands+1)),
                scales_minmax
                ):
                if scale_method in ["dtype_scale", "minmax_scale"]:
                    scale_min, scale_max = scale_minmax
                    resampled += (stretch_array(band, scale_min, scale_max), )
                elif scale_method == "crop":
                    scale_min, scale_max = scale_minmax
                    band[band>scale_max] = scale_max
                    band[band<=scale_min] = scale_min
                    resampled += (band, )
                else:
                    resampled += (band, )
            self.write(resampled)
