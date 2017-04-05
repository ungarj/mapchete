#!/usr/bin/env python

import numpy as np
import numpy.ma as ma
from mapchete import MapcheteProcess

def stretch_array(a, minval, maxval):
    return ((a.astype("float32")-minval)/(maxval-minval)*255).astype("uint8")


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
        # read parameters
        resampling = self.params["resampling"]
        if "scale_method" in self.params:
            scale_method = self.params["scale_method"]
        else:
            scale_method = None
        scales_minmax = self.params["scales_minmax"]

        with self.open("raster", resampling=resampling) as raster_file:
            # exit if input tile is empty
            if raster_file.is_empty():
                return "empty"
            resampled = ()
            mask = ()
            # actually read data and iterate through bands
            raster_data = raster_file.read()
            if raster_data.ndim == 2:
                raster_data = ma.expand_dims(raster_data, axis=0)
            if not scale_method:
                scales_minmax = [
                    (i, i)
                    for i in range(len(raster_data))
                ]
            for band, scale_minmax in zip(
                raster_data,
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
                mask += (band.mask, )

        a = ma.masked_array(np.stack(resampled), np.stack(mask))
        return a
