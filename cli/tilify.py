#!/usr/bin/env python

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
        resampling = self.params["resampling"]
        if "scale_method" in self.params:
            scale_method = self.params["scale_method"]
        else:
            scale_method = None
        scales_minmax = self.params["scales_minmax"]

        with self.open(
            self.params["input_files"]["raster"],
            resampling=resampling
            ) as raster_file:
            if raster_file.is_empty():
                return "empty"
            resampled = ()
            if raster_file.indexes == 1:
                reader = [raster_file.read()]
            else:
                reader = raster_file.read()
            for band, scale_minmax in zip(
                reader,
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
