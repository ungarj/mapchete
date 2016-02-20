#!/usr/bin/env python

from mapchete import MapcheteProcess, read_raster
import numpy as np

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
        Define here your geo process.
        """
        # Read input data

        input_file1 = self.params["input_files"]['file1']
        metadata1, data1 = read_raster(self, input_file1, pixelbuffer=2)

        input_file2 = self.params["input_files"]['file2']
        metadata2, data2 = read_raster(self, input_file2, pixelbuffer=2)

        # Now we have two numpy arrays (data1, data2) with the input data which
        # are cropped and resampled to the current tile extent and resolution.
        # In this example, both also have a 2px buffer which may be needed for
        # some matrix filters (like hillshade, median filter etc.).

        # Now you can do with your numpy arrays what you want.
        output = np.maximum(data1, data2)

        # This function crops your output array to the tile boundaries if it has
        # a pixelbuffer and saves it accordingly using the output_format
        # provided in the process configuration.
        write_raster(
            self,
            metadata1,
            output
        )
