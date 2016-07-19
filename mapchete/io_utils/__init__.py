#!/usr/bin/env python
"""
Makes objects from io module available
"""

from .io_utils import (
    RasterFileTile,
    RasterProcessTile,
    NumpyTile,
    write_raster,
    read_vector
)

from .formats import MapcheteOutputFormat

from .numpy_io import write_numpy
