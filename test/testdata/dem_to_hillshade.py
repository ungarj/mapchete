#!/usr/bin/env python
"""Example process file."""


def execute(mp):
    """User defined process."""
    # Reading and writing data works like this:
    with mp.open("file1") as raster_file:
        if raster_file.is_empty():
            return "empty"
            # This assures a transparent tile instead of a pink error tile
            # is returned when using mapchete serve.
        dem = raster_file.read(resampling="bilinear")
    return mp.hillshade(dem).astype("uint8")
