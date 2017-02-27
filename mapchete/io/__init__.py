"""Functions for reading and writing data."""

import rasterio
from tilematrix import TilePyramid


def get_best_zoom_level(input_file, tile_pyramid_type):
    """
    Determine the best base zoom level for a raster.

    "Best" means the maximum zoom level where no oversampling has to be done.

    Parameters
    ----------
    input_file : path to raster file
    tile_pyramid_type : ``TilePyramid`` projection (``geodetic`` or
        ``mercator``)

    Returns
    -------
    zoom : integer
    """
    tile_pyramid = TilePyramid(tile_pyramid_type)
    # TODO read file bounding box from driver
    input_bbox = file_bbox(input_file, tile_pyramid)
    xmin, ymin, xmax, ymax = input_bbox.bounds
    with rasterio.open(input_file, "r") as src:
        x_dif = xmax - xmin
        y_dif = ymax - ymin
        size = float(src.width + src.height)
        avg_resolution = (
            (x_dif / float(src.width)) * (float(src.width) / size) +
            (y_dif / float(src.height)) * (float(src.height) / size)
        )

    for zoom in range(0, 25):
        if tile_pyramid.pixel_x_size(zoom) <= avg_resolution:
            return zoom-1

    raise ValueError("no fitting zoom level found")
