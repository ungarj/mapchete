"""Functions for reading and writing data."""

import rasterio
from shapely.geometry import box
from tilematrix import TilePyramid

from mapchete.formats.default import raster_file
from mapchete.io.vector import reproject_geometry, segmentize_geometry


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
    with rasterio.open(input_file, "r") as src:
        xmin, ymin, xmax, ymax = reproject_geometry(
            segmentize_geometry(
                box(
                    src.bounds.left, src.bounds.bottom, src.bounds.right,
                    src.bounds.top
                ),
                raster_file.get_segmentize_value(input_file, tile_pyramid)
            ),
            src_crs=src.crs, dst_crs=tile_pyramid.crs
        ).bounds
        x_dif = xmax - xmin
        y_dif = ymax - ymin
        size = float(src.width + src.height)
        avg_resolution = (
            (x_dif / float(src.width)) * (float(src.width) / size) +
            (y_dif / float(src.height)) * (float(src.height) / size)
        )

    for zoom in range(0, 40):
        if tile_pyramid.pixel_x_size(zoom) <= avg_resolution:
            return zoom-1
