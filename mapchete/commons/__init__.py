"""
Useful user functions.

* ``clip()``: Clips raster data with vector geometries.
* ``contours()``: Extract contour lines from elevation raster.
* ``hillshade()``: Generate hillshade from elevation raster.

"""

from .clip import clip_array_with_vector
from .contours import extract_contours
from .hillshade import hillshade
