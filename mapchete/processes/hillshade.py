"""
Calculate hillshade and slopeshade.

Original code is from:
https://github.com/migurski/DEM-Tools/blob/master/Hillup/data/__init__.py#L288-L318

License
-----------------------
Copyright (c) 2011, Michal Migurski, Nelson Minar

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the followg conditions are met:

- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the followg disclaimer.
- Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the followg disclaimer in the documentation
  and/or other materials provided with the distribution.
- Neither the name of the project nor the names of its contributors may be
  used to endorse or promote products derived from this software without
  specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import logging
import math
import warnings
from itertools import product
from typing import Optional, Tuple

import numpy as np
import numpy.ma as ma
from affine import Affine

from mapchete import Empty, RasterInput, VectorInput
from mapchete.io import MatchingMethod
from mapchete.types import ResamplingLike

logger = logging.getLogger(__name__)


def execute(
    dem: RasterInput,
    clip: Optional[VectorInput] = None,
    resampling: ResamplingLike = "nearest",
    azimuth: float = 315.0,
    altitude: float = 45.0,
    z: float = 1.0,
    scale: float = 1.0,
    td_matching_method: MatchingMethod = MatchingMethod.gdal,
    td_matching_max_zoom: Optional[int] = None,
    td_matching_precision: int = 8,
    td_fallback_to_higher_zoom: bool = False,
) -> ma.MaskedArray:
    """
    Calculate hillshade from elevation.
    """
    # read clip geometry
    if clip is None:
        clip_geom = []
    else:
        clip_geom = clip.read()
        if not clip_geom:
            logger.debug("no clip data over tile")
            raise Empty

    if dem.is_empty():
        raise Empty

    logger.debug("reading input raster")
    elevation_data = dem.read(
        resampling=resampling,
        matching_method=td_matching_method,
        matching_max_zoom=td_matching_max_zoom,
        matching_precision=td_matching_precision,
        fallback_to_higher_zoom=td_fallback_to_higher_zoom,
    )

    if elevation_data.mask.all():  # pragma: no cover
        raise Empty

    logger.debug("calculate hillshade")
    return hillshade(
        elevation_data,
        dem.tile.affine,
        azimuth=azimuth,
        altitude=altitude,
        z=z,
        scale=scale,
    )


def calculate_slope_aspect(
    elevation: np.ndarray, xres: float, yres: float, z: float = 1.0, scale: float = 1.0
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Calculate slope and aspect map.

    Return a pair of arrays 2 pixels smaller than the input elevation array.

    Slope is returned in radians, from 0 for sheer face to pi/2 for
    flat ground. Aspect is returned in radians, counterclockwise from -pi
    at north around to pi.

    Logic here is borrowed from hillshade.cpp:
    http://www.perrygeo.net/wordpress/?p=7

    Parameters
    ----------
    elevation : array
        input elevation data
    xres : float
        column width
    yres : float
        row  height
    z : float
        vertical exaggeration factor
    scale : float
        scale factor of pixel size units versus height units (insert 112000
        when having elevation values in meters in a geodetic projection)

    Returns
    -------
    slope shade : array
    """
    with warnings.catch_warnings():
        # this is to filter out division by zero warnings
        warnings.simplefilter("ignore", category=RuntimeWarning)
        z = float(z)
        scale = float(scale)
        height, width = elevation.shape[0] - 2, elevation.shape[1] - 2
        w = [
            z * elevation[row : (row + height), col : (col + width)]
            for (row, col) in product(range(3), range(3))
        ]
        x = ((w[0] + w[3] + w[3] + w[6]) - (w[2] + w[5] + w[5] + w[8])) / (
            8.0 * xres * scale
        )
        y = ((w[6] + w[7] + w[7] + w[8]) - (w[0] + w[1] + w[1] + w[2])) / (
            8.0 * yres * scale
        )
        # in radians, from 0 to pi/2
        slope = math.pi / 2 - np.arctan(np.sqrt(x * x + y * y))
        # in radians counterclockwise, from -pi at north back to pi
        aspect = np.arctan2(x, y)
        return slope, aspect


def hillshade(
    elevation: ma.MaskedArray,
    affine: Affine,
    azimuth: float = 315.0,
    altitude: float = 45.0,
    z: float = 1.0,
    scale: float = 1.0,
) -> ma.MaskedArray:
    """
    Return hillshaded numpy array.

    Parameters
    ----------
    elevation : array
        Input elevation data.
    tile : Tile
        Tile covering the array.
    azimuth : float
        Light source direction in degrees. (default: 315, top left)
    altitude : float
        Light source altitude angle in degrees. (default: 45)
    z : float
        Vertical DEM exaggeration factor. (default: 1)
    scale : float
        Scale factor of pixel size units versus height units (insert 112000
        when having elevation values in meters in a geodetic projection).
    """
    elevation = elevation[0] if elevation.ndim == 3 else elevation
    azimuth = float(azimuth)
    altitude = float(altitude)
    z = float(z)
    scale = float(scale)
    xres = affine[0]
    yres = affine[4]
    slope, aspect = calculate_slope_aspect(elevation, xres, yres, z=z, scale=scale)
    deg2rad = math.pi / 180.0
    # shaded has values between -1.0 and +1.0
    shaded = np.sin(altitude * deg2rad) * np.sin(slope) + np.cos(
        altitude * deg2rad
    ) * np.cos(slope) * np.cos((azimuth - 90.0) * deg2rad - aspect)
    # stretch to 0 - 255 and add one pixel padding using the edge values
    return ma.masked_array(
        data=np.pad(np.clip(shaded * 255.0, 1, 255).astype(np.uint8), 1, mode="edge"),
        mask=elevation.mask,
    )
