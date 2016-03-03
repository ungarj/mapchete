#!/usr/bin/env python

import sys
import math
import numpy as np
from itertools import product
from math import pi, sin, cos


NODATA = -1

"""
license for hillshade()
-----------------------

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

- Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
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
def hillshade(
    elevation,
    xres,
    yres,
    nodata=None,
    azimuth=315.0,
    altitude=45.0,
    z=1.0,
    scale=1.0
    ):
    """ Return a pair of arrays 2 pixels smaller than the input elevation array.

        Slope is returned in radians, from 0 for sheer face to pi/2 for
        flat ground. Aspect is returned in radians, counterclockwise from -pi
        at north around to pi.

        Logic here is borrowed from hillshade.cpp:
          http://www.perrygeo.net/wordpress/?p=7
    """
    window = []

    for row in range(3):
        for col in range(3):
            window.append(elevation[
                row:(row + elevation.shape[0] - 2),
                col:(col + elevation.shape[1] - 2)
            ])

    x = ((z * window[0] + z * window[3] + z * window[3] + z * window[6]) \
       - (z * window[2] + z * window[5] + z * window[5] + z * window[8])) \
      / (8.0 * xres * scale);

    y = ((z * window[6] + z * window[7] + z * window[7] + z * window[8]) \
       - (z * window[0] + z * window[1] + z * window[1] + z * window[2])) \
      / (8.0 * yres * scale);

    rad2deg = 180.0 / math.pi

    slope = 90.0 - np.arctan(np.sqrt(x*x + y*y)) * rad2deg

    aspect = np.arctan2(x, y)

    deg2rad = math.pi / 180.0

    shaded = np.sin(altitude * deg2rad) * np.sin(slope * deg2rad) \
           + np.cos(altitude * deg2rad) * np.cos(slope * deg2rad) \
           * np.cos((azimuth - 90.0) * deg2rad - aspect);

    shaded = shaded * 255

    if nodata is not None:
        for pane in window:
            shaded[pane == nodata] = NODATA

    # invert values & return array in original shape
    shaded = -shaded+256
    shaded[shaded<1] = 0
    return np.pad(shaded, 1, mode='constant')
