#!/usr/bin/env python
"""
Common handy functions which can be used in user processes.
"""

import math
import numpy as np
import numpy.ma as ma
from itertools import product
from math import pi
import matplotlib.pyplot as plt
from rasterio.features import geometry_mask
from shapely.geometry import (
    shape,
    mapping,
    LineString,
    Polygon,
    MultiPolygon,
    GeometryCollection
    )

NODATA = -1

"""
license for calculate_slope_aspect() and hillshade()
-----------------------
Copyright (c) 2011, Michal Migurski, Nelson Minar

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
def calculate_slope_aspect(
    elevation,
    xres,
    yres,
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
    z = float(z)
    scale = float(scale)
    height, width = elevation.shape[0] - 2, elevation.shape[1] - 2

    window = [z * elevation[row:(row + height), col:(col + width)]
              for (row, col)
              in product(range(3), range(3))]

    x = ((window[0] + window[3] + window[3] + window[6]) \
       - (window[2] + window[5] + window[5] + window[8])) \
      / (8.0 * xres * scale)

    y = ((window[6] + window[7] + window[7] + window[8]) \
       - (window[0] + window[1] + window[1] + window[2])) \
      / (8.0 * yres * scale)

    # in radians, from 0 to pi/2
    slope = pi/2 - np.arctan(np.sqrt(x*x + y*y))

    # in radians counterclockwise, from -pi at north back to pi
    aspect = np.arctan2(x, y)

    return slope, aspect


def hillshade(
    elevation,
    self,
    azimuth=315.0,
    altitude=45.0,
    z=1.0,
    scale=1.0
    ):
    """
    Returns hillshaded numpy array.
    """
    azimuth = float(azimuth)
    altitude = float(altitude)
    z = float(z)
    scale = float(scale)
    xres = self.tile.pixel_x_size
    yres = -self.tile.pixel_y_size

    slope, aspect = calculate_slope_aspect(
        elevation,
        xres,
        yres,
        z=z,
        scale=scale
        )

    deg2rad = math.pi / 180.0

    shaded = np.sin(altitude * deg2rad) * np.sin(slope) \
           + np.cos(altitude * deg2rad) * np.cos(slope) \
           * np.cos((azimuth - 90.0) * deg2rad - aspect)

    shaded = (shaded - 1.0) * -128.0
    shaded = np.clip(shaded, 0, 255)

    return np.where(
        elevation.mask,
        np.zeros(elevation.shape),
        np.pad(shaded, 1, mode='constant')
        )

def clip_array_with_vector(
    array,
    array_affine,
    geometries,
    inverted=False,
    clip_buffer=0
    ):
    """
    Clips input array with a vector list.
    """

    buffered_geometries = []
    for feature in geometries:
        geom = shape(feature['geometry']).buffer(clip_buffer)
        if not isinstance(geom, (Polygon, MultiPolygon, GeometryCollection)):
            break
        if geom.is_empty:
            break
        if isinstance(geom, GeometryCollection):
            polygons = [
                subgeom
                for subgeom in geom
                if isinstance(subgeom, (Polygon, MultiPolygon))
            ]
            if not polygons:
                break
            new_geom = MultiPolygon(polygons)
            geom = new_geom
        buffered_geometries.append(geom)

    if buffered_geometries:
        mask = geometry_mask(
            buffered_geometries,
            array.shape,
            array_affine,
            invert=inverted
            )
    else:
        if inverted:
            fill = False
        else:
            fill = True
        mask = np.full(array.shape, fill, dtype=bool)

    return ma.masked_array(array, mask)


def extract_contours(
    array,
    tile,
    interval=100,
    pixelbuffer=0,
    field='elev'
    ):
    """
    Extracts contour lines from an array in a given interval and returns them
    as GeoJSON-like objects using the source tile bounds as georeference
    """
    levels = _get_contour_values(array.min(), array.max(), interval=interval)
    if not levels:
        return []
    try:
        contours = plt.contour(array, levels)
    except:
        raise
    index = 0
    out_contours = []
    left = tile.bounds(pixelbuffer)[0]
    top = tile.bounds(pixelbuffer)[3]
    for level in range(len(contours.collections)):
        elevation = levels[index]
        index += 1
        paths = contours.collections[level].get_paths()
        for path in paths:
            out_coords = [
                (
                    left+(i[1]*tile.pixel_x_size),
                    top-(i[0]*tile.pixel_y_size),
                )
                for i in zip(path.vertices[:, 1], path.vertices[:, 0])
            ]
            if len(out_coords) >= 2:
                line = LineString(out_coords)
                out_contours.append({
                        'properties': {
                            field: elevation
                        },
                        'geometry': mapping(line)
                    }
                )

    return out_contours

def _get_contour_values(min_val, max_val, base=0, interval=100):
    """
    Returns a list of values between min and max within an interval.
    """
    i = base
    out = []

    if min_val < base:
        while i >= min_val:
            i -= interval

    while i <= max_val:
        if i >= min_val:
            out.append(i)
        i += interval

    return out
