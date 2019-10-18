"""Clip array using vector data."""
import numpy as np
import numpy.ma as ma
from shapely.ops import unary_union
from rasterio.features import geometry_mask

from mapchete.io.vector import to_shape


def clip_array_with_vector(
    array, array_affine, geometries, inverted=False, clip_buffer=0
):
    """
    Clip input array with a vector list.

    Parameters
    ----------
    array : array
        input raster data
    array_affine : Affine
        Affine object describing the raster's geolocation
    geometries : iterable
        iterable of dictionaries, where every entry has a 'geometry' and
        'properties' key.
    inverted : bool
        invert clip (default: False)
    clip_buffer : integer
        buffer (in pixels) geometries before clipping

    Returns
    -------
    clipped array : array
    """
    # buffer input geometries and clean up
    buffered_geometries = []
    for feature in geometries:
        feature_geom = to_shape(feature["geometry"])
        if feature_geom.is_empty:
            continue
        if feature_geom.geom_type == "GeometryCollection":
            # for GeometryCollections apply buffer to every subgeometry
            # and make union
            buffered_geom = unary_union([g.buffer(clip_buffer) for g in feature_geom])
        else:
            buffered_geom = feature_geom.buffer(clip_buffer)
        if not buffered_geom.is_empty:
            buffered_geometries.append(buffered_geom)

    # mask raster by buffered geometries
    if buffered_geometries:
        if array.ndim == 2:
            return ma.masked_array(
                array, geometry_mask(
                    buffered_geometries, array.shape, array_affine,
                    invert=inverted))
        elif array.ndim == 3:
            mask = geometry_mask(
                buffered_geometries,
                (array.shape[1], array.shape[2]),
                array_affine,
                invert=inverted
            )
            return ma.masked_array(array, mask=np.stack([mask for band in array]))

    # if no geometries, return unmasked array
    else:
        fill = False if inverted else True
        return ma.masked_array(array, mask=np.full(array.shape, fill, dtype=bool))
