"""Clip array using vector data."""
import numpy as np
import numpy.ma as ma
from shapely.geometry import shape, Polygon, MultiPolygon, GeometryCollection
from rasterio.features import geometry_mask


def clip_array_with_vector(
    array, array_affine, geometries, inverted=False, clip_buffer=0
):
    """Clip input array with a vector list."""
    buffered_geometries = []
    # buffer input geometries and clean up
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
            geom = MultiPolygon(polygons)
        buffered_geometries.append(geom)
    # mask raster by buffered geometries
    if buffered_geometries:
        return ma.masked_array(
            array, mask=geometry_mask(
                buffered_geometries, array.shape, array_affine, invert=inverted
            )
        )
    # if no geometries, return empty array
    else:
        if inverted:
            fill = False
        else:
            fill = True
        return ma.masked_array(
            array, mask=np.full(array.shape, fill, dtype=bool))
