#!/usr/bin/env python
"""
Basic read, write and input file functions.
"""

import os
import fiona
from shapely.geometry import (
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon
    )
from shapely.ops import transform
from shapely.wkt import loads
from functools import partial
import pyproj
import ogr
import rasterio
from rasterio.warp import Resampling



RESAMPLING_METHODS = {
    "nearest": Resampling.nearest,
    "bilinear": Resampling.bilinear,
    "cubic": Resampling.cubic,
    "cubic_spline": Resampling.cubic_spline,
    "lanczos": Resampling.lanczos,
    "average": Resampling.average,
    "mode": Resampling.mode
    }

def clean_geometry_type(geometry, target_type, allow_multipart=True):
    """
    Returns None if input geometry type differs from target type. Filters and
    splits up GeometryCollection into target types.
    allow_multipart allows multipart geometries (e.g. MultiPolygon for Polygon
    type and so on).
    """

    multipart_geoms = {
        "Point": MultiPoint,
        "LineString": MultiLineString,
        "Polygon": MultiPolygon,
        "MultiPoint": MultiPoint,
        "MultiLineString": MultiLineString,
        "MultiPolygon": MultiPolygon
    }
    multipart_geom = multipart_geoms[target_type]

    if geometry.geom_type == target_type:
        out_geom = geometry

    elif geometry.geom_type == "GeometryCollection":
        subgeoms = [
            clean_geometry_type(
                subgeom,
                target_type,
                allow_multipart=allow_multipart
            )
            for subgeom in geometry
        ]
        out_geom = multipart_geom(subgeoms)

    elif allow_multipart and isinstance(geometry, multipart_geom):
        out_geom = geometry

    elif multipart_geoms[geometry.geom_type] == multipart_geom:
        out_geom = geometry

    else:
        return None

    return out_geom

def file_bbox(
    input_file,
    tile_pyramid
    ):
    """
    Returns the bounding box of a raster or vector file in a given CRS.
    """
    out_crs = tile_pyramid.crs
    # Read raster data with rasterio, vector data with fiona.
    extension = os.path.splitext(input_file)[1][1:]
    if extension in ["shp", "geojson"]:
        is_vector = True
    else:
        is_vector = False

    if is_vector:
        with fiona.open(input_file) as inp:
            inp_crs = inp.crs
            left, bottom, right, top = inp.bounds
    else:
        with rasterio.open(input_file) as inp:
            inp_crs = inp.crs
            left = inp.bounds.left
            bottom = inp.bounds.bottom
            right = inp.bounds.right
            top = inp.bounds.top

    # Create bounding box polygon.
    tl = [left, top]
    tr = [right, top]
    br = [right, bottom]
    bl = [left, bottom]
    bbox = Polygon([tl, tr, br, bl])
    out_bbox = bbox
    # If soucre and target CRSes differ, segmentize and reproject
    if inp_crs != out_crs:
        if not is_vector:
            segmentize = _get_segmentize_value(input_file, tile_pyramid)
            try:
                ogr_bbox = ogr.CreateGeometryFromWkb(bbox.wkb)
                ogr_bbox.Segmentize(segmentize)
                segmentized_bbox = loads(ogr_bbox.ExportToWkt())
                bbox = segmentized_bbox
            except:
                raise
        try:
            out_bbox = _reproject(bbox, src_crs=inp_crs, dst_crs=out_crs)
        except:
            raise
    else:
        out_bbox = bbox

    # Validate and, if necessary, try to fix output geometry.
    try:
        assert out_bbox.is_valid
    except:
        cleaned = out_bbox.buffer(0)
        try:
            assert cleaned.is_valid
        except:
            raise TypeError("invalid geometry")
        out_bbox = cleaned
    return out_bbox

def _reproject(
    geometry,
    src_crs=None,
    dst_crs=None
    ):
    """
    Reproject a geometry and returns the reprojected geometry. Also, clips
    geometry if it lies outside the spherical mercator boundary.
    """
    assert src_crs
    assert dst_crs

    # clip input geometry to dst_crs boundaries if necessary
    l, b, r, t = -180, -85.0511, 180, 85.0511
    crs_bbox = Polygon((
       [l, b],
       [r, b],
       [r, t],
       [l, t],
       [l, b]
    ))
    crs_bounds = {
        "epsg:3857": crs_bbox,
        "epsg:3785": crs_bbox
    }
    if dst_crs["init"] in crs_bounds:
        project = partial(
            pyproj.transform,
            pyproj.Proj({"init": "epsg:4326"}),
            pyproj.Proj(src_crs)
        )
        src_bbox = transform(project, crs_bounds[dst_crs["init"]])
        geometry = geometry.intersection(src_bbox)

    # create reproject function
    project = partial(
        pyproj.transform,
        pyproj.Proj(src_crs),
        pyproj.Proj(dst_crs)
    )
    # return reprojected geometry
    return transform(project, geometry)

def _get_segmentize_value(input_file, tile_pyramid):
    """
    Returns the recommended segmentize value in input file units.
    """
    with rasterio.open(input_file, "r") as input_raster:
        pixelsize = input_raster.affine[0]

    return pixelsize * tile_pyramid.tile_size
