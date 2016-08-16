#!/usr/bin/env python
"""
Basic read, write and input file functions.
"""

import os
import fiona
from shapely.geometry import (
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    box
    )
from shapely.ops import transform
from shapely.wkt import loads
from functools import partial
import pyproj
import ogr
import rasterio
from rasterio.warp import Resampling, transform_bounds
from copy import deepcopy

from tilematrix import TilePyramid

from .raster_data import RasterProcessTile, RasterFileTile
from .numpy_data import NumpyTile

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
    if os.path.splitext(input_file)[1][1:] in ["shp", "geojson"]:
        is_vector = True
    else:
        is_vector = False

    if is_vector:
        with fiona.open(input_file) as inp:
            inp_crs = inp.crs
            bounds = inp.bounds
    else:
        with rasterio.open(input_file) as inp:
            inp_crs = inp.crs
            bounds = (
                inp.bounds.left,
                inp.bounds.bottom,
                inp.bounds.right,
                inp.bounds.top
                )

    out_bbox = bbox = box(*bounds)
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
            out_bbox = reproject_geometry(
                bbox,
                src_crs=inp_crs,
                dst_crs=out_crs
                )
        except:
            raise
    else:
        out_bbox = bbox

    # Validate and, if necessary, try to fix output geometry.
    try:
        assert out_bbox.is_valid
    except AssertionError:
        cleaned = out_bbox.buffer(0)
        try:
            assert cleaned.is_valid
        except:
            raise TypeError("invalid geometry")
        out_bbox = cleaned
    return out_bbox

def reproject_geometry(
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
    crs_bbox = box(-180, -85.0511, 180, 85.0511)
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


def get_best_zoom_level(input_file, tile_pyramid_type):
    """
    Determines the best base zoom level for a raster. "Best" means the maximum
    zoom level where no oversampling has to be done.
    """
    tile_pyramid = TilePyramid(tile_pyramid_type)
    dst_crs = tile_pyramid.crs
    with rasterio.open(input_file, "r") as src:
        xmin, ymin, xmax, ymax = transform_bounds(
            src.crs,
            dst_crs,
            *src.bounds
            )

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

def _read_metadata(self):
    """
    Returns a rasterio-like metadata dictionary adapted to tile.
    """
    if isinstance(self, (RasterProcessTile, NumpyTile)):
        out_meta = self.process.output.profile
    elif isinstance(self, RasterFileTile):
        with rasterio.open(self.input_file, "r") as src:
            out_meta = deepcopy(src.meta)
    # create geotransform
    px_size = self.tile_pyramid.pixel_x_size(self.tile.zoom)
    left = self.tile.bounds(pixelbuffer=self.pixelbuffer)[0]
    top = self.tile.bounds(pixelbuffer=self.pixelbuffer)[3]
    tile_geotransform = (left, px_size, 0.0, top, 0.0, -px_size)
    out_meta.update(
        width=self.tile.shape(self.pixelbuffer)[1],
        height=self.tile.shape(self.pixelbuffer)[0],
        transform=tile_geotransform,
        affine=self.tile.affine(pixelbuffer=self.pixelbuffer)
    )
    return out_meta
