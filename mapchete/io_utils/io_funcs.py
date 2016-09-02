#!/usr/bin/env python
"""
Basic read, write and input file functions.
"""

import os
import fiona
from shapely.geometry import MultiPoint, MultiLineString, MultiPolygon, box
from shapely.wkt import loads
from shapely.ops import transform
from functools import partial
import pyproj
import ogr
import rasterio
from rasterio.warp import Resampling
from rasterio.crs import CRS
from copy import deepcopy

from tilematrix import TilePyramid

ogr.UseExceptions()

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
    try:
        multipart_geom = multipart_geoms[target_type]
    except KeyError:
        raise ValueError("target type is not supported: %s" % target_type)
    assert geometry.is_valid

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
        is_vector_file = True
    else:
        is_vector_file = False

    if is_vector_file:
        with fiona.open(input_file) as inp:
            inp_crs = CRS(inp.crs)
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
        if not is_vector_file:
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
        try:
            cleaned = out_bbox.buffer(0)
            assert cleaned.is_valid
        except Exception as e:
            raise TypeError("invalid file bbox geometry: %s" % e)
        out_bbox = cleaned
    return out_bbox

def _get_segmentize_value(input_file, tile_pyramid):
    """
    Returns the recommended segmentize value in input file units.
    """
    with rasterio.open(input_file, "r") as input_raster:
        pixelsize = input_raster.affine[0]

    return pixelsize * tile_pyramid.tile_size

def reproject_geometry(
    geometry,
    src_crs,
    dst_crs,
    error_on_clip=False,
    validity_check=True
    ):
    """
    Reproject a geometry and returns the reprojected geometry. Also, clips
    geometry if it lies outside the destination CRS boundary.
    - geometry: a shapely geometry
    - src_crs: rasterio CRS
    - dst_crs: rasterio CRS
    - error_on_clip: bool; True will raise a RuntimeError if a geometry is
        outside of CRS bounds.
    - validity_check: bool; checks if reprojected geometry is valid, otherwise
        throws RuntimeError.
    Supported CRSes for bounds clip:
    - 4326 (WGS84)
    - 3857 (Spherical Mercator)
    - 3035 (ETRS89 / ETRS-LAEA)
    """
    assert geometry.is_valid
    assert src_crs.is_valid
    assert dst_crs.is_valid

    if src_crs == dst_crs:
        return geometry

    # check if geometry has to be clipped
    if dst_crs.is_epsg_code:
        dst_epsg = int(dst_crs.to_dict()['init'].split(':')[1])
    if dst_crs.is_epsg_code and dst_epsg in CRS_BOUNDS:
        wgs84_crs = CRS().from_epsg(4326)
        # get dst_crs boundaries
        crs_bbox = box(*CRS_BOUNDS[dst_epsg])
        geometry_4326 = _reproject_geom(
            geometry,
            src_crs,
            wgs84_crs,
            validity_check=validity_check
            )
        # raise optional error if geometry has to be clipped
        if error_on_clip and not geometry_4326.within(crs_bbox):
            raise RuntimeError("geometry outside targed CRS bounds")
        # clip geometry dst_crs boundaries
        return _reproject_geom(
            crs_bbox.intersection(geometry_4326),
            wgs84_crs,
            dst_crs,
            validity_check=validity_check
            )
    else:
        # try without clipping
        return _reproject_geom(
            geometry,
            src_crs,
            dst_crs
            )

def _reproject_geom(
    geometry,
    src_crs,
    dst_crs,
    validity_check=True
    ):
    project = partial(
        pyproj.transform,
        pyproj.Proj(src_crs),
        pyproj.Proj(dst_crs)
    )
    out_geom = transform(project, geometry)
    if validity_check:
        try:
            assert out_geom.is_valid
        except:
            raise RuntimeError("invalid geometry after reprojection")
    return out_geom

CRS_BOUNDS = {
    4326: (-180.0000, -90.0000, 180.0000, 90.0000), # http://spatialreference.org/ref/epsg/wgs-84/
    3857: (-180, -85.0511, 180, 85.0511),
    3035: (-10.6700, 34.5000, 31.5500, 71.0500) # http://spatialreference.org/ref/epsg/3035/
    }

def get_best_zoom_level(input_file, tile_pyramid_type):
    """
    Determines the best base zoom level for a raster. "Best" means the maximum
    zoom level where no oversampling has to be done.
    """
    tile_pyramid = TilePyramid(tile_pyramid_type)
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

def _read_metadata(self, tile_type=None):
    """
    Returns a rasterio-like metadata dictionary adapted to tile.
    """
    if tile_type in ["RasterProcessTile", "NumpyTile"]:
        out_meta = self.process.output.profile
    elif tile_type == "RasterFileTile":
        with rasterio.open(self.input_file, "r") as src:
            out_meta = deepcopy(src.meta)
    else:
        raise AttributeError("tile_type required")
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
