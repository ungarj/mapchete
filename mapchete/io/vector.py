"""Functions handling vector data."""

import os
import logging
import warnings
import pyproj
import fiona
from functools import partial
from rasterio.crs import CRS
from shapely.geometry import (
    box, shape, mapping, MultiPoint, MultiLineString, MultiPolygon)
from shapely.ops import transform
from tilematrix import clip_geometry_to_srs_bounds
from itertools import chain

from mapchete.tile import BufferedTile

# suppress shapely warnings
logging.getLogger("shapely").setLevel(logging.ERROR)

CRS_BOUNDS = {
    # http://spatialreference.org/ref/epsg/wgs-84/
    'epsg:4326': (-180.0000, -90.0000, 180.0000, 90.0000),
    # unknown source
    'epsg:3857': (-180, -85.0511, 180, 85.0511),
    # http://spatialreference.org/ref/epsg/3035/
    'epsg:3035': (-10.6700, 34.5000, 31.5500, 71.0500)
    }


def reproject_geometry(
    geometry, src_crs=None, dst_crs=None, error_on_clip=False,
    validity_check=True
):
    """
    Reproject a geometry to target CRS.

    Also, clips geometry if it lies outside the destination CRS boundary.
    Supported destination CRSes for clipping: 4326 (WGS84), 3857 (Spherical
    Mercator) and 3035 (ETRS89 / ETRS-LAEA).

    Parameters
    ----------
    geometry : ``shapely.geometry``
    src_crs : ``rasterio.crs.CRS``
        CRS of source data
    dst_crs : ``rasterio.crs.CRS``
        target CRS
    error_on_clip : bool
        raises a ``RuntimeError`` if a geometry is outside of CRS bounds
        (default: False)
    validity_check : bool
        checks if reprojected geometry is valid and throws ``RuntimeError`` if
        invalid (default: True)

    Returns
    -------
    geometry : ``shapely.geometry``
    """
    # return repaired geometry if no reprojection needed
    if src_crs == dst_crs:
        return geometry.buffer(0)

    # if geometry potentially has to be clipped, reproject to WGS84 and clip
    # with CRS bounds
    elif dst_crs.is_epsg_code and (
        dst_crs.get("init") in CRS_BOUNDS) and (  # if known CRS
        not dst_crs.get("init") == "epsg:4326"  # WGS84 does not need clipping
    ):
        wgs84_crs = CRS().from_epsg(4326)
        # get dst_crs boundaries
        crs_bbox = box(*CRS_BOUNDS[dst_crs.get("init")])
        # reproject geometry to WGS84
        geometry_4326 = _reproject_geom(
            geometry, src_crs, wgs84_crs, validity_check=validity_check)
        # raise error if geometry has to be clipped
        if error_on_clip and not geometry_4326.within(crs_bbox):
            raise RuntimeError("geometry outside target CRS bounds")
        # clip geometry dst_crs boundaries and return
        return _reproject_geom(
            crs_bbox.intersection(geometry_4326), wgs84_crs,
            dst_crs, validity_check=validity_check)

    # return without clipping if destination CRS does not have defined bounds
    else:
        return _reproject_geom(geometry, src_crs, dst_crs)


def _reproject_geom(geometry, src_crs, dst_crs, validity_check=True):
    if geometry.is_empty or src_crs == dst_crs:
        return geometry.buffer(0)
    project = partial(
        pyproj.transform, pyproj.Proj(src_crs), pyproj.Proj(dst_crs))
    out_geom = transform(project, geometry)
    # try to repair geometry as there could be a lot of self-intersection
    # errors after proj handled reprojection;
    try:
        out_geom = out_geom.buffer(0)
    except Exception:
        pass
    if validity_check and not out_geom.is_valid or out_geom.is_empty:
        raise RuntimeError("invalid geometry after reprojection")
    return out_geom


def read_vector_window(input_file, tile, validity_check=True):
    """
    Read a window of an input vector dataset.

    Also clips geometry.

    Parameters:
    -----------
    input_file : string
        path to vector file
    tile : ``Tile``
        tile extent to read data from
    validity_check : bool
        checks if reprojected geometry is valid and throws ``RuntimeError`` if
        invalid (default: True)

    Returns
    -------
    features : list
      a list of reprojected GeoJSON-like features
    """
    # Check if potentially tile boundaries exceed tile matrix boundaries on
    # the antimeridian, the northern or the southern boundary.
    tile_left, tile_bottom, tile_right, tile_top = tile.bounds
    touches_left = tile_left <= tile.tile_pyramid.left
    touches_bottom = tile_bottom <= tile.tile_pyramid.bottom
    touches_right = tile_right >= tile.tile_pyramid.right
    touches_top = tile_top >= tile.tile_pyramid.top
    is_on_edge = touches_left or touches_bottom or touches_right or touches_top
    if tile.pixelbuffer and is_on_edge:
        tile_boxes = clip_geometry_to_srs_bounds(
            tile.bbox, tile.tile_pyramid, multipart=True)
        return chain.from_iterable(
            _get_reprojected_features(
                input_file=input_file, dst_bounds=bbox.bounds,
                dst_crs=tile.crs, validity_check=validity_check)
            for bbox in tile_boxes)
    else:
        features = _get_reprojected_features(
            input_file=input_file, dst_bounds=tile.bounds, dst_crs=tile.crs,
            validity_check=validity_check)
        return features


def write_vector_window(in_tile, out_schema, out_tile, out_path):
    """
    Write features to GeoJSON file.

    Parameters
    ----------
    in_tile : ``BufferedTile``
        input tile including data
    out_schema : dictionary
        output schema for fiona
    out_tile : ``BufferedTile``
        tile used for output extent
    out_path : string
        output path for GeoJSON file
    """
    # Delete existing file.
    try:
        os.remove(out_path)
    except OSError:
        pass
    # Return if tile data is empty
    if not in_tile.data:
        return
    out_features = []
    for feature in in_tile.data:
        feature_geom = shape(feature["geometry"])
        clipped = feature_geom.intersection(out_tile.bbox)
        out_geom = clipped
        target_type = out_schema["geometry"]
        if clipped.geom_type != target_type:
            try:
                out_geom = clean_geometry_type(clipped, target_type)
            except Exception:
                warnings.warn("failed geometry cleaning during writing")
                continue
        if out_geom:
            out_features.append(dict(
                geometry=mapping(out_geom), properties=feature["properties"]))
    # Return if clipped tile data is empty
    if not out_features:
        return
    # Write data
    with fiona.open(
        out_path, 'w', schema=out_schema, driver="GeoJSON",
        crs=out_tile.crs.to_dict()
    ) as dst:
        for feature in out_features:
            dst.write(feature)


def _get_reprojected_features(
    input_file=None, dst_bounds=None, dst_crs=None, validity_check=None
):
    with fiona.open(input_file, 'r') as vector:
        vector_crs = CRS(vector.crs)
        # Reproject tile bounding box to source file CRS for filter:
        if vector_crs == dst_crs:
            dst_bbox = box(*dst_bounds)
        else:
            dst_bbox = reproject_geometry(
                box(*dst_bounds), src_crs=dst_crs, dst_crs=vector_crs,
                validity_check=True)
        for feature in vector.filter(bbox=dst_bbox.bounds):
            feature_geom = shape(feature['geometry'])
            if not feature_geom.is_valid:
                feature_geom = feature_geom.buffer(0)
                if not feature_geom.is_valid:
                    warnings.warn(
                        "irreparable geometry found in vector input file")
                    continue
            geom = clean_geometry_type(
                feature_geom.intersection(dst_bbox), feature_geom.geom_type)
            if geom:
                # Reproject each feature to tile CRS
                if vector_crs == dst_crs and validity_check:
                    assert geom.is_valid
                else:
                    try:
                        geom = reproject_geometry(
                            geom, src_crs=vector_crs, dst_crs=dst_crs,
                            validity_check=validity_check)
                    except ValueError:
                        warnings.warn("feature reprojection failed")
                yield {
                    'properties': feature['properties'],
                    'geometry': mapping(geom)
                }


def clean_geometry_type(geometry, target_type, allow_multipart=True):
    """
    Return geometry of a specific type if possible.

    Filters and splits up GeometryCollection into target types.

    Parameters
    ----------
    geometry : ``shapely.geometry``
    target_type : string
        target geometry type
    allow_multipart : bool
        allow multipart geometries (default: True)

    Returns
    -------
    cleaned geometry : ``shapely.geometry`` or None
        returns None if input geometry type differs from target type
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
        return geometry
    elif geometry.geom_type == "GeometryCollection":
        subgeoms = [
            clean_geometry_type(
                subgeom, target_type, allow_multipart=allow_multipart)
            for subgeom in geometry
        ]
        return multipart_geom(subgeoms)
    elif allow_multipart and isinstance(geometry, multipart_geom):
        return geometry
    elif multipart_geoms[geometry.geom_type] == multipart_geom:
        return geometry
    else:
        return None


def extract_from_tile(in_tile, out_tile):
    """
    Extract features from BufferedTile.

    Parameters
    ----------
    in_tile : ``BufferedTile``
    out_tile : ``BufferedTile``

    Returns
    -------
    extracted features : list
    """
    assert isinstance(in_tile, BufferedTile)
    assert isinstance(out_tile, BufferedTile)
    assert isinstance(in_tile.data, list)
    return [
        feature
        for feature in in_tile.data
        if shape(feature["geometry"]).touches(out_tile.bbox)]
