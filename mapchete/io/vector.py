"""Functions handling vector data."""

import os
import logging
import fiona
from fiona.transform import transform_geom
from rasterio.crs import CRS
from shapely.geometry import (
    box, shape, mapping, MultiPoint, MultiLineString, MultiPolygon, Polygon,
    LinearRing, LineString)
from shapely.errors import TopologicalError
from shapely.validation import explain_validity
import six
from tilematrix import clip_geometry_to_srs_bounds
from itertools import chain

logger = logging.getLogger(__name__)

# suppress shapely warnings
logging.getLogger("shapely").setLevel(logging.ERROR)
logging.getLogger("Fiona").setLevel(logging.ERROR)

CRS_BOUNDS = {
    # http://spatialreference.org/ref/epsg/wgs-84/
    'epsg:4326': (-180., -90., 180., 90.),
    # unknown source
    'epsg:3857': (-180., -85.0511, 180., 85.0511),
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
    src_crs : ``rasterio.crs.CRS`` or EPSG code
        CRS of source data
    dst_crs : ``rasterio.crs.CRS`` or EPSG code
        target CRS
    error_on_clip : bool
        raises a ``RuntimeError`` if a geometry is outside of CRS bounds
        (default: False)
    validity_check : bool
        checks if reprojected geometry is valid and throws ``TopologicalError``
        if invalid (default: True)

    Returns
    -------
    geometry : ``shapely.geometry``
    """
    src_crs = _validated_crs(src_crs)
    dst_crs = _validated_crs(dst_crs)

    def _repair(geom):
        if geom.geom_type in ["Polygon", "MultiPolygon"]:
            return geom.buffer(0)
        else:
            return geom

    def _reproject_geom(geometry, src_crs, dst_crs):
        if geometry.is_empty or src_crs == dst_crs:
            return _repair(geometry)
        out_geom = _repair(
            to_shape(
                transform_geom(
                    src_crs.to_dict(), dst_crs.to_dict(), mapping(geometry)
                )
            )
        )
        if validity_check and (not out_geom.is_valid or out_geom.is_empty):
            raise TopologicalError("invalid geometry after reprojection")
        return out_geom

    # return repaired geometry if no reprojection needed
    if src_crs == dst_crs:
        return _repair(geometry)

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
        geometry_4326 = _reproject_geom(geometry, src_crs, wgs84_crs)
        # raise error if geometry has to be clipped
        if error_on_clip and not geometry_4326.within(crs_bbox):
            raise RuntimeError("geometry outside target CRS bounds")
        # clip geometry dst_crs boundaries and return
        return _reproject_geom(
            crs_bbox.intersection(geometry_4326), wgs84_crs, dst_crs
        )

    # return without clipping if destination CRS does not have defined bounds
    else:
        return _reproject_geom(geometry, src_crs, dst_crs)


def _validated_crs(crs):
    if isinstance(crs, CRS):
        return crs
    elif isinstance(crs, six.string_types):
        return CRS().from_epsg(int(crs))
    elif isinstance(crs, int):
        return CRS().from_epsg(crs)
    else:
        raise TypeError("invalid CRS given")


def segmentize_geometry(geometry, segmentize_value):
    """
    Segmentize Polygon outer ring by segmentize value.

    Just Polygon geometry type supported.

    Parameters
    ----------
    geometry : ``shapely.geometry``
    segmentize_value: float

    Returns
    -------
    geometry : ``shapely.geometry``
    """
    if geometry.geom_type != "Polygon":
        raise TypeError("segmentize geometry type must be Polygon")
    points = []
    p_xy = None
    for xy in geometry.exterior.coords:
        if p_xy is not None:
            line_segment = LineString([p_xy, xy])
            points.extend([
                line_segment.interpolate(segmentize_value * i).coords[0]
                for i in range(int(line_segment.length / segmentize_value))
            ])
        p_xy = xy
        points.append(xy)
    return Polygon(LinearRing(points))


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
            tile.bbox, tile.tile_pyramid, multipart=True
        )
        return chain.from_iterable(
            _get_reprojected_features(
                input_file=input_file, dst_bounds=bbox.bounds,
                dst_crs=tile.crs, validity_check=validity_check
            )
            for bbox in tile_boxes)
    else:
        features = _get_reprojected_features(
            input_file=input_file, dst_bounds=tile.bounds, dst_crs=tile.crs,
            validity_check=validity_check
        )
        return features


def write_vector_window(
    in_data=None, out_schema=None, out_tile=None, out_path=None
):
    """
    Write features to GeoJSON file.

    Parameters
    ----------
    in_data : features
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

    out_features = []
    for feature in in_data:
        try:
            feature_geom = to_shape(feature["geometry"])
            # clip feature geometry to tile bounding box and append for writing
            # if clipped feature still
            out_geom = clean_geometry_type(
                feature_geom.intersection(out_tile.bbox),
                out_schema["geometry"])
            if out_geom:
                out_features.append({
                    "geometry": mapping(out_geom),
                    "properties": feature["properties"]})
        except Exception:
            logger.exception("failed to prepare geometry for writing")
            continue

    if out_features:
        # Write data
        with fiona.open(
            out_path, 'w', schema=out_schema, driver="GeoJSON",
            crs=out_tile.crs.to_dict()
        ) as dst:
            for feature in out_features:
                dst.write(feature)


def _get_reprojected_features(
    input_file=None, dst_bounds=None, dst_crs=None, validity_check=False
):
    with fiona.open(input_file, 'r') as vector:
        vector_crs = CRS(vector.crs)
        # Reproject tile bounding box to source file CRS for filter:
        if vector_crs == dst_crs:
            dst_bbox = box(*dst_bounds)
        else:
            dst_bbox = reproject_geometry(
                box(*dst_bounds), src_crs=dst_crs, dst_crs=vector_crs,
                validity_check=True
            )
        for feature in vector.filter(bbox=dst_bbox.bounds):
            feature_geom = to_shape(feature['geometry'])
            if not feature_geom.is_valid:
                feature_geom = feature_geom.buffer(0)
                # skip feature if geometry cannot be repaired
                if not feature_geom.is_valid:
                    logger.exception(
                        "feature omitted: %s", explain_validity(feature_geom))
                    continue
            # only return feature if geometry type stayed the same after
            # reprojecction
            geom = clean_geometry_type(
                feature_geom.intersection(dst_bbox), feature_geom.geom_type)
            if geom:
                # Reproject each feature to tile CRS
                try:
                    geom = reproject_geometry(
                        geom, src_crs=vector_crs, dst_crs=dst_crs,
                        validity_check=validity_check)
                    if validity_check and not geom.is_valid:
                        raise TopologicalError(
                            "reprojected geometry invalid: %s" % (
                                explain_validity(geom)))
                except TopologicalError:
                    logger.exception("feature omitted: reprojection failed")
                yield {
                    'properties': feature['properties'],
                    'geometry': mapping(geom)}
            else:
                logger.exception(
                    "feature omitted: geometry type changed after reprojection"
                )


def clean_geometry_type(geometry, target_type, allow_multipart=True):
    """
    Return geometry of a specific type if possible.

    Filters and splits up GeometryCollection into target types. This is
    necessary when after clipping and/or reprojecting the geometry types from
    source geometries change (i.e. a Polygon becomes a LineString or a
    LineString becomes Point) in some edge cases.

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
        "MultiPolygon": MultiPolygon}

    if target_type not in multipart_geoms.keys():
        raise TypeError("target type is not supported: %s" % target_type)

    if geometry.geom_type == target_type:
        return geometry

    elif allow_multipart:
        target_multipart_type = multipart_geoms[target_type]
        if geometry.geom_type == "GeometryCollection":
            return target_multipart_type([
                clean_geometry_type(g, target_type, allow_multipart)
                for g in geometry])
        elif any([
            isinstance(geometry, target_multipart_type),
            multipart_geoms[geometry.geom_type] == target_multipart_type
        ]):
            return geometry
    else:
        return None


def to_shape(geom):
    """
    Convert geometry to shapely geometry if necessary.

    Parameters:
    -----------
    geom : shapely geometry or GeoJSON mapping

    Returns:
    --------
    shapely geometry
    """
    return shape(geom) if isinstance(geom, dict) else geom
