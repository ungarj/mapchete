"""Functions handling vector data."""

import warnings
import pyproj
from functools import partial
from rasterio.crs import CRS
from shapely.geometry import box
from shapely.geos import TopologicalError
from shapely.ops import transform


def reproject_geometry(
    geometry, src_crs, dst_crs, error_on_clip=False, validity_check=True
):
    """
    Reproject a geometry and returns the reprojected geometry.

    Also, clips geometry if it lies outside the destination CRS boundary.
    Supported CRSes for bounds clip: 4326 (WGS84), 3857 (Spherical Mercator)
    and 3035 (ETRS89 / ETRS-LAEA).

    - geometry: a shapely geometry
    - src_crs: rasterio CRS
    - dst_crs: rasterio CRS
    - error_on_clip: bool; True will raise a RuntimeError if a geometry is
        outside of CRS bounds.
    - validity_check: bool; checks if reprojected geometry is valid, otherwise
        throws RuntimeError.
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
        try:
            bbox_intersection = crs_bbox.intersection(geometry_4326)
        except TopologicalError:
            try:
                bbox_intersection = crs_bbox.intersection(
                    geometry_4326.buffer(0)
                    )
                warnings.warn("geometry fixed after clipping")
            except:
                raise
        # clip geometry dst_crs boundaries
        return _reproject_geom(
            bbox_intersection,
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
    geometry, src_crs, dst_crs, validity_check=True
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
    # http://spatialreference.org/ref/epsg/wgs-84/
    4326: (-180.0000, -90.0000, 180.0000, 90.0000),
    # http://spatialreference.org/ref/epsg/3035/
    3857: (-180, -85.0511, 180, 85.0511),
    # unknown source
    3035: (-10.6700, 34.5000, 31.5500, 71.0500)
    }
