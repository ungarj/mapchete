import logging
from typing import Optional

import fiona
import pyproj
from pyproj.exceptions import CRSError
from fiona.transform import transform_geom
from rasterio.crs import CRS
from shapely.geometry import mapping, shape

from mapchete.bounds import Bounds
from mapchete.errors import ReprojectionFailed
from mapchete.geometry.latlon import LATLON_CRS
from mapchete.geometry.repair import repair
from mapchete.geometry.segmentize import get_segmentize_value, segmentize_geometry
from mapchete.geometry.shape import to_shape
from mapchete.types import (
    Geometry,
    GeometryLike,
    Polygon,
    LinearRing,
    LineString,
    MultiPolygon,
)
from mapchete.types import CRSLike
from mapchete.validate import validate_crs

logger = logging.getLogger(__name__)


CRS_BOUNDS = {
    # http://spatialreference.org/ref/epsg/wgs-84/
    CRS.from_epsg(4326): Bounds(-180.0, -90.0, 180.0, 90.0, crs=LATLON_CRS),
    # unknown source
    CRS.from_epsg(3857): Bounds(-180.0, -85.0511, 180.0, 85.0511, crs=LATLON_CRS),
    # http://spatialreference.org/ref/epsg/3035/
    CRS.from_epsg(3035): Bounds(-10.6700, 34.5000, 31.5500, 71.0500, crs=LATLON_CRS),
}


def get_crs_bounds(crs: CRS) -> Bounds:
    try:
        # get bounds from known CRSes
        return CRS_BOUNDS[crs]
    except KeyError:
        logger.debug("try to determine CRS bounds using pyproj ...")
        # try to get bounds using pyproj
        try:
            # on UTM CRS, the area_of_use is None if pyproj.CRS is initialized with CRS.to_proj4(), thus
            # prefer using CRS.to_epsg() and only use CRS.to_proj4() as backup
            pyproj_crs = (
                pyproj.CRS(crs.to_epsg())
                if crs.is_epsg_code
                else pyproj.CRS(crs.to_proj4())
            )
            if pyproj_crs.area_of_use:
                return Bounds.from_inp(pyproj_crs.area_of_use.bounds, crs=LATLON_CRS)
        except CRSError as exc:  # pragma: no cover
            logger.debug(exc)
            pass
    raise ValueError(f"bounds of CRS {crs} could not be determined")


def crs_is_epsg_4326(crs: CRS) -> bool:
    return crs == LATLON_CRS


def reproject_geometry(
    geometry: GeometryLike,
    src_crs: CRSLike,
    dst_crs: CRSLike,
    clip_to_crs_bounds: bool = True,
    error_on_clip: bool = False,
    segmentize_on_clip: bool = False,
    segmentize: bool = False,
    segmentize_fraction: float = 100.0,
    validity_check: bool = True,
    antimeridian_cutting: bool = False,
    retry_with_clip: bool = True,
    fiona_env: Optional[dict] = None,
) -> Geometry:
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
    clip_to_crs_bounds : bool
        Always clip geometries to CRS bounds. (default: True)
    antimeridian_cutting : bool
        cut geometry at Antimeridian; can result in a multipart output geometry

    Returns
    -------
    geometry : ``shapely.geometry``
    """
    src_crs = validate_crs(src_crs)
    dst_crs = validate_crs(dst_crs)
    fiona_env = fiona_env or {}
    geometry = to_shape(geometry)

    # return repaired geometry if no reprojection needed
    if src_crs == dst_crs or geometry.is_empty:
        return repair(geometry)

    crs_bounds = None
    # geometry needs to be clipped to its CRS bounds except when projecting to EPSG:4326
    if clip_to_crs_bounds and not crs_is_epsg_4326(dst_crs):
        try:
            crs_bounds = get_crs_bounds(dst_crs)
        except ValueError:
            pass

    # if we know the destination CRS has bounds, we clip the input geometry to
    # these bounds by first reprojecting it to lat/lon
    if crs_bounds:
        # reproject geometry to WGS84
        geometry_latlon = _reproject_geom(
            geometry,
            src_crs,
            LATLON_CRS,
            validity_check,
            antimeridian_cutting,
            fiona_env,
        )
        # raise error if geometry has to be clipped
        if error_on_clip and not geometry_latlon.within(shape(crs_bounds)):
            raise RuntimeError("geometry outside target CRS bounds")

        clipped_latlon = shape(crs_bounds).intersection(geometry_latlon)

        # segmentize clipped geometry using one 100th of with or height depending on
        # which is shorter
        if (segmentize_on_clip or segmentize) and isinstance(
            clipped_latlon, (Polygon, LinearRing, LineString, MultiPolygon)
        ):
            clipped_latlon = segmentize_geometry(
                clipped_latlon,
                get_segmentize_value(clipped_latlon, segmentize_fraction),
            )

        # clip geometry dst_crs boundaries and return
        return _reproject_geom(
            clipped_latlon,
            LATLON_CRS,
            dst_crs,
            validity_check,
            antimeridian_cutting,
            fiona_env,
        )

    # return without clipping if destination CRS does not have defined bounds
    try:
        if segmentize and isinstance(
            geometry, (Polygon, LinearRing, LineString, MultiPolygon)
        ):
            return _reproject_geom(
                segmentize_geometry(
                    geometry,
                    get_segmentize_value(geometry, segmentize_fraction),
                ),
                src_crs,
                dst_crs,
                validity_check,
                antimeridian_cutting,
                fiona_env,
            )
        return _reproject_geom(
            geometry,
            src_crs,
            dst_crs,
            validity_check,
            antimeridian_cutting,
            fiona_env,
        )
    except ValueError as exc:  # pragma: no cover
        if retry_with_clip:
            logger.error(
                "error when transforming %s from %s to %s: %s, trying to use CRS bounds clip",
                geometry,
                src_crs,
                dst_crs,
                exc,
            )
            try:
                return reproject_geometry(
                    geometry,
                    src_crs=src_crs,
                    dst_crs=dst_crs,
                    clip_to_crs_bounds=True,
                    error_on_clip=error_on_clip,
                    segmentize_on_clip=segmentize_on_clip,
                    segmentize=segmentize,
                    segmentize_fraction=segmentize_fraction,
                    validity_check=validity_check,
                    antimeridian_cutting=antimeridian_cutting,
                    retry_with_clip=False,
                )
            except Exception as exc:
                raise ReprojectionFailed(f"geometry cannot be reprojected: {str(exc)}")
        else:
            raise


def _reproject_geom(
    geometry: Geometry,
    src_crs: CRS,
    dst_crs: CRS,
    validity_check: bool,
    antimeridian_cutting: bool,
    fiona_env: dict,
) -> Geometry:
    if geometry.is_empty:
        return geometry

    with fiona.Env(**fiona_env):
        try:
            transformed = transform_geom(
                src_crs.to_dict(),
                dst_crs.to_dict(),
                mapping(geometry),
                antimeridian_cutting=antimeridian_cutting,
            )
        except Exception as exc:
            raise ReprojectionFailed(
                f"fiona.transform.transform_geom could not transform geometry from {src_crs} to {dst_crs}"
            ) from exc
    # Fiona >1.9 returns None if transformation errored
    if transformed is None:  # pragma: no cover
        raise ReprojectionFailed(
            f"fiona.transform.transform_geom could not transform geometry from {src_crs} to {dst_crs}"
        )
    out_geom = to_shape(transformed)
    return repair(out_geom) if validity_check else out_geom
