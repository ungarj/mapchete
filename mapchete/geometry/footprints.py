import logging

from shapely.geometry import MultiPolygon, box
from shapely.ops import unary_union

from mapchete.geometry.latlon import (
    LATLON_CRS,
    latlon_to_utm_crs,
    longitudinal_shift,
    transform_to_latlon,
)
from mapchete.geometry.repair import repair
from mapchete.geometry.reproject import reproject_geometry
from mapchete.geometry.types import Geometry
from mapchete.bounds import Bounds

logger = logging.getLogger(__name__)


def repair_antimeridian_geometry(
    geometry: Geometry, width_threshold: float = 180.0
) -> Geometry:
    """
    Repair geometry and apply fix if it crosses the Antimeridian.

    A geometry crosses the Antimeridian if it is at least partly outside of the
    lat/lon bounding box or if its width exceeds a certain threshold. This can happen
    after reprojection if the geometry coordinates are transformed separately and land
    left and right of the Antimeridian, thus resulting in a polygon spanning almost the
    whole lat/lon bounding box width.
    """
    # repair geometry if it is broken
    geometry = geometry.buffer(0)
    latlon_bbox = box(-180, -90, 180, 90)

    # only attempt to fix if geometry is too wide or reaches over the lat/lon bounds
    if (
        Bounds.from_inp(geometry).width >= width_threshold
        or not geometry.difference(latlon_bbox).is_empty
    ):
        # (1) shift only coordinates on the western hemisphere by 360°, thus "fixing"
        # the footprint, but letting it cross the antimeridian
        shifted_geometry = longitudinal_shift(geometry, only_negative_coords=True)

        # (2) split up geometry in one outside of latlon bounds and one inside
        inside = shifted_geometry.intersection(latlon_bbox)
        outside = shifted_geometry.difference(latlon_bbox)

        # (3) shift back only the polygon outside of latlon bounds by -360, thus moving
        # it back to the western hemisphere
        outside_shifted = longitudinal_shift(
            outside, offset=-360, only_negative_coords=False
        )

        # (4) create a MultiPolygon out from these two polygons
        geometry = unary_union([inside, outside_shifted])

    return geometry


def buffer_antimeridian_safe(footprint: Geometry, buffer_m: float = 0) -> Geometry:
    """Buffer geometry by meters and make it Antimeridian-safe.

    Safe means that if it crosses the Antimeridian and is a MultiPolygon,
    the buffer will only be applied to the edges facing away from the Antimeridian
    thus leaving the polygon intact if shifted back.
    """
    if footprint.is_empty:
        return footprint

    # repair geometry if it is broken
    footprint = repair(footprint)

    if not buffer_m:
        return footprint

    if isinstance(footprint, MultiPolygon):
        # we have a shifted footprint here!
        # (1) unshift one part
        subpolygons = []
        for polygon in footprint.geoms:
            lon = polygon.centroid.x
            if lon < 0:
                polygon = longitudinal_shift(polygon)
            subpolygons.append(polygon)
        # (2) merge to single polygon
        merged = unary_union(subpolygons)
        # (3) apply buffer
        if isinstance(merged, MultiPolygon):
            buffered = unary_union(
                [
                    buffer_antimeridian_safe(polygon, buffer_m=buffer_m)
                    for polygon in merged.geoms
                ]
            )
        else:
            buffered = buffer_antimeridian_safe(merged, buffer_m=buffer_m)

        # (4) fix again
        return repair_antimeridian_geometry(buffered)

    # UTM zone CRS
    utm_crs = latlon_to_utm_crs(footprint.centroid.y, footprint.centroid.x)

    return transform_to_latlon(
        reproject_geometry(
            footprint, src_crs=LATLON_CRS, dst_crs=utm_crs, clip_to_crs_bounds=False
        ).buffer(buffer_m),
        src_crs=utm_crs,
    )
