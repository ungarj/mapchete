from typing import List
from shapely import box
from shapely.affinity import translate
from mapchete.bounds import Bounds
from mapchete.geometry.shape import to_shape
from mapchete.geometry.types import MultipartGeometry
from mapchete.grid import Grid
from mapchete.tile import BufferedTilePyramid
from mapchete.geometry.types import Geometry


def clip_geometry_to_pyramid_bounds(
    geometry: Geometry, pyramid: BufferedTilePyramid
) -> List[Geometry]:
    """
    Clip input geometry to SRS bounds of given TilePyramid.

    If geometry passes the antimeridian, it will be split up in a multipart
    geometry and shifted to within the SRS boundaries.
    Note: geometry SRS must be the TilePyramid SRS!

    - geometry: any shapely geometry
    - pyramid: a TilePyramid object
    - multipart: return list of geometries instead of a GeometryCollection
    """
    if not geometry.is_valid:  # pragma: no cover
        raise ValueError("invalid geometry given")
    pyramid_bbox = box(*pyramid.bounds)

    # Special case for global tile pyramids if geometry extends over tile
    # pyramid boundaries (such as the antimeridian).
    if pyramid.is_global and not geometry.within(pyramid_bbox):
        inside_geom = geometry.intersection(pyramid_bbox)
        outside_geom = geometry.difference(pyramid_bbox)
        # shift outside geometry so it lies within SRS bounds
        if isinstance(outside_geom, MultipartGeometry):
            outside_geoms = outside_geom.geoms
        else:
            outside_geoms = [outside_geom]
        all_geoms = [inside_geom]
        for geom in outside_geoms:
            geom_bounds = Bounds.from_inp(geom.bounds)
            if geom_bounds.left < pyramid.left:
                geom = translate(geom, xoff=2 * pyramid.right)
            elif geom_bounds.right > pyramid.right:
                geom = translate(geom, xoff=-2 * pyramid.right)
            all_geoms.append(geom)
        return all_geoms

    else:
        return [geometry]


def clip_grid_to_pyramid_bounds(grid: Grid, pyramid: BufferedTilePyramid) -> List[Grid]:
    """
    Clip input grid to SRS bounds of given TilePyramid.

    If grid geometry passes the antimeridian, it will be split up in a multipart
    geometry and shifted to within the SRS boundaries.
    Note: geometry SRS must be the TilePyramid SRS!

    - geometry: any shapely geometry
    - pyramid: a TilePyramid object
    - multipart: return list of geometries instead of a GeometryCollection
    """
    pyramid_bbox = box(*pyramid.bounds)

    # Special case for global tile pyramids if geometry extends over tile
    # pyramid boundaries (such as the antimeridian).
    if pyramid.is_global and not to_shape(grid).within(pyramid_bbox):
        inside_grid_geom = to_shape(grid).intersection(pyramid_bbox)
        outside_grid_geom = to_shape(grid).difference(pyramid_bbox)
        # shift outside geometry so it lies within SRS bounds
        if isinstance(outside_grid_geom, MultipartGeometry):  # pragma: no cover
            outside_geoms = outside_grid_geom.geoms
        else:
            outside_geoms = [outside_grid_geom]
        all_grids = [grid.extract(inside_grid_geom.bounds)]
        for geom in outside_geoms:
            geom_bounds = Bounds.from_inp(geom.bounds)
            if geom_bounds.left < pyramid.left:
                geom = translate(geom, xoff=2 * pyramid.right)
            elif geom_bounds.right > pyramid.right:  # pragma: no cover
                geom = translate(geom, xoff=-2 * pyramid.right)
            all_grids.append(grid.extract(geom.bounds))
        return all_grids

    else:
        return [grid]
