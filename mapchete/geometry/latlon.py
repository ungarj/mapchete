import math
from functools import partial

from fiona.transform import transform as fiona_transform
from rasterio.crs import CRS

from mapchete.geometry.transform import custom_transform
from mapchete.types import CoordArrays, Geometry
from mapchete.types import CRSLike

LATLON_CRS = CRS.from_epsg(4326)


def longitudinal_shift(
    geometry: Geometry, offset: float = 360.0, only_negative_coords: bool = False
) -> Geometry:
    """Return geometry with either all or only Western hemisphere coordinates shifted by some offset."""
    return custom_transform(
        geometry,
        partial(
            _coords_longitudinal_shift,
            by=offset,
            only_negative_coords=only_negative_coords,
        ),
    )


def _coords_longitudinal_shift(
    coords: CoordArrays,
    by: float = 360,
    only_negative_coords: bool = False,
) -> CoordArrays:
    x_coords, y_coords = coords
    x_coords = (
        (
            x_coord + by
            if (only_negative_coords and x_coord < 0) or not only_negative_coords
            else x_coord
        )
        for x_coord in x_coords
    )
    return x_coords, y_coords


def latlon_to_utm_crs(lat: float, lon: float) -> CRS:
    min_zone = 1
    max_zone = 60
    utm_zone = (
        f"{max([min([(math.floor((lon + 180) / 6) + 1), max_zone]), min_zone]):02}"
    )
    """Return corresponding UTM zone CRS for given latitude and longitude pair."""
    hemisphere_code = "7" if lat <= 0 else "6"
    return CRS.from_string(f"EPSG:32{hemisphere_code}{utm_zone}")


def transform_to_latlon(
    geometry: Geometry, src_crs: CRSLike, width_threshold: float = 180.0
) -> Geometry:
    """Transforms a geometry to lat/lon coordinates.

    If resulting geometry crosses the Antimeridian it will be fixed by moving coordinates
    from the Western Hemisphere to outside of the lat/lon bounds on the East, making sure
    the correct geometry shape is preserved.

    As a next step, repair_antimeridian_geometry() can be applied, which then splits up
    this geometry into a multipart geometry where all of its subgeometries are within the
    lat/lon bounds again.
    """

    def _coords_transform(
        coords: CoordArrays, src_crs: CRSLike, dst_crs: CRSLike
    ) -> CoordArrays:
        return fiona_transform(src_crs, dst_crs, *coords)

    def transform_shift_coords(coords: CoordArrays) -> CoordArrays:
        out_x_coords, out_y_coords = fiona_transform(src_crs, LATLON_CRS, *coords)
        if max(out_x_coords) - min(out_x_coords) > width_threshold:
            # we probably have an antimeridian crossing here!
            out_x_coords, out_y_coords = _coords_longitudinal_shift(
                _coords_transform(coords, src_crs, LATLON_CRS),
                only_negative_coords=True,
            )
        return (out_x_coords, out_y_coords)

    return custom_transform(geometry, transform_shift_coords)
