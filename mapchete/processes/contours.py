"""Contour line extraction using matplotlib."""

import logging
from typing import Generator, List, Optional

import numpy as np
from shapely.geometry import LineString, mapping, shape
from shapely.ops import unary_union

from mapchete import Empty, RasterInput, VectorInput
from mapchete.io import MatchingMethod
from mapchete.tile import BufferedTile
from mapchete.types import ResamplingLike

logger = logging.getLogger(__name__)


def execute(
    dem: RasterInput,
    clip: Optional[VectorInput] = None,
    resampling: ResamplingLike = "nearest",
    interval: float = 100,
    field: str = "elev",
    base: float = 0,
    td_matching_method: MatchingMethod = MatchingMethod.gdal,
    td_matching_max_zoom: Optional[int] = None,
    td_matching_precision: int = 8,
    td_fallback_to_higher_zoom: bool = False,
    clip_pixelbuffer=0,
) -> List[dict]:
    """
    Generate hillshade from DEM.

    Inputs
    ------
    dem
        Input DEM.
    clip (optional)
        Vector data used to clip output.

    Parameters
    ----------
    resampling : str (default: 'nearest')
        Resampling used when reading from TileDirectory.
    interval : integer
        Elevation value interval when drawing contour lines.
    field : string
        Output field name containing elevation value.
    base : integer
        Elevation base value the intervals are computed from.
    td_matching_method : str ('gdal' or 'min') (default: 'gdal')
        gdal: Uses GDAL's standard method. Here, the target resolution is
            calculated by averaging the extent's pixel sizes over both x and y
            axes. This approach returns a zoom level which may not have the
            best quality but will speed up reading significantly.
        min: Returns the zoom level which matches the minimum resolution of the
            extents four corner pixels. This approach returns the zoom level
            with the best possible quality but with low performance. If the
            tile extent is outside of the destination pyramid, a
            TopologicalError will be raised.
    td_matching_max_zoom : int (optional, default: None)
        If set, it will prevent reading from zoom levels above the maximum.
    td_matching_precision : int (default: 8)
        Round resolutions to n digits before comparing.
    td_fallback_to_higher_zoom : bool (default: False)
        In case no data is found at zoom level, try to read data from higher
        zoom levels. Enabling this setting can lead to many IO requests in
        areas with no data.
    clip_pixelbuffer : int
        Use pixelbuffer when clipping output by geometry. (default: 0)

    Output
    ------
    list of GeoJSON-like features
    """
    # read clip geometry
    if clip:
        clip_geom = clip.read()
        if not clip_geom:
            logger.debug("no clip data over tile")
            raise Empty("no clip data over tile")
    else:
        clip_geom = []

    if dem.is_empty():
        raise Empty("no DEM data over tile")

    logger.debug("reading input raster")
    dem_data = dem.read(
        1,
        resampling=resampling,
        matching_method=td_matching_method,
        matching_max_zoom=td_matching_max_zoom,
        matching_precision=td_matching_precision,
        fallback_to_higher_zoom=td_fallback_to_higher_zoom,
    )
    if dem_data.mask.all():  # pragma: no cover
        logger.debug("DEM data empty over tile")
        raise Empty("DEM data empty over tile")

    logger.debug("calculate hillshade")
    contour_lines = list(
        generate_contours(
            dem_data,
            dem.tile,
            interval=interval,
            field=field,
            base=base,
        )
    )

    if clip:
        logger.debug("clipping output with geometry")
        # use inverted clip geometry to extract contours
        clip_geom = dem.tile.bbox.difference(
            unary_union([shape(i["geometry"]) for i in clip_geom]).buffer(
                clip_pixelbuffer * dem.tile.pixel_x_size
            )
        )
        out_contours = []
        for contour in contour_lines:
            out_geom = shape(contour["geometry"]).intersection(clip_geom)
            if not out_geom.is_empty:
                out_contours.append(
                    dict(
                        contour,
                        geometry=mapping(out_geom),
                    )
                )
        return out_contours
    else:
        return contour_lines


def generate_contours(
    array: np.ndarray,
    tile: BufferedTile,
    interval: float = 100,
    field: str = "elev",
    base: float = 0,
) -> Generator[dict, None, None]:
    import matplotlib.pyplot as plt

    elevations = (
        get_contour_values(array.min(), array.max(), interval=interval, base=base) or []
    )
    for elevation, contours in zip(
        elevations, plt.contour(array, elevations).collections
    ):
        for path in contours.get_paths():
            out_coords = [
                (
                    tile.left + (y * tile.pixel_x_size),
                    tile.top - (x * tile.pixel_y_size),
                )
                for x, y in np.asarray(path.vertices)
            ]
            if len(out_coords) >= 2:
                yield dict(
                    properties={field: elevation},
                    geometry=mapping(LineString(out_coords)),
                )


def get_contour_values(
    min_val: float, max_val: float, base: float = 0, interval: float = 100
) -> List[float]:
    """Return a list of values between min and max within an interval."""
    i = base
    out = []
    if min_val < base:
        while i >= min_val:
            i -= interval
    while i <= max_val:
        if i >= min_val:
            out.append(i)
        i += interval
    return out
