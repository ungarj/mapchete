import logging
from shapely.geometry import mapping, shape
from shapely.ops import unary_union

logger = logging.getLogger(__name__)


def execute(
    mp,
    resampling="nearest",
    interval=100,
    field="elev",
    base=0,
    td_matching_method="gdal",
    td_matching_max_zoom=None,
    td_matching_precision=8,
    td_fallback_to_higher_zoom=False,
    clip_pixelbuffer=0,
    **kwargs
):
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
    if "clip" in mp.params["input"]:
        clip_geom = mp.open("clip").read()
        if not clip_geom:
            logger.debug("no clip data over tile")
            return "empty"
    else:
        clip_geom = []

    with mp.open(
        "dem",
    ) as dem:
        logger.debug("reading input raster")
        dem_data = dem.read(
            resampling=resampling,
            matching_method=td_matching_method,
            matching_max_zoom=td_matching_max_zoom,
            matching_precision=td_matching_precision,
            fallback_to_higher_zoom=td_fallback_to_higher_zoom,
        )
        if dem_data.mask.all():
            logger.debug("raster empty")
            return "empty"

    logger.debug("calculate hillshade")
    contours = mp.contours(
        dem_data,
        interval=interval,
        field=field,
        base=base,
    )

    if clip_geom:
        logger.debug("clipping output with geometry")
        # use inverted clip geometry to extract contours
        clip_geom = mp.tile.bbox.difference(
            unary_union([shape(i["geometry"]) for i in clip_geom]).buffer(
                clip_pixelbuffer * mp.tile.pixel_x_size
            )
        )
        out_contours = []
        for contour in contours:
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
        return contours
