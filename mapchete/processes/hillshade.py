import logging

logger = logging.getLogger(__name__)


def execute(
    mp,
    resampling="nearest",
    azimuth=315.0,
    altitude=45.0,
    z=1.0,
    scale=1.0,
    td_matching_method="gdal",
    td_matching_max_zoom=None,
    td_matching_precision=8,
    td_fallback_to_higher_zoom=False,
    clip_pixelbuffer=0,
    **kwargs
):
    """
    Extract contour lines from DEM.

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
    azimuth : float
        Light source direction in degrees. (default: 315, top left)
    altitude : float
        Light source altitude angle in degrees. (default: 45)
    z : float
        Vertical DEM exaggeration factor. (default: 1)
    scale : float
        Scale factor of pixel size units versus height units (insert 112000
        when having elevation values in meters in a geodetic projection).
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
    np.ndarray
    """
    # read clip geometry
    if "clip" in mp.params["input"]:
        clip_geom = mp.open("clip").read()
        if not clip_geom:
            logger.debug("no clip data over tile")
            return "empty"
    else:
        clip_geom = []

    with mp.open("dem",) as dem:
        logger.debug("reading input raster")
        dem_data = dem.read(
            resampling=resampling,
            matching_method=td_matching_method,
            matching_max_zoom=td_matching_max_zoom,
            matching_precision=td_matching_precision,
            fallback_to_higher_zoom=td_fallback_to_higher_zoom
        )
        if dem_data.mask.all():
            logger.debug("raster empty")
            return "empty"

    logger.debug("calculate hillshade")
    hillshade = mp.hillshade(
        dem_data,
        azimuth=azimuth,
        altitude=altitude,
        z=z,
        scale=scale,
    )

    if clip_geom:
        logger.debug("clipping output with geometry")
        # apply original nodata mask and clip
        return mp.clip(hillshade, clip_geom, clip_buffer=clip_pixelbuffer)
    else:
        return hillshade
