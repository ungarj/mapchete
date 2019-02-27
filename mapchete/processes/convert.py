import logging
import numpy as np

logger = logging.getLogger(__name__)


def execute(
    mp,
    td_resampling="nearest",
    td_matching_method="gdal",
    td_matching_max_zoom=None,
    td_matching_precision=8,
    td_fallback_to_higher_zoom=False,
    clip_pixelbuffer=0,
    **kwargs
):
    """
    Convert and optionally clip input raster data.

    Inputs:
    -------
    raster
        singleband or multiband data input
    clip (optional)
        vector data used to clip output

    Parameters
    ----------
    td_resampling : str (default: 'nearest')
        Resampling used when reading from TileDirectory.
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

    with mp.open(
        "raster",
        matching_method=td_matching_method,
        matching_max_zoom=td_matching_max_zoom,
        matching_precision=td_matching_precision,
        fallback_to_higher_zoom=td_fallback_to_higher_zoom,
        resampling=td_resampling
    ) as raster:
        raster_data = raster.read()
        if raster.is_empty() or raster_data[0].mask.all():
            logger.debug("raster empty")
            return "empty"

    if clip_geom:
        # apply original nodata mask and clip
        clipped = mp.clip(
            np.where(raster_data[0].mask, mp.params["output"].nodata, raster_data),
            clip_geom,
            clip_buffer=clip_pixelbuffer,
            inverted=True
        )
        return np.where(clipped.mask, clipped, mp.params["output"].nodata)
    else:
        return np.where(raster_data[0].mask, mp.params["output"].nodata, raster_data)
