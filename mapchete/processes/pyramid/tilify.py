"""Internal process used by mapchete pyramid command."""

import logging
import numpy as np
import numpy.ma as ma

logger = logging.getLogger(__name__)


def _stretch_array(a, minval, maxval):
    return (
        (a.astype("float32") - minval) / (maxval - minval) * 255
    ).astype("uint8")


def execute(
    mp,
    resampling="nearest",
    scale_method=None,
    scales_minmax=None,
    **kwargs
):
    """Read, stretch and return tile."""
    with mp.open("raster", resampling=resampling) as raster_file:

        # exit if input tile is empty
        if raster_file.is_empty():
            return "empty"

        # actually read data and iterate through bands
        scaled = ()
        mask = ()
        raster_data = raster_file.read()
        if raster_data.ndim == 2:
            raster_data = ma.expand_dims(raster_data, axis=0)
        if not scale_method:
            scales_minmax = [(i, i) for i in range(len(raster_data))]

        for band, (scale_min, scale_max) in zip(raster_data, scales_minmax):
            if scale_method in ["dtype_scale", "minmax_scale"]:
                scaled += (_stretch_array(band, scale_min, scale_max), )
            elif scale_method == "crop":
                scaled += (np.clip(band, scale_min, scale_max), )
            else:
                scaled += (band, )
            mask += (band.mask, )

    return ma.masked_array(np.stack(scaled), np.stack(mask))
