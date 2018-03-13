"""Internal process used by mapchete pyramid command."""


import numpy as np
import numpy.ma as ma


def _stretch_array(a, minval, maxval):
    return (
        (a.astype("float32") - minval) / (maxval - minval) * 255
    ).astype("uint8")


def execute(mp):
    """Read, stretch and return tile."""
    # read parameters
    resampling = mp.params["resampling"]
    scale_method = mp.params.get("scale_method", None)
    scales_minmax = mp.params["scales_minmax"]

    with mp.open("raster", resampling=resampling) as raster_file:
        # exit if input tile is empty
        if raster_file.is_empty():
            return "empty"
        resampled = ()
        mask = ()
        # actually read data and iterate through bands
        raster_data = raster_file.read()
        if raster_data.ndim == 2:
            raster_data = ma.expand_dims(raster_data, axis=0)
        if not scale_method:
            scales_minmax = [(i, i) for i in range(len(raster_data))]
        for band, scale_minmax in zip(raster_data, scales_minmax):
            if scale_method in ["dtype_scale", "minmax_scale"]:
                scale_min, scale_max = scale_minmax
                resampled += (_stretch_array(band, scale_min, scale_max), )
            elif scale_method == "crop":
                scale_min, scale_max = scale_minmax
                band[band > scale_max] = scale_max
                band[band <= scale_min] = scale_min
                resampled += (band, )
            else:
                resampled += (band, )
            mask += (band.mask, )

    return ma.masked_array(np.stack(resampled), np.stack(mask))
