import logging
from typing import List, Optional, Union

import numpy as np
from rasterio.dtypes import dtype_ranges

from mapchete import Empty, RasterInput, VectorInput
from mapchete.io import MatchingMethod
from mapchete.io.raster.array import clip_array_with_vector
from mapchete.types import BandIndexes, ResamplingLike

logger = logging.getLogger(__name__)


def execute(
    inp: Union[RasterInput, VectorInput],
    clip: Optional[VectorInput] = None,
    resampling: ResamplingLike = "nearest",
    band_indexes: Optional[BandIndexes] = None,
    td_matching_method: MatchingMethod = MatchingMethod.gdal,
    td_matching_max_zoom: Optional[int] = None,
    td_matching_precision: int = 8,
    td_fallback_to_higher_zoom: bool = False,
    clip_pixelbuffer: int = 0,
    scale_ratio: float = 1.0,
    scale_offset: float = 0.0,
    clip_to_output_dtype: Optional[str] = None,
    **kwargs,
) -> Union[np.ndarray, List[dict]]:
    """
    Convert and optionally clip input raster or vector data.

    Inputs
    ------
    inp
        Raster or vector input.
    clip (optional)
        Vector data used to clip output.

    Parameters
    ----------
    resampling : str (default: 'nearest')
        Resampling used when reading from TileDirectory.
    band_indexes : list
        Bands to be read.
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
    scale_ratio : float
        Scale factor for input values. (default: 1.0)
    scale_offset : float
        Offset to add to input values. (default: 0.0)
    clip_to_output_dtype : str
        Clip output values to range of given dtype. (default: None)

    Output
    ------
    np.ndarray
    """
    # read clip geometry
    if clip is None:
        clip_geom = []
    else:
        clip_geom = clip.read()
        if not clip_geom:
            logger.debug("no clip data over tile")
            raise Empty

    if inp.is_empty():
        raise Empty

    logger.debug("reading input data")
    if isinstance(inp, RasterInput):
        input_data = inp.read(
            indexes=band_indexes,
            resampling=resampling,
            matching_method=td_matching_method,
            matching_max_zoom=td_matching_max_zoom,
            matching_precision=td_matching_precision,
            fallback_to_higher_zoom=td_fallback_to_higher_zoom,
        )
        if scale_offset != 0.0:
            logger.debug("apply scale offset %s", scale_offset)
            input_data = input_data.astype("float64", copy=False) + scale_offset
        if scale_ratio != 1.0:
            logger.debug("apply scale ratio %s", scale_ratio)
            input_data = input_data.astype("float64", copy=False) * scale_ratio
        if (
            clip_to_output_dtype
            and (scale_offset != 0.0 or scale_ratio != 1.0)
            and clip_to_output_dtype in dtype_ranges
        ):
            logger.debug("clip to output dtype ranges")
            input_data.clip(*dtype_ranges[clip_to_output_dtype], out=input_data)

        if clip_geom:
            logger.debug("clipping output with geometry")
            # apply original nodata mask and clip
            return clip_array_with_vector(
                input_data, inp.tile.affine, clip_geom, clip_buffer=clip_pixelbuffer
            )
        else:
            return input_data

    elif isinstance(inp, VectorInput):
        input_data = inp.read()
        if clip_geom:  # pragma: no cover
            raise NotImplementedError("clipping vector data is not yet implemented")
        else:
            logger.debug(f"writing {len(input_data)} features")
            return input_data
    else:  # pragma: no cover
        raise TypeError(
            f"inp must either be of type RasterInput or VectorInput, not {type(inp)}"
        )
