import logging
from typing import Optional

import numpy.ma as ma

from mapchete import Empty, RasterInput, VectorInput
from mapchete.io import MatchingMethod
from mapchete.types import BandIndexes, ResamplingLike

logger = logging.getLogger(__name__)


def execute(
    inp: RasterInput,
    clip: VectorInput,
    resampling: ResamplingLike = "nearest",
    band_indexes: Optional[BandIndexes] = None,
    td_matching_method: MatchingMethod = MatchingMethod.gdal,
    td_matching_max_zoom: Optional[int] = None,
    td_matching_precision: int = 8,
    td_fallback_to_higher_zoom: bool = False,
    clip_pixelbuffer: int = 0,
    **kwargs,
) -> ma.MaskedArray:
    """
    Clip raster with vector data.

    """
    # read clip geometry
    if clip.is_empty():
        raise Empty("no clip data over tile")
    elif inp.is_empty():
        raise Empty("no data over tile")

    logger.debug("reading input data")
    input_data = inp.read(
        indexes=band_indexes,
        resampling=resampling,
        matching_method=td_matching_method,
        matching_max_zoom=td_matching_max_zoom,
        matching_precision=td_matching_precision,
        fallback_to_higher_zoom=td_fallback_to_higher_zoom,
    )
    logger.debug("clipping output with geometry")
    # apply original nodata mask and clip
    return ma.MaskedArray(
        input_data,
        mask=input_data.mask
        | clip.read_as_raster_mask(
            pixelbuffer=clip_pixelbuffer, band_count=input_data.shape[0]
        ),
        dtype=input_data.dtype,
        fill_value=input_data.fill_value,
    )
