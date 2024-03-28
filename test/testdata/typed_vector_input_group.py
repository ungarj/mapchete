"""Example process file."""

import numpy.ma as ma

from mapchete import VectorInputGroup


def execute(
    vectors: VectorInputGroup,
) -> ma.MaskedArray:
    """User defined process."""
    for vector in vectors:
        if vector.is_empty():
            return "empty"

        data = vector.read(resampling="bilinear")

    return data
