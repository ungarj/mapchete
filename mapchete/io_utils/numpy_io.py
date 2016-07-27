#!/usr/bin/env python

import bloscpack as bp
import numpy as np

def read_numpy(
    path
    ):
    return bp.unpack_ndarray_file(path)

def write_numpy(
    tile,
    bands,
    pixelbuffer=0):
    if pixelbuffer > 0:
        raise NotImplementedError(
            "pixelbuffers on NumPy output not yet supported"
        )
    if isinstance(bands, tuple):
        bp.pack_ndarray_file(
            np.stack(bands),
            tile.path
            )
    elif isinstance(bands, np.ndarray):
        bp.pack_ndarray_file(
            bands,
            tile.path
            )
