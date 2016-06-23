#!/usr/bin/env python
import bloscpack as bp
import numpy as np

def write_numpy(
    tile,
    metadata,
    bands,
    pixelbuffer=0):
    if pixelbuffer > 0:
        raise NotImplementedError(
            "pixelbuffers on NumPy output not yet supported"
        )

    # data_compressed = blosc.pack_array(
    #     np.stack(bands),
    #     cname=tile.output.compression
    #     )
    bp.pack_ndarray_file(
        np.stack(bands),
        tile.path,
        # blosc_args={
        #     'cname': tile.output.compression}
        )

def read_numpy(
    path
    ):
    data = bp.unpack_ndarray_file(path)
    return tuple(
        element
        for element in data
    )
