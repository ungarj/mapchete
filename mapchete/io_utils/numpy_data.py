#!/usr/bin/env python
"""
NumPy handling.
"""

# from numpy import stack, ndarray
from numpy.ma import MaskedArray
import os

from .io_funcs import RESAMPLING_METHODS, _read_metadata
from .numpy_io import read_numpy

class NumpyTile(object):
    """
    Class representing a tile (existing or virtual) of target pyramid from a
    Mapchete NumPy process output.
    """
    def __init__(
        self,
        input_mapchete,
        tile,
        pixelbuffer=0,
        resampling="nearest"
        ):

        try:
            assert os.path.isfile(input_mapchete.config.process_file)
        except:
            raise IOError("input file does not exist: %s" %
                input_mapchete.config.process_file)

        try:
            assert pixelbuffer == 0
        except:
            raise NotImplementedError(
                "pixelbuffers for NumPy data not yet supported"
            )

        try:
            assert isinstance(pixelbuffer, int)
        except:
            raise ValueError("pixelbuffer must be an integer")

        try:
            assert resampling in RESAMPLING_METHODS
        except:
            raise ValueError("resampling method %s not found." % resampling)

        self.process = input_mapchete
        self.tile_pyramid = self.process.tile_pyramid
        self.tile = tile
        self.input_file = input_mapchete
        self.pixelbuffer = pixelbuffer
        self.resampling = resampling
        self.profile = _read_metadata(self, "NumpyTile")
        self.affine = self.profile["affine"]
        self.nodata = self.profile["nodata"]
        self.indexes = self.profile["count"]
        self.dtype = self.profile["dtype"]
        self.crs = self.tile_pyramid.crs
        self.shape = (self.profile["width"], self.profile["height"])

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        # TODO cleanup
        pass

    def read(self):
        """
        Generates numpy arrays from input process bands.
        - dst_tile: this tile (self.tile)
        - src_tile(s): original MapcheteProcess pyramid tile
        Note: this is a semi-hacky variation as it uses an os.system call to
        generate a temporal mosaic using the gdalbuildvrt command.
        """

        tile = self.process.tile(self.tile)

        if tile.exists():
            return self._np_cache
        else:
            return "empty"
        #
        # else:
        #     empty_array =  ma.masked_array(
        #         ma.zeros(
        #             self.shape,
        #             dtype=self.dtype
        #         ),
        #         mask=True
        #         )
        #     return (
        #         empty_array
        #     )


    def is_empty(self):
        """
        Returns true if all items are masked.
        """
        src_bbox = self.input_file.config.process_area(self.tile.zoom)
        tile_geom = self.tile.bbox(
            pixelbuffer=self.pixelbuffer
        )
        if not tile_geom.intersects(src_bbox):
            return True

        tile = self.process.tile(self.tile)

        if not tile.exists():
            return True
        else:
            return False

        if isinstance(self._np_cache, MaskedArray):
            return self._np_cache.mask.all()
        else:
            return True

    @property
    def _np_cache(self):
        """
        Caches numpy array.
        """
        np_data = read_numpy(self.process.tile(self.tile).path)
        try:
            assert isinstance(np_data, np.ndarray)
        except AssertionError:
            raise IOError("not a valid numpy tile")
        return np_data
