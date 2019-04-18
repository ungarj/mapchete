"""
Main base classes for input and output formats.

When writing a new driver, please inherit from these classes and implement the
respective interfaces.
"""

from itertools import chain
import logging
import numpy as np
import numpy.ma as ma
import os
from shapely.geometry import shape
from tilematrix import TilePyramid
import types
import warnings

from mapchete.formats import write_output_metadata
from mapchete.io import makedirs, path_exists
from mapchete.io.raster import (
    create_mosaic, extract_from_array, prepare_array, read_raster_window
)
from mapchete.io.vector import read_vector_window
from mapchete.tile import BufferedTilePyramid

logger = logging.getLogger(__name__)


class InputData(object):
    """
    Template class handling geographic input data.

    Parameters
    ----------
    input_params : dictionary
        driver specific parameters

    Attributes
    ----------
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    """

    METADATA = {
        "driver_name": None,
        "data_type": None,
        "mode": "r"
    }

    def __init__(self, input_params, **kwargs):
        """Initialize relevant input information."""
        self.pyramid = input_params["pyramid"]
        self.pixelbuffer = input_params["pixelbuffer"]
        self.crs = self.pyramid.crs

    def open(self, tile, **kwargs):
        """
        Return InputTile object.

        Parameters
        ----------
        tile : ``Tile``

        Returns
        -------
        input tile : ``InputTile``
            tile view of input data
        """
        raise NotImplementedError

    def bbox(self, out_crs=None):
        """
        Return data bounding box.

        Parameters
        ----------
        out_crs : ``rasterio.crs.CRS``
            rasterio CRS object (default: CRS of process pyramid)

        Returns
        -------
        bounding box : geometry
            Shapely geometry object
        """
        raise NotImplementedError

    def exists(self):
        """
        Check if data or file even exists.

        Returns
        -------
        file exists : bool
        """
        raise NotImplementedError

    def cleanup(self):
        """Optional cleanup function called when Mapchete exits."""
        pass


class InputTile(object):
    """
    Target Tile representation of input data.

    Parameters
    ----------
    tile : ``Tile``
    kwargs : keyword arguments
        driver specific parameters
    """

    def __init__(self, tile, **kwargs):
        """Initialize."""

    def read(self, **kwargs):
        """
        Read reprojected & resampled input data.

        Returns
        -------
        data : array or list
            NumPy array for raster data or feature list for vector data
        """
        raise NotImplementedError

    def is_empty(self):
        """
        Check if there is data within this tile.

        Returns
        -------
        is empty : bool
        """
        raise NotImplementedError

    def __enter__(self):
        """Required for 'with' statement."""
        return self

    def __exit__(self, t, v, tb):
        """Clean up."""
        pass


class OutputData(object):
    """
    Template class handling process output data.

    Parameters
    ----------
    output_params : dictionary
        output parameters from Mapchete file

    Attributes
    ----------
    pixelbuffer : integer
        buffer around output tiles
    pyramid : ``tilematrix.TilePyramid``
        output ``TilePyramid``
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    """

    METADATA = {
        "driver_name": None,
        "data_type": None,
        "mode": "w"
    }

    def __init__(self, output_params, readonly=False):
        """Initialize."""
        logger.debug("base.Output.__new__()")
        self.pixelbuffer = output_params["pixelbuffer"]
        if "type" in output_params:
            warnings.warn(DeprecationWarning("'type' is deprecated and should be 'grid'"))
            if "grid" not in output_params:
                output_params["grid"] = output_params.pop("type")
        self.pyramid = BufferedTilePyramid(
            grid=output_params["grid"],
            metatiling=output_params["metatiling"]
        )
        self.crs = self.pyramid.crs
        self._bucket = None

    def read(self, output_tile):
        """
        Read existing process output.

        Parameters
        ----------
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        process output : array or list
        """
        raise NotImplementedError()

    def write(self, process_tile, data):
        """
        Write data from one or more process tiles.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        raise NotImplementedError()

    def is_valid_with_config(self, config):
        """
        Check if output format is valid with other process parameters.

        Parameters
        ----------
        config : dictionary
            output configuration parameters

        Returns
        -------
        is_valid : bool
        """
        raise NotImplementedError()

    def get_path(self, tile):
        """
        Determine target file path.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        path : string
        """
        return os.path.join(*[
            self.path,
            str(tile.zoom),
            str(tile.row),
            str(tile.col) + self.file_extension
        ])

    def prepare_path(self, tile):
        """
        Create directory and subdirectory if necessary.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``
        """
        makedirs(os.path.dirname(self.get_path(tile)))

    def for_web(self, data):
        """
        Convert data to web output (raster only).

        Parameters
        ----------
        data : array

        Returns
        -------
        web data : array
        """
        raise NotImplementedError()

    def empty(self, process_tile):
        """
        Return empty data.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``

        Returns
        -------
        empty data : array or list
            empty array with correct data type for raster data or empty list
            for vector data
        """
        raise NotImplementedError()

    def output_is_valid(self, process_data):
        """
        Check whether process output is allowed with output driver.

        Parameters
        ----------
        process_data : raw process output

        Returns
        -------
        True or False
        """
        if self.METADATA["data_type"] == "raster":
            return (
                is_numpy_or_masked_array(process_data) or
                is_numpy_or_masked_array_with_tags(process_data)
            )
        elif self.METADATA["data_type"] == "vector":
            return is_feature_list(process_data)

    def output_cleaned(self, process_data):
        """
        Return verified and cleaned output.

        Parameters
        ----------
        process_data : raw process output

        Returns
        -------
        NumPy array or list of features.
        """
        if self.METADATA["data_type"] == "raster":
            if is_numpy_or_masked_array(process_data):
                return process_data
            elif is_numpy_or_masked_array_with_tags(process_data):
                data, tags = process_data
                return self.output_cleaned(data), tags
        elif self.METADATA["data_type"] == "vector":
            return list(process_data)

    def extract_subset(self, input_data_tiles=None, out_tile=None):
        """
        Extract subset from multiple tiles.

        input_data_tiles : list of (``Tile``, process data) tuples
        out_tile : ``Tile``

        Returns
        -------
        NumPy array or list of features.
        """
        if self.METADATA["data_type"] == "raster":
            mosaic = create_mosaic(input_data_tiles)
            return extract_from_array(
                in_raster=prepare_array(
                    mosaic.data,
                    nodata=self.nodata,
                    dtype=self.output_params["dtype"]
                ),
                in_affine=mosaic.affine,
                out_tile=out_tile
            )
        elif self.METADATA["data_type"] == "vector":
            return [
                feature for feature in list(
                    chain.from_iterable([features for _, features in input_data_tiles])
                )
                if shape(feature["geometry"]).intersects(out_tile.bbox)
            ]

    def open(self, tile, process):
        """
        Open process output as input for other process.

        Parameters
        ----------
        tile : ``Tile``
        process : ``MapcheteProcess``
        """
        raise NotImplementedError()


class TileDirectoryOutput(OutputData):

    def __init__(self, output_params, readonly=False):
        """Initialize."""
        super(OutputData, self).__init__(output_params, readonly=readonly)
        if not readonly:
            write_output_metadata(output_params)

    def tiles_exist(self, process_tile=None, output_tile=None):
        """
        Check whether output tiles of a tile (either process or output) exists.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        exists : bool
        """
        if process_tile and output_tile:
            raise ValueError("just one of 'process_tile' and 'output_tile' allowed")
        if process_tile:
            return any(
                path_exists(self.get_path(tile))
                for tile in self.pyramid.intersecting(process_tile)
            )
        if output_tile:
            return path_exists(self.get_path(output_tile))


class SingleFileOutput(OutputData):

    def __init__(self, output_params, readonly=False):
        """Initialize."""
        super(OutputData, self).__init__(output_params, readonly=readonly)

    def tiles_exist(self, process_tile=None, output_tile=None):
        """
        Check whether output tiles of a tile (either process or output) exists.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        output_tile : ``BufferedTile``
            must be member of output ``TilePyramid``

        Returns
        -------
        exists : bool
        """
        # TODO
        raise NotImplementedError

    def _read_as_tiledir(
        self,
        out_tile=None,
        td_crs=None,
        tiles_paths=None,
        profile=None,
        validity_check=False,
        indexes=None,
        resampling=None,
        dst_nodata=None,
        gdal_opts=None,
        **kwargs
    ):
        """
        Read reprojected & resampled input data.

        Parameters
        ----------
        validity_check : bool
            vector file: also run checks if reprojected geometry is valid,
            otherwise throw RuntimeError (default: True)

        indexes : list or int
            raster file: a list of band numbers; None will read all.
        dst_nodata : int or float, optional
            raster file: if not set, the nodata value from the source dataset
            will be used
        gdal_opts : dict
            raster file: GDAL options passed on to rasterio.Env()

        Returns
        -------
        data : list for vector files or numpy array for raster files
        """
        return _read_as_tiledir(
            data_type=self.METADATA["data_type"],
            out_tile=out_tile,
            td_crs=td_crs,
            tiles_paths=tiles_paths,
            profile=profile,
            validity_check=validity_check,
            indexes=indexes,
            resampling=resampling,
            dst_nodata=dst_nodata,
            gdal_opts=gdal_opts,
            **{k: v for k, v in kwargs.items() if k != "data_type"}
        )


def is_numpy_or_masked_array(data):
    return isinstance(data, (np.ndarray, ma.MaskedArray))


def is_numpy_or_masked_array_with_tags(data):
    return (
        isinstance(data, tuple) and
        len(data) == 2 and
        is_numpy_or_masked_array(data[0]) and
        isinstance(data[1], dict)
    )


def is_feature_list(data):
    return isinstance(data, (list, types.GeneratorType))


def _read_as_tiledir(
    data_type=None,
    out_tile=None,
    td_crs=None,
    tiles_paths=None,
    profile=None,
    validity_check=False,
    indexes=None,
    resampling=None,
    dst_nodata=None,
    gdal_opts=None,
    **kwargs
):
    logger.debug("reading data from CRS %s to CRS %s", td_crs, out_tile.tp.crs)
    if data_type == "vector":
        if tiles_paths:
            return read_vector_window(
                [path for _, path in tiles_paths],
                out_tile,
                validity_check=validity_check
            )
        else:
            return []
    elif data_type == "raster":
        if tiles_paths:
            return read_raster_window(
                [path for _, path in tiles_paths],
                out_tile,
                indexes=indexes,
                resampling=resampling,
                src_nodata=profile["nodata"],
                dst_nodata=dst_nodata,
                gdal_opts=gdal_opts
            )
        else:
            bands = len(indexes) if indexes else profile["count"]
            return ma.masked_array(
                data=np.full(
                    (bands, out_tile.height, out_tile.width),
                    profile["nodata"],
                    dtype=profile["dtype"]
                ),
                mask=True
            )
