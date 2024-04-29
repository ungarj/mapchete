"""
Main base classes for input and output formats.

When writing a new driver, please inherit from these classes and implement the
respective interfaces.
"""

import logging
import types
import warnings
from itertools import chain
from typing import Any, List, Optional, Tuple

import numpy as np
import numpy.ma as ma
from pydantic import NonNegativeInt
from shapely.geometry import shape

from mapchete.config import get_hash
from mapchete.errors import MapcheteNodataTile, MapcheteProcessOutputError
from mapchete.formats import write_output_metadata

# from mapchete.formats.models import BaseInputParams
from mapchete.formats.protocols import InputDataProtocol, InputTileProtocol
from mapchete.io.raster import (
    create_mosaic,
    extract_from_array,
    prepare_array,
    read_raster_window,
)
from mapchete.io.vector import read_vector_window
from mapchete.path import MPath
from mapchete.processing.tasks import Task
from mapchete.tile import BufferedTile, BufferedTilePyramid
from mapchete.types import CRSLike

logger = logging.getLogger(__name__)


DEFAULT_TILE_PATH_SCHEMA = "{zoom}/{row}/{col}.{extension}"


class InputTile(InputTileProtocol):
    """
    Target Tile representation of input data.

    Parameters
    ----------
    tile : ``Tile``
    kwargs : keyword arguments
        driver specific parameters
    """

    preprocessing_tasks_results: dict
    input_key: str
    tile: BufferedTile

    def __init__(self, tile: BufferedTile, input_key: str, **kwargs):
        """Initialize."""
        self.tile = tile
        self.input_key = input_key
        self.preprocessing_tasks_results = {}

    def set_preprocessing_task_result(self, task_key: str, result: Any = None) -> None:
        """
        Adds a preprocessing task result.
        """
        self.preprocessing_tasks_results[task_key] = result

    def __enter__(self):
        """Required for 'with' statement."""
        return self

    def __exit__(self, t, v, tb):
        """Clean up."""


class InputData(InputDataProtocol):
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

    input_key: str
    pyramid: BufferedTilePyramid
    pixelbuffer: int
    crs: CRSLike
    preprocessing_tasks: dict
    preprocessing_tasks_results: dict
    METADATA = {"driver_name": None, "data_type": None, "mode": "r"}

    def __init__(self, input_params: dict, input_key: Optional[str] = None, **kwargs):
        """Initialize relevant input information."""
        self.input_key = input_key or get_hash(input_params)
        self.pyramid = input_params.get("pyramid")
        self.pixelbuffer = input_params.get("pixelbuffer")
        self.crs = self.pyramid.crs if self.pyramid else None
        # collect preprocessing tasks to be run by the Executor
        self.preprocessing_tasks = {}
        # storage location of all preprocessing tasks
        self.preprocessing_tasks_results = {}
        self.storage_options = input_params.get("abstract", {}).get(
            "storage_options", {}
        )

    def cleanup(self) -> None:
        """Optional cleanup function called when Mapchete exits."""

    def add_preprocessing_task(
        self, func, fargs=None, fkwargs=None, key=None, geometry=None, bounds=None
    ):
        """
        Add longer running preprocessing function to be called right before processing.

        Applied correctly this will speed up process initialization and if multiple tasks
        are required they run in parallel as they are being passed on the Executor.
        """
        fargs = fargs or ()
        if not isinstance(fargs, (tuple, list)):
            fargs = (fargs,)
        fkwargs = fkwargs or {}
        key = f"{func}-{get_hash((func, fargs, fkwargs))}" if key is None else key
        if self.input_key:
            key = f"{self.input_key}:{key}"
        if key in self.preprocessing_tasks:  # pragma: no cover
            raise KeyError(f"preprocessing task with key {key} already exists")
        logger.debug(f"add preprocessing task {key, func}")
        self.preprocessing_tasks[key] = Task(
            id=f"{key}",
            result_key_name=f"preprocessing_task-{key}_result",
            func=func,
            fargs=fargs,
            fkwargs=fkwargs,
            geometry=geometry,
            bounds=bounds,
        )

    def get_preprocessing_task_result(self, task_key):
        """
        Get result of preprocessing task.
        """
        if self.input_key and not task_key.startswith(f"{self.input_key}:"):
            task_key = f"{self.input_key}:{task_key}"
        if task_key not in self.preprocessing_tasks:
            raise KeyError(f"task {task_key} is not a task for current input")
        if task_key not in self.preprocessing_tasks_results:
            raise ValueError(f"task {task_key} has not yet been executed")
        return self.preprocessing_tasks_results[task_key]

    def set_preprocessing_task_result(self, task_key, result):
        """
        Set result of preprocessing task.
        """
        if self.input_key and not task_key.startswith(
            f"{self.input_key}:"
        ):  # pragma: no cover
            task_key = f"{self.input_key}:{task_key}"
        if task_key not in self.preprocessing_tasks:  # pragma: no cover
            raise KeyError(f"task {task_key} is not a task for current input")
        # The following part was commented out because on some rare occasions a
        # mapchete Hub job would fail because of this.
        # if task_key in self.preprocessing_tasks_results:  # pragma: no cover
        #     raise KeyError(f"task {task_key} has already been set")
        self.preprocessing_tasks_results[task_key] = result

    def preprocessing_task_finished(self, task_key):
        """
        Return whether preprocessing task already ran.
        """
        if self.input_key and not task_key.startswith(f"{self.input_key}:"):
            task_key = f"{self.input_key}:{task_key}"
        if task_key not in self.preprocessing_tasks:  # pragma: no cover
            raise KeyError(f"task {task_key} is not a task for current input")
        return task_key in self.preprocessing_tasks_results


class OutputDataBase:
    write_in_parent_process = False
    pixelbuffer: NonNegativeInt
    pyramid: BufferedTilePyramid
    crs: CRSLike

    def __init__(self, output_params: dict, readonly: bool = False, **kwargs):
        """Initialize."""
        self.pixelbuffer = output_params["pixelbuffer"]
        if "type" in output_params:  # pragma: no cover
            warnings.warn(
                DeprecationWarning("'type' is deprecated and should be 'grid'")
            )
            if "grid" not in output_params:
                output_params["grid"] = output_params.pop("type")
        self.pyramid = BufferedTilePyramid(
            grid=output_params["grid"],
            metatiling=output_params["metatiling"],
            pixelbuffer=output_params["pixelbuffer"],
        )
        self.crs = self.pyramid.crs
        self.storage_options = output_params.get("storage_options")

    # TODO: move to path based output
    def get_path(self, tile: BufferedTile) -> MPath:
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
        return self.path / self.tile_path_schema.format(
            zoom=str(tile.zoom),
            row=str(tile.row),
            col=str(tile.col),
            extension=self.file_extension.lstrip("."),
        )

    # TODO: split up into vector and raster based output (mixin classes)
    def extract_subset(
        self, input_data_tiles: List[Tuple[BufferedTile, Any]], out_tile: BufferedTile
    ) -> Any:
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
                array=prepare_array(
                    mosaic.data,
                    nodata=self.output_params["nodata"],
                    dtype=self.output_params["dtype"],
                ),
                in_affine=mosaic.affine,
                out_tile=out_tile,
            )
        elif self.METADATA["data_type"] == "vector":
            return [
                feature
                for feature in list(
                    chain.from_iterable([features for _, features in input_data_tiles])
                )
                if shape(feature["geometry"]).intersects(out_tile.bbox)
            ]

    def prepare(self, **kwargs):
        pass


class OutputSTACMixin:
    """Adds STAC related features."""

    path: MPath
    output_params: dict

    @property
    def stac_path(self) -> MPath:
        """Return path to STAC JSON file."""
        return self.path / f"{self.stac_item_id}.json"

    @property
    def stac_item_id(self) -> str:
        """
        Return STAC item ID according to configuration.

        Defaults to path basename.
        """
        return self.output_params.get("stac", {}).get("id") or self.path.stem

    @property
    def stac_item_metadata(self):
        """Custom STAC metadata."""
        return self.output_params.get("stac", {})

    @property
    def stac_asset_type(self):  # pragma: no cover
        """Asset MIME type."""
        raise ValueError("no MIME type set for this output")


class OutputDataReader(OutputDataBase):
    def read(self, output_tile):  # pragma: no cover
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

    def empty(self, process_tile):  # pragma: no cover
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

    def open(self, tile, process):  # pragma: no cover
        """
        Open process output as input for other process.

        Parameters
        ----------
        tile : ``Tile``
        process : ``MapcheteProcess``
        """
        raise NotImplementedError

    def for_web(self, data):  # pragma: no cover
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


class OutputDataWriter(OutputDataReader):
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

    METADATA = {"driver_name": None, "data_type": None, "mode": "w"}
    use_stac = False

    def write(self, process_tile, data):  # pragma: no cover
        """
        Write data from one or more process tiles.

        Parameters
        ----------
        process_tile : ``BufferedTile``
            must be member of process ``TilePyramid``
        """
        raise NotImplementedError

    def prepare_path(self, tile):
        """
        Create directory and subdirectory if necessary.

        Parameters
        ----------
        tile : ``BufferedTile``
            must be member of output ``TilePyramid``
        """
        self.get_path(tile).parent.makedirs()

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
            return is_numpy_or_masked_array(
                process_data
            ) or is_numpy_or_masked_array_with_tags(process_data)
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
                return prepare_array(
                    process_data,
                    masked=True,
                    nodata=self.output_params["nodata"],
                    dtype=self.profile()["dtype"],
                )
            elif is_numpy_or_masked_array_with_tags(process_data):
                data, tags = process_data
                return self.output_cleaned(data), tags
        elif self.METADATA["data_type"] == "vector":
            return list(process_data)

    def streamline_output(self, process_data):
        if isinstance(process_data, str) and process_data == "empty":
            raise MapcheteNodataTile
        elif process_data is None:  # pragma: no cover
            raise MapcheteProcessOutputError("process output is empty")
        elif self.output_is_valid(process_data):
            return self.output_cleaned(process_data)
        else:
            raise MapcheteProcessOutputError(
                "invalid output type: %s" % type(process_data)
            )

    def close(self, exc_type=None, exc_value=None, exc_traceback=None):
        """Gets called if process is closed."""


class TileDirectoryOutputReader(OutputDataReader, OutputSTACMixin):
    tile_path_schema: str = DEFAULT_TILE_PATH_SCHEMA

    def __init__(self, output_params, readonly=False):
        """Initialize."""
        super().__init__(output_params, readonly=readonly)
        self.tile_path_schema = output_params.get(
            "tile_path_schema", DEFAULT_TILE_PATH_SCHEMA
        )
        if not readonly:
            write_output_metadata(
                {k: v for k, v in output_params.items() if k not in ["stac"]}
            )

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
        if process_tile and output_tile:  # pragma: no cover
            raise ValueError("just one of 'process_tile' and 'output_tile' allowed")
        if process_tile:
            for tile in self.pyramid.intersecting(process_tile):
                if self.get_path(tile).exists():
                    return True
            else:
                return False
        if output_tile:
            return self.get_path(output_tile).exists()

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
        **kwargs,
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
            **{k: v for k, v in kwargs.items() if k != "data_type"},
        )


class TileDirectoryOutputWriter(OutputDataWriter, TileDirectoryOutputReader):
    pass


class SingleFileOutputReader(OutputDataReader, OutputSTACMixin):
    def __init__(self, output_params, readonly=False):
        """Initialize."""
        super().__init__(output_params, readonly=readonly)

    def tiles_exist(self, process_tile=None, output_tile=None):  # pragma: no cover
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


class SingleFileOutputWriter(OutputDataWriter, SingleFileOutputReader):
    pass


def is_numpy_or_masked_array(data):
    return isinstance(data, (np.ndarray, ma.MaskedArray))


def is_numpy_or_masked_array_with_tags(data):
    return (
        isinstance(data, tuple)
        and len(data) == 2
        and is_numpy_or_masked_array(data[0])
        and isinstance(data[1], dict)
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
    **kwargs,
):
    logger.debug("reading data from CRS %s to CRS %s", td_crs, out_tile.tp.crs)
    if data_type == "vector":
        if tiles_paths:
            return read_vector_window(
                [path for _, path in tiles_paths],
                out_tile,
                validity_check=validity_check,
                skip_missing_files=True,
            )
        else:  # pragma: no cover
            return []
    elif data_type == "raster":
        if tiles_paths:
            return read_raster_window(
                [path for _, path in tiles_paths],
                out_tile,
                indexes=indexes or list(range(1, profile["count"] + 1)),
                resampling=resampling,
                src_nodata=profile["nodata"],
                dst_nodata=dst_nodata,
                gdal_opts=gdal_opts,
                skip_missing_files=True,
                dst_dtype=profile["dtype"],
            )
        else:
            bands = len(indexes) if indexes else profile["count"]
            return ma.masked_array(
                data=np.full(
                    (bands, out_tile.height, out_tile.width),
                    profile["nodata"],
                    dtype=profile["dtype"],
                ),
                mask=True,
            )
    else:  # pragma: no cover
        raise NotImplementedError(f"driver data_type {data_type} not supported")
