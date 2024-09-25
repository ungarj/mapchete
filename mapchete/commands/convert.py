import logging
import os
from contextlib import AbstractContextManager
from multiprocessing import cpu_count
from pprint import pformat
from typing import List, Optional, Tuple, Type, Union

import tilematrix
from rasterio.crs import CRS
from rasterio.enums import Resampling
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry

from mapchete.commands.execute import execute
from mapchete.commands.observer import ObserverProtocol, Observers
from mapchete.commands.parser import InputInfo, OutputInfo
from mapchete.config import DaskSettings
from mapchete.enums import Concurrency, DataType, ProcessingMode
from mapchete.errors import JobCancelledError
from mapchete.executor import Executor
from mapchete.formats import available_output_formats
from mapchete.io import MPath, fiona_open, get_best_zoom_level
from mapchete.geometry import reproject_geometry
from mapchete.tile import BufferedTilePyramid
from mapchete.types import MPathLike, ResamplingLike, to_resampling

logger = logging.getLogger(__name__)
OUTPUT_FORMATS = available_output_formats()


def convert(
    input_path: MPathLike,
    output_path: MPathLike,
    zoom: Optional[Union[int, List[int]]] = None,
    area: Optional[Union[BaseGeometry, str, dict]] = None,
    area_crs: Optional[Union[CRS, str]] = None,
    bounds: Optional[Tuple[float, float, float, float]] = None,
    bounds_crs: Optional[Union[CRS, str]] = None,
    point: Optional[Tuple[float, float]] = None,
    point_crs: Optional[Tuple[float, float]] = None,
    overwrite: bool = False,
    concurrency: Concurrency = Concurrency.processes,
    dask_settings: DaskSettings = DaskSettings(),
    workers: Optional[int] = None,
    clip_geometry: Optional[str] = None,
    bidx: Optional[Union[List[int], int]] = None,
    output_pyramid: Optional[Union[str, dict, MPathLike]] = None,
    output_metatiling: Optional[int] = None,
    output_format: Optional[str] = None,
    output_dtype: Optional[str] = None,
    output_geometry_type: Optional[str] = None,
    creation_options: Optional[dict] = None,
    scale_ratio: Optional[float] = None,
    scale_offset: Optional[float] = None,
    resampling_method: ResamplingLike = Resampling.nearest,
    overviews: bool = False,
    overviews_resampling_method: Resampling = Resampling.cubic_spline,
    cog: bool = False,
    src_fs_opts: Optional[dict] = None,
    dst_fs_opts: Optional[dict] = None,
    executor_getter: AbstractContextManager = Executor,
    observers: Optional[List[ObserverProtocol]] = None,
    retry_on_exception: Tuple[Type[Exception], Type[Exception]] = Exception,
    cancel_on_exception: Type[Exception] = JobCancelledError,
    retries: int = 0,
) -> None:
    """
    Convert mapchete outputs or other geodata.

    This is a wrapper around the mapchete.processes.convert process which helps generating tiled
    outputs for raster and vector data or single COGs from TileDirectory raster inputs.

    It also supports clipping of the input by a vector dataset.

    If only a subset of a TileDirectory is desired, please see the mapchete.commands.cp command.
    """
    resampling_method = to_resampling(resampling_method)
    overviews_resampling_method = to_resampling(overviews_resampling_method)
    all_observers = Observers(observers)
    workers = workers or cpu_count()
    creation_options = creation_options or {}
    bidx = [bidx] if isinstance(bidx, int) else bidx
    input_path = MPath.from_inp(input_path, storage_options=src_fs_opts)
    output_path = MPath.from_inp(output_path, storage_options=dst_fs_opts)
    try:
        input_info = InputInfo.from_path(input_path)
        logger.debug("input params: %s", input_info)
        output_info = OutputInfo.from_path(output_path)
        logger.debug("output params: %s", output_info)
    except Exception as e:
        raise ValueError(e)

        # try to read output grid definition from a file
    if not (
        isinstance(output_pyramid, str)
        and output_pyramid in tilematrix._conf.PYRAMID_PARAMS.keys()
    ):
        try:
            output_pyramid = MPath.from_inp(output_pyramid).read_json()  # type: ignore
        except Exception:  # pragma: no cover
            pass

    # collect mapchete configuration
    mapchete_config = dict(
        process="mapchete.processes.convert",
        input=dict(inp=input_path, clip=clip_geometry),
        pyramid=(
            dict(
                grid=output_pyramid,
                metatiling=(
                    output_metatiling
                    or (
                        input_info.output_pyramid.metatiling
                        if input_info.output_pyramid
                        else 1
                    )
                ),
                pixelbuffer=(
                    input_info.output_pyramid.pixelbuffer
                    if input_info.output_pyramid
                    else 0
                ),
            )
            if output_pyramid
            else (
                input_info.output_pyramid.to_dict()
                if input_info.output_pyramid
                else None
            )
        ),
        output=dict(
            {
                k: v
                for k, v in input_info.output_params.items()
                if k not in ["delimiters", "bounds", "mode"]
            },
            path=output_path,
            format=(
                output_format
                or output_info.driver
                or input_info.output_params["format"]
            ),
            dtype=output_dtype or input_info.output_params.get("dtype"),
            **creation_options,
            **(
                dict(overviews=True, overviews_resampling=overviews_resampling_method)
                if overviews
                else dict()
            ),
        ),
        config_dir=os.getcwd(),
        zoom_levels=zoom or input_info.zoom_levels,
        process_parameters=dict(
            scale_ratio=scale_ratio,
            scale_offset=scale_offset,
            resampling=resampling_method,
            band_indexes=bidx,
        ),
    )

    # assert all required information is there
    if mapchete_config["output"]["format"] is None:
        # this happens if input file is e.g. JPEG2000 and output is a tile directory
        raise ValueError("Output format required.")
    if mapchete_config["output"]["format"] == "GTiff":
        mapchete_config["output"].update(cog=cog)
    output_type = DataType[
        OUTPUT_FORMATS[mapchete_config["output"]["format"]]["data_type"]
    ]
    if bidx is not None:
        mapchete_config["output"].update(bands=len(bidx))
    if mapchete_config["pyramid"] is None:
        raise ValueError("Output pyramid required.")
    elif mapchete_config["zoom_levels"] is None:
        try:
            mapchete_config.update(
                zoom_levels=dict(
                    min=0,
                    max=get_best_zoom_level(
                        input_path, mapchete_config["pyramid"]["grid"]
                    ),
                )
            )
        except Exception as exc:
            raise ValueError("Zoom levels required.") from exc
    elif input_info.data_type != output_type:
        raise ValueError(
            f"Output format type ({output_type}) is incompatible with input format ({input_info.data_type})."
        )
    if output_metatiling:
        mapchete_config["pyramid"].update(metatiling=output_metatiling)
        mapchete_config["output"].update(metatiling=output_metatiling)
    if (
        input_info.output_params.get("schema") and output_geometry_type
    ):  # pragma: no cover
        mapchete_config["output"]["schema"].update(geometry=output_geometry_type)

    # determine process bounds
    out_pyramid = BufferedTilePyramid.from_dict(mapchete_config["pyramid"])
    inp_bounds = (
        bounds
        or reproject_geometry(
            input_info.bounds.geometry,
            src_crs=input_info.crs,
            dst_crs=out_pyramid.crs,
        ).bounds
        if input_info.bounds
        else out_pyramid.bounds
    )
    # if clip-geometry is available, intersect determined bounds with clip bounds
    if clip_geometry:
        clip_intersection = _clip_bbox(
            clip_geometry, dst_crs=out_pyramid.crs
        ).intersection(box(*inp_bounds))
        if clip_intersection.is_empty:
            all_observers.notify(
                message="Process area is empty: clip bounds don't intersect with input bounds."
            )
            return
    # add process bounds and output type
    mapchete_config.update(
        bounds=(clip_intersection.bounds if clip_geometry else inp_bounds),
        bounds_crs=bounds_crs,
    )
    mapchete_config["process_parameters"].update(
        clip_to_output_dtype=mapchete_config["output"].get("dtype", None),
    )
    logger.debug(f"temporary config generated: {pformat(mapchete_config)}")

    return execute(
        mapchete_config=mapchete_config,
        mode=ProcessingMode.OVERWRITE if overwrite else ProcessingMode.CONTINUE,
        zoom=zoom,
        point=point,
        point_crs=point_crs,
        bounds=bounds,
        bounds_crs=bounds_crs,
        area=area,
        area_crs=area_crs,
        concurrency=concurrency,
        dask_settings=dask_settings,
        workers=workers,
        executor_getter=executor_getter,
        observers=observers,
        retry_on_exception=retry_on_exception,
        cancel_on_exception=cancel_on_exception,
        retries=retries,
    )


def _clip_bbox(clip_geometry, dst_crs=None):
    with fiona_open(clip_geometry) as src:
        return reproject_geometry(box(*src.bounds), src_crs=src.crs, dst_crs=dst_crs)
