from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Union

from rasterio.crs import CRS
from rasterio.vrt import WarpedVRT

from mapchete.bounds import Bounds
from mapchete.config.base import MapcheteConfig
from mapchete.config.parse import raw_conf, raw_conf_output_pyramid
from mapchete.enums import DataType, InputType, OutputType
from mapchete.formats import (
    available_input_formats,
    available_output_formats,
    driver_from_file,
)
from mapchete.io import fiona_open, rasterio_open
from mapchete.path import MPath
from mapchete.tile import BufferedTilePyramid
from mapchete.types import CRSLike, MPathLike
from mapchete.zoom_levels import ZoomLevels

logger = logging.getLogger(__name__)
OUTPUT_FORMATS = available_output_formats()


@dataclass
class InputInfo:
    output_params: dict
    crs: CRSLike
    data_type: DataType
    input_type: InputType
    bounds: Optional[Bounds] = None
    zoom_levels: Optional[ZoomLevels] = None
    output_pyramid: Optional[BufferedTilePyramid] = None
    pixel_size: Optional[int] = None

    @staticmethod
    def from_inp(inp: Union[MPathLike, dict, MapcheteConfig]) -> InputInfo:
        try:
            path = MPath.from_inp(inp)

        except Exception:
            if isinstance(inp, dict):
                return InputInfo.from_config_dict(inp)
            elif isinstance(inp, MapcheteConfig):  # pragma: no cover
                return InputInfo.from_mapchete_config(inp)

            raise TypeError(f"cannot create InputInfo from {inp}")  # pragma: no cover

        return InputInfo.from_path(path)

    @staticmethod
    def from_config_dict(conf: dict) -> InputInfo:
        output_params = conf["output"]
        output_pyramid = raw_conf_output_pyramid(conf)
        return InputInfo(
            input_type=InputType.mapchete,
            output_params=output_params,
            output_pyramid=output_pyramid,
            crs=output_pyramid.crs,
            zoom_levels=ZoomLevels.from_inp(conf["zoom_levels"]),
            data_type=DataType[OUTPUT_FORMATS[output_params["format"]]["data_type"]],
            bounds=Bounds.from_inp(conf.get("bounds")) if conf.get("bounds") else None,
        )

    @staticmethod
    def from_mapchete_config(
        mapchete_config: MapcheteConfig,
    ) -> InputInfo:  # pragma: no cover
        return InputInfo(
            input_type=InputType.mapchete,
            output_params=mapchete_config.output.params,
            output_pyramid=mapchete_config.output_pyramid,
            crs=mapchete_config.output_pyramid.crs,
            zoom_levels=mapchete_config.zoom_levels,
            data_type=DataType[
                OUTPUT_FORMATS[mapchete_config.output.params["format"]]["data_type"]
            ],
            bounds=mapchete_config.bounds,
        )

    @staticmethod
    def from_path(path: MPath) -> InputInfo:
        # assuming single file if path has a file extension
        if path.suffix:
            logger.debug("assuming single file")
            driver = driver_from_file(path)

            # single file input can be a mapchete file or a rasterio/fiona file
            if driver == "Mapchete":
                logger.debug("input is mapchete file")
                return InputInfo.from_mapchete_file(path)

            elif driver == "raster_file":
                # this should be readable by rasterio
                logger.debug("input is raster_file")
                return InputInfo.from_rasterio_file(path)

            elif driver == "vector_file":
                # this should be readable by Fiona
                return InputInfo.from_fiona_file(path)

            else:  # pragma: no cover
                raise NotImplementedError(f"driver {driver} is not supported")

        # assuming tile directory
        else:
            logger.debug("input is maybe a tile directory")
            return InputInfo.from_tile_directory(path)

    @staticmethod
    def from_mapchete_file(path: MPath) -> InputInfo:
        return InputInfo.from_config_dict(raw_conf(path))

    @staticmethod
    def from_rasterio_file(path: MPath) -> InputInfo:
        with rasterio_open(path) as src:
            if src.transform.is_identity:
                if src.gcps[1] is not None:
                    with WarpedVRT(src) as dst:
                        bounds = dst.bounds
                        crs = src.gcps[1]
                elif src.rpcs:  # pragma: no cover
                    with WarpedVRT(src) as dst:
                        bounds = dst.bounds
                        crs = CRS.from_string("EPSG:4326")
                else:  # pragma: no cover
                    raise TypeError("cannot determine georeference")
            else:
                crs = src.crs
                bounds = src.bounds
            return InputInfo(
                input_type=InputType.single_file,
                output_params=dict(
                    bands=src.meta["count"],
                    dtype=src.meta["dtype"],
                    format=src.driver
                    if src.driver in available_input_formats()
                    else None,
                ),
                crs=crs,
                pixel_size=src.transform[0],
                data_type=DataType.raster,
                bounds=Bounds(*bounds),
            )

    @staticmethod
    def from_fiona_file(path: MPath) -> InputInfo:
        with fiona_open(path) as src:
            return InputInfo(
                input_type=InputType.single_file,
                output_params=dict(
                    schema=src.schema,
                    format=src.driver
                    if src.driver in available_input_formats()
                    else None,
                ),
                crs=src.crs,
                data_type=DataType.vector,
                bounds=Bounds(*src.bounds) if len(src) else None,
            )

    @staticmethod
    def from_tile_directory(path) -> InputInfo:
        conf = (path / "metadata.json").read_json()
        pyramid = BufferedTilePyramid.from_dict(conf["pyramid"])
        return InputInfo(
            input_type=InputType.tile_directory,
            output_params=conf["driver"],
            output_pyramid=pyramid,
            crs=pyramid.crs,
            data_type=DataType[OUTPUT_FORMATS[conf["driver"]["format"]]["data_type"]],
        )


@dataclass
class OutputInfo:
    type: OutputType
    driver: Optional[str]

    @staticmethod
    def from_path(path: MPath) -> OutputInfo:
        if path.suffix:
            if path.suffix == ".tif":
                return OutputInfo(type=OutputType.single_file, driver="GTiff")
            else:
                raise ValueError("currently only single file GeoTIFFs are allowed")

        if not path.suffix:
            return OutputInfo(type=OutputType.tile_directory, driver=None)
