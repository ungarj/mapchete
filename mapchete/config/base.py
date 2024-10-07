from __future__ import annotations

import hashlib
import logging
import warnings
from collections import OrderedDict
from copy import deepcopy
from functools import cached_property
from typing import Any, Iterator, Optional, Tuple, Union

import oyaml as yaml
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from mapchete.bounds import Bounds
from mapchete.config.models import ProcessConfig
from mapchete.config.parse import (
    get_zoom_levels,
    guess_geometry,
    parse_config,
    raw_conf_at_zoom,
    zoom_parameters,
)
from mapchete.config.process_func import ProcessFunc
from mapchete.enums import ProcessingMode
from mapchete.errors import MapcheteConfigError, MapcheteDriverError
from mapchete.formats import (
    available_output_formats,
    load_input_reader,
    load_output_reader,
    load_output_writer,
)
from mapchete.io import MPath, absolute_path
from mapchete.geometry import reproject_geometry
from mapchete.tile import BufferedTile, BufferedTilePyramid, snap_geometry_to_tiles
from mapchete.timer import Timer
from mapchete.types import BoundsLike, MPathLike
from mapchete.validate import (
    validate_bounds,
    validate_bufferedtilepyramid,
    validate_values,
    validate_zooms,
)
from mapchete.zoom_levels import ZoomLevels

logger = logging.getLogger(__name__)

__all__ = ["validate_bounds", "validate_zooms", "validate_values"]

# TODO remove these
# parameters for output configuration
_OUTPUT_PARAMETERS = [
    "format",
    "path",
    "grid",
    "pixelbuffer",
    "metatiling",
    "delimiters",
    "mode",
    "stac",
]


class MapcheteConfig(object):
    """
    Process configuration.

    MapcheteConfig reads and parses a Mapchete configuration, verifies the
    parameters, creates the necessary metadata required and provides the
    configuration snapshot for every zoom level.

    Parameters
    ----------
    input_config : string or dictionary
        a Mapchete configuration file or a configuration dictionary
    zoom : list or integer
        zoom level or a pair of minimum and maximum zoom level the process is
        initialized with
    bounds : tuple
        left, bottom, right, top boundaries the process is initalized with
    mode : string
        * ``memory``: Generate process output on demand without reading
          pre-existing data or writing new data.
        * ``readonly``: Just read data without processing new data.
        * ``continue``: (default) Don't overwrite existing output.
        * ``overwrite``: Overwrite existing output.

    """

    parsed_config: ProcessConfig = None
    mode: ProcessingMode = ProcessingMode.CONTINUE
    preprocessing_tasks_finished: bool = False
    config_dir: MPath = None
    process: Union[ProcessFunc, None] = None
    process_pyramid: BufferedTilePyramid
    output_pyramid: BufferedTilePyramid
    baselevels: Union[dict, None]
    area: BaseGeometry
    bounds: Bounds
    zoom_levels: ZoomLevels
    init_area: BaseGeometry
    init_bounds: Bounds
    init_zoom_levels: ZoomLevels
    effective_area: BaseGeometry
    effective_bounds: Bounds
    input: OrderedDict
    output: "OutputDataWriter"  # noqa: F821
    output_reader: "OutputDataReader"  # noqa: F821

    def __init__(
        self,
        input_config: Union[dict, MPathLike],
        zoom=None,
        area=None,
        area_crs=None,
        bounds=None,
        bounds_crs=None,
        mode="continue",
        stric_parsing=False,
        **kwargs,
    ):
        """Initialize configuration."""
        # get dictionary representation of input_config and
        # (1) map deprecated params to new structure
        logger.debug(f"parsing {input_config}")
        try:
            self.parsed_config = parse_config(input_config, strict=stric_parsing)
            self.parsed_config.model_dump()
        except Exception as exc:
            raise MapcheteConfigError(exc)
        self._init_zoom_levels = zoom
        self._init_bounds = bounds
        self._init_bounds_crs = bounds_crs
        self._init_area = area
        self._init_area_crs = area_crs
        self._cache_area_at_zoom = {}
        self._cache_full_process_area = None

        try:
            self.mode = ProcessingMode(mode)
        except Exception as exc:
            raise MapcheteConfigError from exc
        self.preprocessing_tasks_finished = False

        # (2) check user process
        self.config_dir = self.parsed_config.config_dir
        if self.mode != ProcessingMode.READONLY:
            if self.parsed_config.process is None:
                raise MapcheteConfigError(
                    f"process must be provided on {self.mode} mode"
                )
            logger.debug("validating process code")
            self.process = ProcessFunc(
                self.parsed_config.process, config_dir=self.config_dir
            )

        # (3) set process and output pyramids
        logger.debug("initializing pyramids")
        try:
            process_metatiling = self.parsed_config.pyramid.metatiling
            # output metatiling defaults to process metatiling if not set
            # explicitly
            output_metatiling = self.parsed_config.output.get(
                "metatiling", process_metatiling
            )
            # we cannot properly handle output tiles which are bigger than
            # process tiles
            if output_metatiling > process_metatiling:
                raise ValueError(
                    "output metatiles must be smaller than process metatiles"
                )
            # these two BufferedTilePyramid instances will help us with all
            # the tile geometries etc.
            self.process_pyramid = BufferedTilePyramid(
                **dict(self.parsed_config.pyramid)
            )
            self.output_pyramid = BufferedTilePyramid(
                self.parsed_config.pyramid.grid,
                metatiling=output_metatiling,
                pixelbuffer=self.parsed_config.output.get("pixelbuffer", 0),
            )
        except Exception as e:
            logger.exception(e)
            raise MapcheteConfigError(e)

        # (4) set approach how to handle inputs
        # don't inititalize inputs on readonly mode or if only overviews are going to be
        # built
        self._init_inputs = (
            False
            if (
                self.mode == "readonly"
                or (
                    not len(
                        set(self.baselevels["zooms"]).intersection(
                            set(self.init_zoom_levels)
                        )
                    )
                    if self.baselevels
                    else False
                )
            )
            else True
        )

        # (5) prepare process parameters per zoom level without initializing
        # input and output classes
        logger.debug("preparing process parameters")
        self._params_at_zoom = raw_conf_at_zoom(
            self.parsed_config, self.init_zoom_levels
        )
        # TODO: check execute function parameters and provide warnings in case parameters
        # have been omitted, are not defined in the config, or have the wrong type
        if self.process:
            self.process.analyze_parameters(
                {
                    zoom: zoom_parameters(self.parsed_config, zoom)
                    for zoom in self.init_zoom_levels
                }
            )

        # (6) determine process area and process boundaries both from config as well
        # as from initialization.
        # First, the area and bounds parameters from the configuration are checked. If
        # both are provided, the intersection will be taken into account. If none are,
        # the process pyramid area is assumed.
        # Second, they can be overrided by the area and bounds kwargs when constructing
        # the configuration.
        # To finally determine the process tiles, the intersection of process area and the
        # union of all inputs is considered.
        self.area = self._get_process_area(
            area=self.parsed_config.area,
            bounds=self.parsed_config.bounds,
            area_fallback=box(*self.process_pyramid.bounds),
            bounds_fallback=self.process_pyramid.bounds,
            area_crs=area_crs,
            bounds_crs=bounds_crs,
        )
        logger.info(f"process area: {self.area}")
        self.bounds = Bounds(*self.area.bounds)
        logger.debug(f"process bounds: {self.bounds}")
        self.init_area = self._get_process_area(
            area=self._init_area,
            bounds=self._init_bounds,
            area_fallback=self.area,
            bounds_fallback=self.bounds,
            area_crs=area_crs,
            bounds_crs=bounds_crs,
        )
        logger.info(f"init area: {self.init_area}")
        self.init_bounds = Bounds(*self.init_area.bounds)
        logger.debug(f"init bounds: {self.init_bounds}")

        # (7) the delimiters are used by some input drivers
        self._delimiters

        # (8) initialize output
        logger.info("initializing output")
        self.output

        # (9) initialize input items
        # depending on the inputs this action takes the longest and is done
        # in the end to let all other actions fail earlier if necessary
        logger.info("initializing input")
        self.input

        # (10) some output drivers such as the GeoTIFF single file driver also needs the
        # process area to prepare
        logger.debug("prepare output")
        self.output.prepare(process_area=self.area_at_zoom())

    def __repr__(self):  # pragma: no cover
        return f"<MapcheteConfig init_zoom_levels={self.init_zoom_levels}, init_bounds={self.init_bounds}>"

    def input_at_zoom(self, key=None, zoom=None):
        if zoom is None:  # pragma: no cover
            raise ValueError("zoom not provided")
        return self.input[get_input_key(self._params_at_zoom[zoom]["input"][key])]

    def preprocessing_tasks_per_input(self):
        """Get all preprocessing tasks defined by the input drivers."""
        return {
            k: inp.preprocessing_tasks
            for k, inp in self.input.items()
            if inp is not None
        }

    def preprocessing_tasks(self):
        """Get mapping of all preprocessing tasks."""
        return {
            task_key: task
            for _, inp_preprocessing_tasks in self.preprocessing_tasks_per_input().items()
            for task_key, task in inp_preprocessing_tasks.items()
        }

    def preprocessing_tasks_count(self):
        """Return number of preprocessing tasks."""
        return len(self.preprocessing_tasks())

    def preprocessing_task_finished(self, task_key):
        """Return True if task of given key has already been run."""
        inp_key, task_key = task_key.split(":")[0], ":".join(task_key.split(":")[1:])
        try:
            inp = self.input[inp_key]
        except KeyError:  # pragma: no cover
            raise KeyError(f"input {inp_key} not found")
        return inp.preprocessing_task_finished(task_key)

    def set_preprocessing_task_result(self, task_key, result):
        """Append preprocessing task result to input."""
        if ":" in task_key:
            inp_key = task_key.split(":")[0]
        else:
            raise KeyError(
                f"preprocessing task cannot be assigned to an input: {task_key}"
            )
        for inp in self.input.values():
            if inp_key == inp.input_key:
                break
        else:  # pragma: no cover
            raise KeyError(
                f"task {task_key} cannot be assigned to input with key {inp_key}"
            )
        inp.set_preprocessing_task_result(task_key, result)

    @cached_property
    def zoom_levels(self):
        """Process zoom levels as defined in the configuration."""
        return validate_zooms(self.parsed_config.zoom_levels)

    @cached_property
    def init_zoom_levels(self):
        """
        Zoom levels this process is currently initialized with.

        This gets triggered by using the ``zoom`` kwarg. If not set, it will
        be equal to self.zoom_levels.
        """
        try:
            return get_zoom_levels(
                process_zoom_levels=self.parsed_config.zoom_levels,
                init_zoom_levels=self._init_zoom_levels,
            )
        except Exception as e:
            logger.exception(e)
            raise MapcheteConfigError(e)

    @cached_property
    def effective_bounds(self):
        """
        Effective process bounds required to initialize inputs.

        Process bounds sometimes have to be larger, because all intersecting process
        tiles have to be covered as well.
        """
        # highest process (i.e. non-overview) zoom level
        zoom = (
            min(self.baselevels["zooms"])
            if self.baselevels
            else min(self.init_zoom_levels)
        )
        return snap_bounds(
            bounds=clip_bounds(
                bounds=self.init_bounds, clip=self.process_pyramid.bounds
            ),
            pyramid=self.process_pyramid,
            zoom=zoom,
        )

    @cached_property
    def effective_area(self):
        """
        Effective process area required to initialize inputs.

        This area is the true process area of all process tiles combined.
        """
        with Timer() as timer:
            # highest process (i.e. non-overview) zoom level
            zoom = (
                min(self.baselevels["zooms"])
                if self.baselevels
                else min(self.init_zoom_levels)
            )
            geom = snap_geometry_to_tiles(
                self.area.intersection(self.init_area), self.process_pyramid, zoom
            )
        logger.debug("created effective area in %s", timer)
        return geom

    @cached_property
    def _delimiters(self):
        return dict(
            zoom=self.init_zoom_levels,
            bounds=self.init_bounds,
            process_bounds=self.bounds,
            effective_bounds=self.effective_bounds,
            effective_area=self.effective_area,
        )

    @cached_property
    def _output_params(self):
        """Output params of driver."""
        output_params = dict(
            self.parsed_config.output,
            grid=self.output_pyramid.grid,
            pixelbuffer=self.output_pyramid.pixelbuffer,
            metatiling=self.output_pyramid.metatiling,
            delimiters=self._delimiters,
            mode=self.mode,
        )
        if "path" in output_params:
            output_params.update(
                path=MPath.from_inp(output_params).absolute_path(
                    base_dir=self.config_dir
                )
            )

        if "format" not in output_params:
            raise MapcheteConfigError("output format not specified")

        if output_params["format"] not in available_output_formats():
            raise MapcheteConfigError(
                "format %s not available in %s"
                % (output_params["format"], str(available_output_formats()))
            )
        return output_params

    @cached_property
    def output(self):
        """Output writer class of driver."""
        writer = load_output_writer(
            self._output_params, readonly=self._output_params["mode"] == "readonly"
        )
        try:
            writer.is_valid_with_config(self._output_params)
        except Exception as e:
            logger.exception(e)
            raise MapcheteConfigError(
                "driver %s not compatible with configuration: %s"
                % (writer.METADATA["driver_name"], e)
            )
        return writer

    @cached_property
    def output_reader(self):
        """Output reader class of driver."""
        if self.baselevels:
            return load_output_reader(self._output_params)
        else:
            return self.output

    @cached_property
    def input(self):
        """
        Input items used for process stored in a dictionary.

        Keys are the hashes of the input parameters, values the respective InputData
        classes.

        If process mode is `readonly` or if only overviews are about to be built, no
        inputs are required and thus not initialized due to performance reasons. However,
        process bounds which otherwise are dependant on input bounds, may change if not
        explicitly provided in process configuration.
        """
        # get input items only of initialized zoom levels
        try:
            raw_inputs = OrderedDict(
                [
                    # convert input definition to hash
                    (get_input_key(v), v)
                    for zoom in self.init_zoom_levels
                    if "input" in self._params_at_zoom[zoom]
                    # to preserve file groups, "flatten" the input tree and use
                    # the tree paths as keys
                    for key, v in _flatten_tree(self._params_at_zoom[zoom]["input"])
                    if v is not None
                ]
            )
        except TypeError as exc:
            raise MapcheteConfigError(exc)
        if self._init_inputs:
            return initialize_inputs(
                raw_inputs,
                config_dir=self.config_dir,
                pyramid=self.process_pyramid,
                delimiters=self._delimiters,
                readonly=self.mode == "readonly",
            )

        else:
            return OrderedDict([(k, None) for k in raw_inputs.keys()])

    @cached_property
    def baselevels(self):
        """
        Base levels are zoom levels which are processed but not generated by other zoom levels.

        If base levels are not defined, all zoom levels will be processed.

        baselevels:
            min: <zoom>
            max: <zoom>
            lower: <resampling method>
            higher: <resampling method>
        """
        if self.parsed_config.baselevels is None:
            return {}
        baselevels = self.parsed_config.baselevels
        minmax = {k: v for k, v in baselevels.items() if k in ["min", "max"]}

        if not minmax:
            raise MapcheteConfigError("no min and max values given for baselevels")
        for v in minmax.values():
            if not isinstance(v, int) or v < 0:
                raise MapcheteConfigError(
                    "invalid baselevel zoom parameter given: %s" % minmax.values()
                )

        zooms = list(
            range(
                minmax.get("min", min(self.zoom_levels)),
                minmax.get("max", max(self.zoom_levels)) + 1,
            )
        )

        if not set(self.zoom_levels).difference(set(zooms)):
            raise MapcheteConfigError("baselevels zooms fully cover process zooms")

        return dict(
            zooms=zooms,
            lower=baselevels.get("lower", "nearest"),
            higher=baselevels.get("higher", "nearest"),
            tile_pyramid=BufferedTilePyramid(
                self.output_pyramid.grid,
                pixelbuffer=self.output_pyramid.pixelbuffer,
                metatiling=self.process_pyramid.metatiling,
            ),
        )

    @cached_property
    def overview_levels(self) -> Union[ZoomLevels, None]:
        if self.baselevels:
            return self.zoom_levels.difference(self.baselevels["zooms"])
        else:
            return None

    @cached_property
    def processing_levels(self) -> ZoomLevels:
        if self.overview_levels:
            return self.zoom_levels.difference(self.overview_levels)
        else:
            return self.zoom_levels

    def get_process_func_params(self, zoom):
        """Return function kwargs."""
        return self.process.filter_parameters(
            self.params_at_zoom(zoom).get("process_parameters", {})
        )

    def get_inputs_for_tile(self, tile):
        """Get and open all inputs for given tile."""

        return OrderedDict(
            list(open_inputs(self.params_at_zoom(tile.zoom)["input"], tile))
        )

    def params_at_zoom(self, zoom):
        """
        Return configuration parameters snapshot for zoom as dictionary.

        Parameters
        ----------
        zoom : int
            zoom level

        Returns
        -------
        configuration snapshot : dictionary
        zoom level dependent process configuration
        """
        if zoom not in self.init_zoom_levels:
            raise ValueError(
                f"zoom level {zoom} not available with current configuration: {self.init_zoom_levels}"
            )
        out = OrderedDict(
            self._params_at_zoom[zoom], input=OrderedDict(), output=self.output
        )
        if "input" in self._params_at_zoom[zoom]:
            flat_inputs = OrderedDict()
            for k, v in _flatten_tree(self._params_at_zoom[zoom]["input"]):
                if v is None:  # pragma: no cover
                    flat_inputs[k] = None
                else:
                    flat_inputs[k] = self.input[get_input_key(v)]
            out["input"] = _unflatten_tree(flat_inputs)
        else:
            out["input"] = OrderedDict()
        return out

    def area_at_zoom(self, zoom=None):
        """
        Return process area for zoom level.

        The current process area is an intersection of the defined process area from the configuration,
        the spatial subsets provided by extra arguments (e.g. bounds) and the union of all inputs.

        Parameters
        ----------
        zoom : int or None
            if None, the union of all zoom level areas is returned

        Returns
        -------
        process area : shapely geometry
        """
        if not self._init_inputs:
            return self.init_area
        if zoom is None:
            if not self._cache_full_process_area:
                logger.debug("calculate process area ...")
                self._cache_full_process_area = unary_union(
                    [self._area_at_zoom(z) for z in self.init_zoom_levels]
                ).buffer(0)
            return self._cache_full_process_area
        else:
            if zoom not in self.init_zoom_levels:
                raise ValueError(
                    f"zoom level {zoom} not available with current configuration: {self.init_zoom_levels}"
                )
            return self._area_at_zoom(zoom)

    def _area_at_zoom(self, zoom):
        if zoom not in self._cache_area_at_zoom:
            # use union of all input items and, if available, intersect with
            # init_bounds
            if "input" in self._params_at_zoom[zoom]:
                input_union = unary_union(
                    [
                        self.input[get_input_key(v)].bbox(self.process_pyramid.crs)
                        for k, v in _flatten_tree(self._params_at_zoom[zoom]["input"])
                        if v is not None
                    ]
                )
                inputs_and_init = (
                    input_union.intersection(self.init_area)
                    if self.init_area
                    else input_union
                )
            # if no input items are available, just use init_bounds
            else:
                inputs_and_init = self.init_area
            self._cache_area_at_zoom[zoom] = inputs_and_init.intersection(self.area)
        return self._cache_area_at_zoom[zoom]

    def bounds_at_zoom(self, zoom=None):
        """
        Return process bounds for zoom level.

        Parameters
        ----------
        zoom : integer or list

        Returns
        -------
        process bounds : tuple
            left, bottom, right, top
        """
        return (
            ()
            if self.area_at_zoom(zoom).is_empty
            else Bounds(*self.area_at_zoom(zoom).bounds)
        )

    def _get_process_area(
        self,
        area=None,
        bounds=None,
        area_fallback=None,
        bounds_fallback=None,
        area_crs=None,
        bounds_crs=None,
    ):
        """
        Determine process area by combining configuration with instantiation arguments.

        In the configuration the process area can be provided by using the (1) ``area``
        option, (2) ``bounds`` option or (3) a combination of both.

        (1) If only ``area`` is provided, output shall be the area geometry
        (2) If only ``bounds`` is provided, output shall be box(*self.bounds)
        (3) If both are provided, output shall be the intersection between ``area`` and
        ``bounds``

        The area parameter can be provided in multiple variations, see _guess_geometry().
        """
        try:
            dst_crs = self.process_pyramid.crs

            if bounds is None and area is None:
                return area_fallback

            elif bounds is None:
                area, crs = guess_geometry(area, base_dir=self.config_dir)
                # in case vector file has no CRS use manually provided CRS
                area_crs = crs or area_crs

                return reproject_geometry(
                    area, src_crs=area_crs or dst_crs, dst_crs=dst_crs
                )

            elif area is None:
                return reproject_geometry(
                    box(*Bounds.from_inp(bounds)),
                    src_crs=bounds_crs or dst_crs,
                    dst_crs=dst_crs,
                )

            else:
                area, crs = guess_geometry(area, base_dir=self.config_dir)
                # in case vector file has no CRS use manually provided CRS
                area_crs = crs or area_crs

                bounds = Bounds.from_inp(bounds)

                # reproject area and bounds to process CRS and return intersection
                return reproject_geometry(
                    area, src_crs=area_crs or dst_crs, dst_crs=dst_crs
                ).intersection(
                    reproject_geometry(
                        box(*Bounds.from_inp(bounds)),
                        src_crs=bounds_crs or dst_crs,
                        dst_crs=dst_crs,
                    ),
                )
        except Exception as e:
            raise MapcheteConfigError(e)

    # deprecated:
    #############

    @cached_property
    def crs(self):
        """Deprecated."""
        warnings.warn(DeprecationWarning("self.crs is now self.process_pyramid.crs."))
        return self.process_pyramid.crs

    @cached_property
    def metatiling(self):
        """Deprecated."""
        warnings.warn(
            DeprecationWarning(
                "self.metatiling is now self.process_pyramid.metatiling."
            )
        )
        return self.process_pyramid.metatiling

    @cached_property
    def pixelbuffer(self):
        """Deprecated."""
        warnings.warn(
            DeprecationWarning(
                "self.pixelbuffer is now self.process_pyramid.pixelbuffer."
            )
        )
        return self.process_pyramid.pixelbuffer

    @cached_property
    def inputs(self):
        """Deprecated."""
        warnings.warn(DeprecationWarning("self.inputs renamed to self.input."))
        return self.input

    def at_zoom(self, zoom):
        """Deprecated."""
        warnings.warn(
            DeprecationWarning("Method renamed to self.params_at_zoom(zoom).")
        )
        return self.params_at_zoom(zoom)


def get_input_key(
    input_definition: Union[MPathLike, dict], hash_length: int = 6
) -> str:
    try:
        path = MPath.from_inp(input_definition)
        return f"{path}-{get_hash(input_definition, length=hash_length)}"
    except ValueError:
        pass
    if isinstance(input_definition, dict):
        return f"{input_definition['format']}-{get_hash(input_definition, length=hash_length)}"
    else:  # pragma: no cover
        raise ValueError(f"cannot generate input_key from {input_definition}")


def get_hash(some_object: Any, length: int = 16) -> str:
    """Return hash of some_object."""
    if isinstance(some_object, MPath):
        some_object = str(some_object)
    try:
        return hashlib.sha224(yaml.dump(dict(key=some_object)).encode()).hexdigest()[
            :length
        ]
    except TypeError:  # pragma: no cover
        # in case yaml.dump fails, we just try to get a string representation of object
        return hashlib.sha224(str(some_object).encode()).hexdigest()[:length]


def snap_bounds(
    bounds: BoundsLike = None, pyramid: BufferedTilePyramid = None, zoom: int = None
) -> Bounds:
    """
    Snap bounds to tiles boundaries of specific zoom level.

    Parameters
    ----------
    bounds : bounds to be snapped
    pyramid : TilePyramid
    zoom : int

    Returns
    -------
    Bounds(left, bottom, right, top)
    """
    bounds = Bounds.from_inp(bounds)
    pyramid = validate_bufferedtilepyramid(pyramid)
    lb = pyramid.tile_from_xy(bounds.left, bounds.bottom, zoom, on_edge_use="rt").bounds
    rt = pyramid.tile_from_xy(bounds.right, bounds.top, zoom, on_edge_use="lb").bounds
    return Bounds(lb.left, lb.bottom, rt.right, rt.top)


def clip_bounds(bounds: BoundsLike = None, clip: BoundsLike = None) -> Bounds:
    """
    Clip bounds by clip.

    Parameters
    ----------
    bounds : bounds to be clipped
    clip : clip bounds

    Returns
    -------
    Bounds(left, bottom, right, top)
    """
    bounds = Bounds.from_inp(bounds)
    clip = Bounds.from_inp(clip)
    return Bounds(
        max(bounds.left, clip.left),
        max(bounds.bottom, clip.bottom),
        min(bounds.right, clip.right),
        min(bounds.top, clip.top),
    )


def initialize_inputs(
    raw_inputs: dict,
    config_dir: Optional[MPathLike] = None,
    pyramid: BufferedTilePyramid = None,
    delimiters: dict = None,
    readonly: bool = False,
) -> OrderedDict:
    initalized_inputs = OrderedDict()
    for k, v in raw_inputs.items():
        # for files and tile directories
        if isinstance(v, (str, MPath)):
            logger.debug("load input reader for simple input %s", v)
            try:
                reader = load_input_reader(
                    dict(
                        path=absolute_path(path=v, base_dir=config_dir),
                        pyramid=pyramid,
                        pixelbuffer=pyramid.pixelbuffer,
                        delimiters=delimiters,
                    ),
                    readonly=readonly,
                    input_key=k,
                )
            except Exception as e:
                logger.exception(e)
                raise MapcheteDriverError(
                    "error when loading input %s: %s" % (v, e)
                ) from e
            logger.debug("input reader for simple input %s is %s", v, reader)

        # for abstract inputs
        elif isinstance(v, dict):
            logger.debug("load input reader for abstract input %s", v)
            try:
                abstract = deepcopy(v)
                # make path absolute and add filesystem options
                if "path" in abstract:
                    abstract.update(
                        path=MPath.from_inp(abstract).absolute_path(config_dir)
                    )
                reader = load_input_reader(
                    dict(
                        abstract=abstract,
                        pyramid=pyramid,
                        pixelbuffer=pyramid.pixelbuffer,
                        delimiters=delimiters,
                        conf_dir=config_dir,
                    ),
                    readonly=readonly,
                    input_key=k,
                )
            except Exception as e:
                logger.exception(e)
                raise MapcheteDriverError("error when loading input %s: %s" % (v, e))
            logger.debug("input reader for abstract input %s is %s", v, reader)
        else:  # pragma: no cover
            raise MapcheteConfigError("invalid input type %s", type(v))
        # trigger bbox creation
        reader.bbox(out_crs=pyramid.crs)
        initalized_inputs[k] = reader

    logger.debug(
        "initialized inputs: %s",
        initalized_inputs.keys(),
    )
    return initalized_inputs


def open_inputs(inputs: dict, tile: BufferedTile) -> Iterator[Tuple]:
    for k, v in inputs.items():
        if v is None:
            continue
        elif isinstance(v, dict):
            yield (k, list(open_inputs(v, tile)))
        else:
            yield (k, v.open(tile))


def _flatten_tree(tree, old_path=None):
    """Flatten dict tree into dictionary where keys are paths of old dict."""
    flat_tree = []
    for key, value in tree.items():
        new_path = "/".join([old_path, key]) if old_path else key
        if isinstance(value, dict) and "format" not in value:
            flat_tree.extend(_flatten_tree(value, old_path=new_path))
        else:
            flat_tree.append((new_path, value))
    return flat_tree


def _unflatten_tree(flat):
    """Reverse tree flattening."""
    tree = OrderedDict()
    for key, value in flat.items():
        path = key.split("/")
        # we are at the end of a branch
        if len(path) == 1:
            tree[key] = value
        # there are more branches
        else:
            # create new dict
            if path[0] not in tree:
                tree[path[0]] = _unflatten_tree({"/".join(path[1:]): value})
            # add keys to existing dict
            else:
                branch = _unflatten_tree({"/".join(path[1:]): value})
                if path[1] not in tree[path[0]]:
                    tree[path[0]][path[1]] = branch[path[1]]
                else:
                    tree[path[0]][path[1]].update(branch[path[1]])
    return tree
