"""
Configuration object required to rund a Mapchete process.

Before running a process, a MapcheteConfig object has to be initialized by
either using a Mapchete file or a dictionary holding the process parameters.
Upon creation, all parameters are validated and the InputData objects are
created which are then exposed to the user process.

An invalid process configuration or an invalid process file cause an Exception
when initializing the configuration.
"""

from cached_property import cached_property
from collections import OrderedDict
from copy import deepcopy
import fiona
import hashlib
import importlib
import inspect
import logging
import operator
import os
import oyaml as yaml
import py_compile
from shapely import wkt
from shapely.geometry import box, Point, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import cascaded_union
import sys
from tempfile import NamedTemporaryFile
from tilematrix._funcs import Bounds
import warnings

from mapchete.validate import (
    validate_bounds,
    validate_zooms,
    validate_values,
    validate_bufferedtilepyramid,
)
from mapchete.errors import (
    MapcheteConfigError,
    MapcheteProcessSyntaxError,
    MapcheteProcessImportError,
    MapcheteDriverError,
    GeometryTypeError,
)
from mapchete.formats import (
    load_output_reader,
    load_output_writer,
    available_output_formats,
    load_input_reader,
)
from mapchete.io import absolute_path
from mapchete.io.vector import clean_geometry_type, reproject_geometry
from mapchete.log import add_module_logger
from mapchete.tile import BufferedTilePyramid


logger = logging.getLogger(__name__)

# parameters which have to be provided in the configuration and their types
_MANDATORY_PARAMETERS = [
    ("process", (str, list, type(None))),  # path to .py file or module path
    ("pyramid", dict),  # process pyramid definition
    ("input", (dict, type(None))),  # files & other types
    ("output", dict),  # process output parameters
    ("zoom_levels", (int, dict, list)),  # process zoom levels
]

# parameters with special functions which cannot be used for user parameters
_RESERVED_PARAMETERS = [
    "area",  # geometry limiting process area
    "area_crs",  # optional CRS of area (default: process CRS)
    "baselevels",  # enable interpolation from other zoom levels
    "bounds",  # process bounds
    "bounds_crs",  # optional CRS of bounds (default: process CRS)
    "config_dir",  # configuration base directory
    "metatiling",  # process metatile size (deprecated)
    "pixelbuffer",  # buffer around each tile in pixels (deprecated)
    "process",  # path to .py file or module path
    "process_minzoom",  # minimum zoom where process is valid (deprecated)
    "process_maxzoom",  # maximum zoom where process is valid (deprecated)
    "process_zoom",  # single zoom where process is valid (deprecated)
    "process_bounds",  # process boundaries (deprecated)
]

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

MULTIPROCESSING_DEFAULT_START_METHOD = "spawn"


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
    single_input_file : string
        single input file if supported by process
    mode : string
        * ``memory``: Generate process output on demand without reading
          pre-existing data or writing new data.
        * ``readonly``: Just read data without processing new data.
        * ``continue``: (default) Don't overwrite existing output.
        * ``overwrite``: Overwrite existing output.

    Attributes
    ----------
    mode : string
        process mode
    process : string
        absolute path to process file
    config_dir : string
        path to configuration directory
    process_pyramid : ``tilematrix.TilePyramid``
        ``TilePyramid`` used to process data
    output_pyramid : ``tilematrix.TilePyramid``
        ``TilePyramid`` used to write output data
    input : dictionary
        inputs for process
    output : ``OutputData``
        driver specific output object
    zoom_levels : list
        process zoom levels
    zoom : list or int
        subset zoom levels provided in process configuration
    bounds : tuple
        override bounds or area provided in process configuration
    bounds_crs : str or CRS
        CRS of bounds (default: process CRS)
    area : geometry
        override bounds or area provided in process configuration
    area_crs : str or CRS
        CRS of area (default: process CRS)
    baselevels : dictionary
        base zoomlevels, where data is processed; zoom levels not included are
        generated from baselevels

    Deprecated Attributes
    ---------------------
    raw : dictionary
        raw process configuration
    mapchete_file : string
        path to Mapchete file
    output_type : string (moved to OutputData)
        process output type (``raster`` or ``vector``)
    crs : ``rasterio.crs.CRS`` (moved to process_pyramid)
        object describing the process coordinate reference system
    pixelbuffer : integer (moved to process_pyramid)
        buffer around process tiles
    metatiling : integer (moved to process_pyramid)
        process metatiling
    """

    def __init__(
        self,
        input_config,
        zoom=None,
        area=None,
        area_crs=None,
        bounds=None,
        bounds_crs=None,
        single_input_file=None,
        mode="continue",
        debug=False,
        **kwargs,
    ):
        """Initialize configuration."""
        logger.debug(f"parsing {input_config}")
        # get dictionary representation of input_config and
        # (0) map deprecated params to new structure
        self._raw = _map_to_new_config(_config_to_dict(input_config))
        self._raw["init_zoom_levels"] = zoom
        self._raw["init_bounds"] = bounds
        self._raw["init_bounds_crs"] = bounds_crs
        self._raw["init_area"] = area
        self._raw["init_area_crs"] = area_crs
        self._cache_area_at_zoom = {}
        self._cache_full_process_area = None

        if mode not in ["memory", "continue", "readonly", "overwrite"]:
            raise MapcheteConfigError("unknown mode %s" % mode)
        self.mode = mode
        self.preprocessing_tasks_finished = False

        # (1) assert mandatory params are available
        try:
            validate_values(self._raw, _MANDATORY_PARAMETERS)
        except Exception as e:
            raise MapcheteConfigError(e)

        # (2) check user process
        self.config_dir = self._raw["config_dir"]
        self.process_name = self.process_path = self.process = self._raw["process"]
        if self.mode != "readonly":
            logger.debug("validating process code")
            self.process_func

        # (3) set process and output pyramids
        logger.debug("initializing pyramids")
        try:
            process_metatiling = self._raw["pyramid"].get("metatiling", 1)
            # output metatiling defaults to process metatiling if not set
            # explicitly
            output_metatiling = self._raw["output"].get(
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
                self._raw["pyramid"]["grid"],
                metatiling=process_metatiling,
                pixelbuffer=self._raw["pyramid"].get("pixelbuffer", 0),
            )
            self.output_pyramid = BufferedTilePyramid(
                self._raw["pyramid"]["grid"],
                metatiling=output_metatiling,
                pixelbuffer=self._raw["output"].get("pixelbuffer", 0),
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
        self._params_at_zoom = _raw_at_zoom(self._raw, self.init_zoom_levels)

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
            area=self._raw.get("area"),
            bounds=self._raw.get("bounds"),
            area_fallback=box(*self.process_pyramid.bounds),
            bounds_fallback=self.process_pyramid.bounds,
            area_crs=area_crs,
            bounds_crs=bounds_crs,
        )
        logger.debug(f"process area: {self.area}")
        self.bounds = Bounds(*self.area.bounds)
        logger.debug(f"process bounds: {self.bounds}")
        self.init_area = self._get_process_area(
            area=self._raw.get("init_area"),
            bounds=self._raw.get("init_bounds"),
            area_fallback=self.area,
            bounds_fallback=self.bounds,
            area_crs=area_crs,
            bounds_crs=bounds_crs,
        )
        logger.debug(f"init area: {self.init_area}")
        self.init_bounds = Bounds(*self.init_area.bounds)
        logger.debug(f"init bounds: {self.init_bounds}")

        # (7) the delimiters are used by some input drivers
        self._delimiters = dict(
            zoom=self.init_zoom_levels,
            bounds=self.init_bounds,
            process_bounds=self.bounds,
            effective_bounds=self.effective_bounds,
        )

        # (8) initialize output
        logger.debug("initializing output")
        self.output

        # (9) initialize input items
        # depending on the inputs this action takes the longest and is done
        # in the end to let all other actions fail earlier if necessary
        logger.debug("initializing input")
        self.input

        # (10) some output drivers such as the GeoTIFF single file driver also needs the
        # process area to prepare
        logger.debug("prepare output")
        self.output.prepare(process_area=self.area_at_zoom())

    def input_at_zoom(self, key=None, zoom=None):
        if zoom is None:  # pragma: no cover
            raise ValueError("zoom not provided")
        return self.input[get_hash(self._params_at_zoom[zoom]["input"][key])]

    def preprocessing_tasks_per_input(self):
        """Get all preprocessing tasks defined by the input drivers."""
        return {
            k: inp.preprocessing_tasks
            for k, inp in self.input.items()
            if inp is not None
        }

    def preprocessing_tasks(self):
        return {
            task_key: task
            for preprocessing_tasks in self.preprocessing_tasks_per_input().values()
            for task_key, task in preprocessing_tasks.items()
        }

    def preprocessing_tasks_count(self):
        """Return number of unique preprocessing tasks."""
        return len(self.preprocessing_tasks())

    def set_preprocessing_task_result(self, task_key, result):
        """Append preprocessing task result to input."""
        found = False
        for inp in self.input.values():
            if task_key in inp.preprocessing_tasks:
                found = True
                inp.preprocessing_tasks_results[task_key] = result
        if not found:
            raise KeyError(f"task key {task_key} not found in any input")

    @cached_property
    def zoom_levels(self):
        """Process zoom levels as defined in the configuration."""
        return validate_zooms(self._raw["zoom_levels"])

    @cached_property
    def init_zoom_levels(self):
        """
        Zoom levels this process is currently initialized with.

        This gets triggered by using the ``zoom`` kwarg. If not set, it will
        be equal to self.zoom_levels.
        """
        try:
            return get_zoom_levels(
                process_zoom_levels=self._raw["zoom_levels"],
                init_zoom_levels=self._raw["init_zoom_levels"],
            )
        except Exception as e:
            raise MapcheteConfigError(e)

    @cached_property
    def effective_bounds(self):
        """
        Effective process bounds required to initialize inputs.

        Process bounds sometimes have to be larger, because all intersecting process
        tiles have to be covered as well.
        """
        return snap_bounds(
            bounds=clip_bounds(
                bounds=self.init_bounds, clip=self.process_pyramid.bounds
            ),
            pyramid=self.process_pyramid,
            zoom=min(self.baselevels["zooms"])
            if self.baselevels
            else min(self.init_zoom_levels),
        )

    @cached_property
    def _output_params(self):
        """Output params of driver."""
        output_params = dict(
            self._raw["output"],
            grid=self.output_pyramid.grid,
            pixelbuffer=self.output_pyramid.pixelbuffer,
            metatiling=self.output_pyramid.metatiling,
            delimiters=self._delimiters,
            mode=self.mode,
        )
        if "path" in output_params:
            output_params.update(
                path=absolute_path(path=output_params["path"], base_dir=self.config_dir)
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
        raw_inputs = OrderedDict(
            [
                # convert input definition to hash
                (get_hash(v), v)
                for zoom in self.init_zoom_levels
                if "input" in self._params_at_zoom[zoom]
                # to preserve file groups, "flatten" the input tree and use
                # the tree paths as keys
                for key, v in _flatten_tree(self._params_at_zoom[zoom]["input"])
                if v is not None
            ]
        )

        initalized_inputs = OrderedDict()

        if self._init_inputs:
            for k, v in raw_inputs.items():
                # for files and tile directories
                if isinstance(v, str):
                    logger.debug("load input reader for simple input %s", v)
                    try:
                        reader = load_input_reader(
                            dict(
                                path=absolute_path(path=v, base_dir=self.config_dir),
                                pyramid=self.process_pyramid,
                                pixelbuffer=self.process_pyramid.pixelbuffer,
                                delimiters=self._delimiters,
                            ),
                            readonly=self.mode == "readonly",
                        )
                    except Exception as e:
                        logger.exception(e)
                        raise MapcheteDriverError(
                            "error when loading input %s: %s" % (v, e)
                        )
                    logger.debug("input reader for simple input %s is %s", v, reader)

                # for abstract inputs
                elif isinstance(v, dict):
                    logger.debug("load input reader for abstract input %s", v)
                    try:
                        reader = load_input_reader(
                            dict(
                                abstract=deepcopy(v),
                                pyramid=self.process_pyramid,
                                pixelbuffer=self.process_pyramid.pixelbuffer,
                                delimiters=self._delimiters,
                                conf_dir=self.config_dir,
                            ),
                            readonly=self.mode == "readonly",
                        )
                    except Exception as e:
                        logger.exception(e)
                        raise MapcheteDriverError(
                            "error when loading input %s: %s" % (v, e)
                        )
                    logger.debug("input reader for abstract input %s is %s", v, reader)
                else:
                    raise MapcheteConfigError("invalid input type %s", type(v))
                # trigger bbox creation
                reader.bbox(out_crs=self.process_pyramid.crs)
                initalized_inputs[k] = reader

        else:
            for k in raw_inputs.keys():
                initalized_inputs[k] = None

        return initalized_inputs

    @cached_property
    def baselevels(self):
        """
        Optional baselevels configuration.

        baselevels:
            min: <zoom>
            max: <zoom>
            lower: <resampling method>
            higher: <resampling method>
        """
        if "baselevels" not in self._raw:
            return {}
        baselevels = self._raw["baselevels"]
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
    def process_func(self):
        """Import process function and make syntax check."""
        if self.mode == "readonly":
            raise MapcheteConfigError(
                "process function cannot be loaded in readonly mode."
            )
        else:
            return get_process_func(
                process=self.process,
                config_dir=self.config_dir,
                run_compile=True,
            )

    def get_process_func_params(self, zoom):
        """Return function kwargs."""
        return {
            k: v
            for k, v in self.params_at_zoom(zoom).items()
            if k in inspect.signature(self.process_func).parameters
        }

    def get_inputs_for_tile(self, tile):
        """Get and open all inputs for given tile."""

        def _open_inputs(i):
            for k, v in i.items():
                if v is None:
                    continue
                elif isinstance(v, dict):
                    yield (k, list(_open_inputs(v)))
                else:
                    yield (k, v.open(tile))

        return OrderedDict(list(_open_inputs(self.params_at_zoom(tile.zoom)["input"])))

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
                if v is None:
                    flat_inputs[k] = None
                else:
                    flat_inputs[k] = self.input[get_hash(v)]
            out["input"] = _unflatten_tree(flat_inputs)
        else:
            out["input"] = OrderedDict()
        return out

    def area_at_zoom(self, zoom=None):
        """
        Return process area for zoom level.

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
                self._cache_full_process_area = cascaded_union(
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
                input_union = cascaded_union(
                    [
                        self.input[get_hash(v)].bbox(self.process_pyramid.crs)
                        for k, v in _flatten_tree(self._params_at_zoom[zoom]["input"])
                        if v is not None
                    ]
                )
                self._cache_area_at_zoom[zoom] = (
                    input_union.intersection(self.init_area)
                    if self.init_area
                    else input_union
                )
            # if no input items are available, just use init_bounds
            else:
                self._cache_area_at_zoom[zoom] = self.init_area
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
                area, crs = _guess_geometry(area, base_dir=self.config_dir)
                # in case vector file has no CRS use manually provided CRS
                area_crs = crs or area_crs

                return reproject_geometry(
                    area, src_crs=area_crs or dst_crs, dst_crs=dst_crs
                )

            elif area is None:
                return reproject_geometry(
                    box(*validate_bounds(bounds)),
                    src_crs=bounds_crs or dst_crs,
                    dst_crs=dst_crs,
                )

            else:
                area, crs = _guess_geometry(area, base_dir=self.config_dir)
                # in case vector file has no CRS use manually provided CRS
                area_crs = crs or area_crs

                bounds = validate_bounds(bounds)

                # reproject area and bounds to process CRS and return intersection
                return reproject_geometry(
                    area, src_crs=area_crs or dst_crs, dst_crs=dst_crs
                ).intersection(
                    reproject_geometry(
                        box(*validate_bounds(bounds)),
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

    @cached_property
    def process_file(self):
        """Deprecated."""
        warnings.warn(DeprecationWarning("'self.process_file' is deprecated"))
        return os.path.join(self._raw["config_dir"], self._raw["process"])

    def at_zoom(self, zoom):
        """Deprecated."""
        warnings.warn(
            DeprecationWarning("Method renamed to self.params_at_zoom(zoom).")
        )
        return self.params_at_zoom(zoom)

    def process_area(self, zoom=None):
        """Deprecated."""
        warnings.warn(DeprecationWarning("Method renamed to self.area_at_zoom(zoom)."))
        return self.area_at_zoom(zoom)

    def process_bounds(self, zoom=None):
        """Deprecated."""
        warnings.warn(
            DeprecationWarning("Method renamed to self.bounds_at_zoom(zoom).")
        )
        return self.bounds_at_zoom(zoom)


def get_hash(x, length=16):
    """Return hash of x."""
    return hashlib.sha224(yaml.dump(dict(key=x)).encode()).hexdigest()[:length]


def get_zoom_levels(process_zoom_levels=None, init_zoom_levels=None):
    """Validate and return zoom levels."""
    process_zoom_levels = validate_zooms(process_zoom_levels)
    if init_zoom_levels is None:
        return process_zoom_levels
    else:
        init_zoom_levels = validate_zooms(init_zoom_levels)
        if not set(init_zoom_levels).issubset(
            set(process_zoom_levels)
        ):  # pragma: no cover
            raise ValueError("init zooms must be a subset of process zoom")
        return init_zoom_levels


def snap_bounds(bounds=None, pyramid=None, zoom=None):
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
    bounds = validate_bounds(bounds)
    pyramid = validate_bufferedtilepyramid(pyramid)
    lb = pyramid.tile_from_xy(bounds.left, bounds.bottom, zoom, on_edge_use="rt").bounds
    rt = pyramid.tile_from_xy(bounds.right, bounds.top, zoom, on_edge_use="lb").bounds
    return Bounds(lb.left, lb.bottom, rt.right, rt.top)


def clip_bounds(bounds=None, clip=None):
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
    bounds = validate_bounds(bounds)
    clip = validate_bounds(clip)
    return Bounds(
        max(bounds.left, clip.left),
        max(bounds.bottom, clip.bottom),
        min(bounds.right, clip.right),
        min(bounds.top, clip.top),
    )


def raw_conf(mapchete_file):
    """
    Load a mapchete_file into a dictionary.

    Parameters
    ----------
    mapchete_file : str
        Path to a Mapchete file.

    Returns
    -------
    dictionary
    """
    if isinstance(mapchete_file, dict):
        return _map_to_new_config(mapchete_file)
    else:
        return _map_to_new_config(yaml.safe_load(open(mapchete_file, "r").read()))


def raw_conf_process_pyramid(raw_conf, reset_pixelbuffer=False):
    """
    Load the process pyramid of a raw configuration.

    Parameters
    ----------
    raw_conf : dict
        Raw mapchete configuration as dictionary.

    Returns
    -------
    BufferedTilePyramid
    """
    pixelbuffer = 0 if reset_pixelbuffer else raw_conf["pyramid"].get("pixelbuffer", 0)
    return BufferedTilePyramid(
        raw_conf["pyramid"]["grid"],
        metatiling=raw_conf["pyramid"].get("metatiling", 1),
        pixelbuffer=pixelbuffer,
    )


def raw_conf_output_pyramid(raw_conf):
    """
    Load the process pyramid of a raw configuration.

    Parameters
    ----------
    raw_conf : dict
        Raw mapchete configuration as dictionary.

    Returns
    -------
    BufferedTilePyramid
    """
    return BufferedTilePyramid(
        raw_conf["pyramid"]["grid"],
        metatiling=raw_conf["output"].get(
            "metatiling", raw_conf["pyramid"].get("metatiling", 1)
        ),
        pixelbuffer=raw_conf["pyramid"].get(
            "pixelbuffer", raw_conf["pyramid"].get("pixelbuffer", 0)
        ),
    )


def bounds_from_opts(
    wkt_geometry=None,
    point=None,
    point_crs=None,
    zoom=None,
    bounds=None,
    bounds_crs=None,
    raw_conf=None,
):
    """
    Return process bounds depending on given inputs.

    Parameters
    ----------
    wkt_geometry : string
        WKT geometry used to generate bounds.
    point : iterable
        x and y coordinates of point whose corresponding process tile bounds shall be
        returned.
    point_crs : str or CRS
        CRS of point (default: process pyramid CRS)
    zoom : int
        Mandatory zoom level if point is provided.
    bounds : iterable
        Bounding coordinates to be used
    bounds_crs : str or CRS
        CRS of bounds (default: process pyramid CRS)

    raw_conf : dict
        Raw mapchete configuration as dictionary.

    Returns
    -------
    BufferedTilePyramid
    """
    if wkt_geometry:
        return Bounds(*wkt.loads(wkt_geometry).bounds)
    elif point:
        x, y = point
        tp = raw_conf_process_pyramid(raw_conf)
        if point_crs:
            reproj = reproject_geometry(Point(x, y), src_crs=point_crs, dst_crs=tp.crs)
            x = reproj.x
            y = reproj.y
        zoom_levels = get_zoom_levels(
            process_zoom_levels=raw_conf["zoom_levels"], init_zoom_levels=zoom
        )
        return Bounds(*tp.tile_from_xy(x, y, max(zoom_levels)).bounds)
    elif bounds:
        bounds = validate_bounds(bounds)
        if bounds_crs:
            tp = raw_conf_process_pyramid(raw_conf)
            bounds = Bounds(
                *reproject_geometry(
                    box(*bounds), src_crs=bounds_crs, dst_crs=tp.crs
                ).bounds
            )
        return bounds
    else:
        return


def get_process_func(process=None, config_dir=None, run_compile=False):
    """Import and return process function."""
    logger.debug(f"get process function from {process}")
    process_module = _load_process_module(
        process=process, config_dir=config_dir, run_compile=run_compile
    )
    try:
        if hasattr(process_module, "Process"):
            logger.error(
                """instanciating MapcheteProcess is deprecated, """
                """provide execute() function instead"""
            )
        if hasattr(process_module, "execute"):
            return process_module.execute
        else:
            raise ImportError("No execute() function found in %s" % process)
    except ImportError as e:
        raise MapcheteProcessImportError(e)


def _load_process_module(process=None, config_dir=None, run_compile=False):
    tmpfile = None
    try:
        if isinstance(process, list):
            tmpfile = NamedTemporaryFile(suffix=".py")
            logger.debug(f"writing process code to temporary file {tmpfile.name}")
            with open(tmpfile.name, "w") as dst:
                for line in process:
                    dst.write(line + "\n")
            process = tmpfile.name
        if process.endswith(".py"):
            module_path = absolute_path(path=process, base_dir=config_dir)
            if not os.path.isfile(module_path):
                raise MapcheteConfigError(f"{module_path} is not available")
            try:
                if run_compile:
                    py_compile.compile(module_path, doraise=True)
                module_name = os.path.splitext(os.path.basename(module_path))[0]
                # load module
                spec = importlib.util.spec_from_file_location(module_name, module_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                # required to make imported module available using multiprocessing
                sys.modules[module_name] = module
                # configure process file logger
                add_module_logger(module.__name__)
            except py_compile.PyCompileError as e:
                raise MapcheteProcessSyntaxError(e)
            except ImportError as e:
                raise MapcheteProcessImportError(e)
        else:
            try:
                module = importlib.import_module(process)
            except ImportError as e:
                raise MapcheteProcessImportError(e)
        logger.debug(f"return process func: {module}")
    finally:
        if tmpfile:
            logger.debug(f"removing {tmpfile.name}")
            tmpfile.close()
    return module


def _config_to_dict(input_config):
    if isinstance(input_config, dict):
        if "config_dir" not in input_config:
            raise MapcheteConfigError("config_dir parameter missing")
        return OrderedDict(input_config, mapchete_file=None)
    # from Mapchete file
    elif os.path.splitext(input_config)[1] == ".mapchete":
        with open(input_config, "r") as config_file:
            return OrderedDict(
                yaml.safe_load(config_file.read()),
                config_dir=os.path.dirname(os.path.realpath(input_config)),
                mapchete_file=input_config,
            )
    # throw error if unknown object
    else:  # pragma: no cover
        raise MapcheteConfigError(
            "Configuration has to be a dictionary or a .mapchete file."
        )


def _raw_at_zoom(config, zooms):
    """Return parameter dictionary per zoom level."""
    params_per_zoom = OrderedDict()
    for zoom in zooms:
        params = OrderedDict()
        for name, element in config.items():
            if name not in _RESERVED_PARAMETERS:
                out_element = _element_at_zoom(name, element, zoom)
                if out_element is not None:
                    params[name] = out_element
        params_per_zoom[zoom] = params
    return OrderedDict(params_per_zoom)


def _element_at_zoom(name, element, zoom):
    """
    Return the element filtered by zoom level.

    - An input integer or float gets returned as is.
    - An input string is checked whether it starts with "zoom". Then, the
      provided zoom level gets parsed and compared with the actual zoom
      level. If zoom levels match, the element gets returned.
    TODOs/gotchas:
    - Provided zoom levels for one element in config file are not allowed
      to "overlap", i.e. there is not yet a decision mechanism implemented
      which handles this case.
    """
    # If element is a dictionary, analyze subitems.
    if isinstance(element, dict):
        if "format" in element:
            # we have an input or output driver here
            return element
        out_elements = OrderedDict()
        for sub_name, sub_element in element.items():
            out_element = _element_at_zoom(sub_name, sub_element, zoom)
            if name == "input":
                out_elements[sub_name] = out_element
            elif out_element is not None:
                out_elements[sub_name] = out_element
        # If there is only one subelement, collapse unless it is
        # input. In such case, return a dictionary.
        if len(out_elements) == 1 and name != "input":
            return next(iter(out_elements.values()))
        # If subelement is empty, return None
        if len(out_elements) == 0:
            return None
        return out_elements
    # If element is a zoom level statement, filter element.
    elif isinstance(name, str):
        if name.startswith("zoom"):
            return _filter_by_zoom(
                conf_string=name.strip("zoom").strip(), zoom=zoom, element=element
            )
        # If element is a string but not a zoom level statement, return
        # element.
        else:
            return element
    # Return all other types as they are.
    else:  # pragma: no cover
        return element


def _filter_by_zoom(element=None, conf_string=None, zoom=None):
    """Return element only if zoom condition matches with config string."""
    for op_str, op_func in [
        # order of operators is important:
        # prematurely return in cases of "<=" or ">=", otherwise
        # _strip_zoom() cannot parse config strings starting with "<"
        # or ">"
        ("=", operator.eq),
        ("<=", operator.le),
        (">=", operator.ge),
        ("<", operator.lt),
        (">", operator.gt),
    ]:
        if conf_string.startswith(op_str):
            return element if op_func(zoom, _strip_zoom(conf_string, op_str)) else None


def _strip_zoom(input_string, strip_string):
    """Return zoom level as integer or throw error."""
    try:
        return int(input_string.strip(strip_string))
    except Exception as e:
        raise MapcheteConfigError("zoom level could not be determined: %s" % e)


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
            if not path[0] in tree:
                tree[path[0]] = _unflatten_tree({"/".join(path[1:]): value})
            # add keys to existing dict
            else:
                branch = _unflatten_tree({"/".join(path[1:]): value})
                if not path[1] in tree[path[0]]:
                    tree[path[0]][path[1]] = branch[path[1]]
                else:
                    tree[path[0]][path[1]].update(branch[path[1]])
    return tree


def _map_to_new_config(config):
    try:
        validate_values(config, [("output", dict)])
    except Exception as e:
        raise MapcheteConfigError(e)

    if "type" in config["output"]:  # pragma: no cover
        warnings.warn(DeprecationWarning("'type' is deprecated and should be 'grid'"))
        if "grid" not in config["output"]:
            config["output"]["grid"] = config["output"].pop("type")

    if "pyramid" not in config:
        warnings.warn(
            DeprecationWarning("'pyramid' needs to be defined in root config element.")
        )
        config["pyramid"] = dict(
            grid=config["output"]["grid"],
            metatiling=config.get("metatiling", 1),
            pixelbuffer=config.get("pixelbuffer", 0),
        )

    if "zoom_levels" not in config:
        warnings.warn(
            DeprecationWarning(
                "use new config element 'zoom_levels' instead of 'process_zoom', "
                "'process_minzoom' and 'process_maxzoom'"
            )
        )
        if "process_zoom" in config:
            config["zoom_levels"] = config["process_zoom"]
        elif all([i in config for i in ["process_minzoom", "process_maxzoom"]]):
            config["zoom_levels"] = dict(
                min=config["process_minzoom"], max=config["process_maxzoom"]
            )
        else:
            raise MapcheteConfigError("process zoom levels not provided in config")

    if "bounds" not in config:
        if "process_bounds" in config:
            warnings.warn(
                DeprecationWarning(
                    "'process_bounds' are deprecated and renamed to 'bounds'"
                )
            )
            config["bounds"] = config["process_bounds"]
        else:
            config["bounds"] = None

    if "input" not in config:
        if "input_files" in config:
            warnings.warn(
                DeprecationWarning(
                    "'input_files' are deprecated and renamed to 'input'"
                )
            )
            config["input"] = config["input_files"]
        else:
            raise MapcheteConfigError("no 'input' found")

    elif "input_files" in config:
        raise MapcheteConfigError(
            "'input' and 'input_files' are not allowed at the same time"
        )

    if "process_file" in config:
        warnings.warn(
            DeprecationWarning("'process_file' is deprecated and renamed to 'process'")
        )
        config["process"] = config.pop("process_file")

    return config


def _guess_geometry(i, base_dir=None):
    """
    Guess and parse geometry if possible.

    - a WKT string
    - a GeoJSON mapping
    - a shapely geometry
    - a path to a Fiona-readable file
    """
    crs = None
    # WKT or path:
    if isinstance(i, str):
        if i.upper().startswith(("POLYGON ", "MULTIPOLYGON ")):
            geom = wkt.loads(i)
        else:
            with fiona.open(absolute_path(path=i, base_dir=base_dir)) as src:
                geom = cascaded_union([shape(f["geometry"]) for f in src])
                crs = src.crs
    # GeoJSON mapping
    elif isinstance(i, dict):
        geom = shape(i)
    # shapely geometry
    elif isinstance(i, BaseGeometry):
        geom = i
    else:
        raise TypeError(
            "area must be either WKT, GeoJSON mapping, shapely geometry or a "
            "Fiona-readable path."
        )
    if not geom.is_valid:  # pragma: no cover
        raise TypeError("area is not a valid geometry")
    try:
        geom = clean_geometry_type(geom, "Polygon", allow_multipart=True)
    except GeometryTypeError:
        raise GeometryTypeError(
            f"area must either be a Polygon or a MultiPolygon, not {geom.geom_type}"
        )
    return geom, crs
