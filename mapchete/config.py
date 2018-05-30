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
from copy import deepcopy
import imp
import inspect
import logging
import operator
import os
import py_compile
from shapely.geometry import box
from shapely.ops import cascaded_union
import six
from tilematrix._funcs import Bounds
import warnings
import yaml

from mapchete.formats import (
    load_output_writer, available_output_formats, load_input_reader
)
from mapchete.tile import BufferedTilePyramid
from mapchete.errors import (
    MapcheteConfigError, MapcheteProcessSyntaxError, MapcheteProcessImportError,
    MapcheteDriverError
)


logger = logging.getLogger(__name__)

# parameters whigh have to be provided in the configuration and their types
_MANDATORY_PARAMETERS = [
    ("process_file", six.string_types),  # python file for process code
    ("pyramid", dict),                   # process pyramid definition
    ("input", (dict, type(None))),       # files & other types
    ("output", dict),                    # process output parameters
    ("zoom_levels", (int, dict, list))   # process zoom levels
]

# parameters with special functions which cannot be used for user parameters
_RESERVED_PARAMETERS = [
    "baselevels",       # enable interpolation from other zoom levels
    "bounds",           # process bounds
    "process_file",     # process file with Python code
    "config_dir",       # configuration base directory
    "process_minzoom",  # minimum zoom where process is valid (deprecated)
    "process_maxzoom",  # maximum zoom where process is valid (deprecated)
    "process_zoom",     # single zoom where process is valid (deprecated)
    "process_bounds",   # process boundaries (deprecated)
    "metatiling",       # process metatile size (deprecated)
    "pixelbuffer",      # buffer around each tile in pixels (deprecated)
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
    process_file : string
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
    bounds : tuple
        process bounds
    init_zoom_levels : list
        zoom levels the process configuration was initialized with
    init_bounds : tuple
        bounds the process configuration was initialized with
    baselevels : dictionary
        base zoomlevels, where data is processed; zoom levels not included are
        generated from baselevels

    Deprecated Attributes:
    ----------------------
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
        self, input_config, zoom=None, bounds=None, single_input_file=None,
        mode="continue", debug=False
    ):
        """Initialize configuration."""
        # get dictionary representation of input_config and
        # (0) map deprecated params to new structure
        self._raw = _map_to_new_config(_config_to_dict(input_config))
        self._raw["init_zoom_levels"] = zoom
        self._raw["init_bounds"] = bounds
        self._cache_area_at_zoom = {}
        self._cache_full_process_area = None

        # (1) assert mandatory params are available
        try:
            validate_values(self._raw, _MANDATORY_PARAMETERS)
        except Exception as e:
            raise MapcheteConfigError(e)

        # (2) check .py file
        logger.debug("validating process file")
        self.process_file = _validate_process_file(self._raw)
        self.config_dir = self._raw["config_dir"]
        self.process_func

        # (3) set process and output pyramids
        logger.debug("initializing pyramids")
        try:
            process_metatiling = self._raw["pyramid"].get("metatiling", 1)
            # output metatiling defaults to process metatiling if not set
            # explicitly
            output_metatiling = self._raw["output"].get(
                "metatiling", process_metatiling)
            # we cannot properly handle output tiles which are bigger than
            # process tiles
            if output_metatiling > process_metatiling:
                raise ValueError(
                    "output metatiles must be smaller than process metatiles")
            # these two BufferedTilePyramid instances will help us with all
            # the tile geometries etc.
            self.process_pyramid = BufferedTilePyramid(
                self._raw["pyramid"]["grid"],
                metatiling=process_metatiling,
                pixelbuffer=self._raw["pyramid"].get("pixelbuffer", 0))
            self.output_pyramid = BufferedTilePyramid(
                self._raw["pyramid"]["grid"],
                metatiling=output_metatiling,
                pixelbuffer=self._raw["output"].get("pixelbuffer", 0))
        except Exception as e:
            logger.exception(e)
            raise MapcheteConfigError(e)

        # (4) set mode
        if mode not in ["memory", "continue", "readonly", "overwrite"]:
            raise MapcheteConfigError("unknown mode %s" % mode)
        self.mode = mode

        # (5) prepare process parameters per zoom level without initializing
        # input and output classes
        logger.debug("preparing process parameters")
        self._params_at_zoom = _raw_at_zoom(self._raw, self.init_zoom_levels)

        # (6) initialize output
        logger.debug("initializing output")
        self.output

        # (7) initialize input items
        # depending on the inputs this action takes the longest and is done
        # in the end to let all other actions fail earlier if necessary
        logger.debug("initializing input")
        self.input

    @cached_property
    def zoom_levels(self):
        """Process zoom levels as defined in the configuration."""
        return _validate_zooms(self._raw["zoom_levels"])

    @cached_property
    def init_zoom_levels(self):
        """
        Zoom levels this process is currently initialized with.

        This gets triggered by using the ``zoom`` kwarg. If not set, it will
        be equal to self.zoom_levels.
        """
        return get_zoom_levels(
            process_zoom_levels=self._raw["zoom_levels"],
            init_zoom_levels=self._raw["init_zoom_levels"]
        )

    @cached_property
    def bounds(self):
        """Process bounds as defined in the configuration."""
        if self._raw["bounds"] is None:
            return self.process_pyramid.bounds
        else:
            return Bounds(*_validate_bounds(self._raw["bounds"]))

    @cached_property
    def init_bounds(self):
        """
        Process bounds this process is currently initialized with.

        This gets triggered by using the ``bounds`` kwarg. If not set, it will
        be equal to self.bounds.
        """
        if self._raw["init_bounds"] is None:
            return self.bounds
        else:
            return Bounds(*_validate_bounds(self._raw["init_bounds"]))

    @cached_property
    def output(self):
        """Output object of driver."""
        output_params = self._raw["output"]
        if "path" in output_params:
            output_params.update(
                path=os.path.normpath(
                    os.path.join(self.config_dir, output_params["path"])))
        output_params.update(
            type=self.output_pyramid.grid,
            pixelbuffer=self.output_pyramid.pixelbuffer,
            metatiling=self.output_pyramid.metatiling)
        if "format" not in output_params:
            raise MapcheteConfigError("output format not specified")
        if output_params["format"] not in available_output_formats():
            raise MapcheteConfigError(
                "format %s not available in %s" % (
                    output_params["format"], str(available_output_formats())))
        writer = load_output_writer(output_params)
        try:
            writer.is_valid_with_config(output_params)
        except Exception as e:
            logger.exception(e)
            raise MapcheteConfigError(
                "driver %s not compatible with configuration: %s" % (
                    writer.METADATA["driver_name"], e))
        return writer

    @cached_property
    def input(self):
        """
        Input items used for process stored in a dictionary.

        Keys are the hashes of the input parameters, values the respective
        InputData classes.
        """
        # the delimiters are used by some input drivers
        delimiters = dict(
            zoom=self.init_zoom_levels, bounds=self.init_bounds,
            process_bounds=self.bounds)

        # get input items only of initialized zoom levels
        raw_inputs = {
            # convert input definition to hash
            get_hash(v): v
            for zoom in self.init_zoom_levels
            if "input" in self._params_at_zoom[zoom]
            # to preserve file groups, "flatten" the input tree and use
            # the tree paths as keys
            for key, v in _flatten_tree(self._params_at_zoom[zoom]["input"])
            if v is not None
        }
        initalized_inputs = {}
        for k, v in six.iteritems(raw_inputs):
            if isinstance(v, six.string_types):
                # get absolute paths if not remote
                path = v if v.startswith(
                    ("s3://", "https://", "http://")) else os.path.normpath(
                    os.path.join(self.config_dir, v))
                logger.debug("load input reader for file %s",  v)
                try:
                    reader = load_input_reader(
                        dict(
                            path=deepcopy(path), pyramid=self.process_pyramid,
                            pixelbuffer=self.process_pyramid.pixelbuffer,
                            delimiters=delimiters
                        ), self.mode == "readonly")
                except Exception as e:
                    logger.exception(e)
                    raise MapcheteDriverError(e)
                logger.debug(
                    "input reader for file %s is %s", v, reader)
            # for abstract inputs
            elif isinstance(v, dict):
                logger.debug(
                    "load input reader for abstract input %s", v)
                try:
                    reader = load_input_reader(
                        dict(
                            abstract=deepcopy(v), pyramid=self.process_pyramid,
                            pixelbuffer=self.process_pyramid.pixelbuffer,
                            delimiters=delimiters, conf_dir=self.config_dir
                        ), self.mode == "readonly")
                except Exception as e:
                    logger.exception(e)
                    raise MapcheteDriverError(e)
                logger.debug(
                    "input reader for abstract input %s is %s", v, reader)
            else:
                raise MapcheteConfigError("invalid input type %s", type(v))
            # trigger bbox creation
            reader.bbox(out_crs=self.process_pyramid.crs)
            initalized_inputs[k] = reader
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
        minmax = {
            k: v for k, v in six.iteritems(baselevels) if k in ["min", "max"]}
        if not minmax:
            raise MapcheteConfigError(
                "no min and max values given for baselevels")
        for v in minmax.values():
            if not isinstance(v, int) or v < 0:
                raise MapcheteConfigError(
                    "invalid baselevel zoom parameter given: %s" % (
                        minmax.values()))
        return dict(
            zooms=range(
                minmax.get("min", min(self.zoom_levels)),
                minmax.get("max", max(self.zoom_levels)) + 1),
            lower=baselevels.get("lower", "nearest"),
            higher=baselevels.get("higher", "nearest"),
            tile_pyramid=BufferedTilePyramid(
                self.output_pyramid.grid,
                pixelbuffer=self.output_pyramid.pixelbuffer,
                metatiling=self.process_pyramid.metatiling))

    @cached_property
    def process_func(self):
        try:
            user_process_py = imp.load_source(
                os.path.splitext(os.path.basename(self.process_file))[0],
                self.process_file
            )
            if hasattr(user_process_py, "Process"):
                logger.error(
                    """instanciating MapcheteProcess is deprecated, """
                    """provide execute() function instead""")
            if hasattr(user_process_py, "execute"):
                user_execute = user_process_py.execute
                if len(inspect.getargspec(user_execute).args) == 0:
                    raise ImportError(
                        "execute() function has to accept at least one argument"
                    )
                return user_execute
            else:
                raise ImportError(
                    "No execute() function found in %s" % self.process_file
                )
        except ImportError as e:
            raise MapcheteProcessImportError(e)

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
                "zoom level not available with current configuration")
        out = dict(**self._params_at_zoom[zoom])
        out.update(input={}, output=self.output)
        if "input" in self._params_at_zoom[zoom]:
            flat_inputs = {}
            for k, v in _flatten_tree(self._params_at_zoom[zoom]["input"]):
                if v is None:
                    flat_inputs[k] = None
                else:
                    flat_inputs[k] = self.input[get_hash(v)]
            out["input"] = _unflatten_tree(flat_inputs)
        else:
            out["input"] = {}
        return out

    def area_at_zoom(self, zoom=None):
        """
        Return process bounding box for zoom level.

        Parameters
        ----------
        zoom : int or None
            if None, the union of all zoom level areas is returned

        Returns
        -------
        process area : shapely geometry
        """
        if zoom is None:
            if not self._cache_full_process_area:
                logger.debug("calculate process area ...")
                self._cache_full_process_area = cascaded_union([
                    self._area_at_zoom(z) for z in self.init_zoom_levels]
                ).buffer(0)
            return self._cache_full_process_area
        else:
            if zoom not in self.init_zoom_levels:
                raise ValueError(
                    "zoom level not available with current configuration")
            return self._area_at_zoom(zoom)

    def _area_at_zoom(self, zoom):
        if zoom not in self._cache_area_at_zoom:
            # use union of all input items and, if available, intersect with
            # init_bounds
            if "input" in self._params_at_zoom[zoom]:
                input_union = cascaded_union([
                    self.input[get_hash(v)].bbox(self.process_pyramid.crs)
                    for k, v in six.iteritems(
                        self._params_at_zoom[zoom]["input"])
                    if v is not None
                ])
                self._cache_area_at_zoom[zoom] = input_union.intersection(
                    box(*self.init_bounds)
                ) if self.init_bounds else input_union
            # if no input items are available, just use init_bounds
            else:
                self._cache_area_at_zoom[zoom] = box(*self.init_bounds)
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
        return () if self.area_at_zoom(zoom).is_empty else Bounds(
            *self.area_at_zoom(zoom).bounds)

    # deprecated:
    #############

    @cached_property
    def crs(self):
        """Deprecated."""
        warnings.warn("self.crs is now self.process_pyramid.crs.")
        return self.process_pyramid.crs

    @cached_property
    def metatiling(self):
        """Deprecated."""
        warnings.warn(
            "self.metatiling is now self.process_pyramid.metatiling.")
        return self.process_pyramid.metatiling

    @cached_property
    def pixelbuffer(self):
        """Deprecated."""
        warnings.warn(
            "self.pixelbuffer is now self.process_pyramid.pixelbuffer.")
        return self.process_pyramid.pixelbuffer

    @cached_property
    def inputs(self):
        """Deprecated."""
        warnings.warn("self.inputs renamed to self.input.")
        return self.input

    def at_zoom(self, zoom):
        """Deprecated."""
        warnings.warn("Method renamed to self.params_at_zoom(zoom).")
        return self.params_at_zoom(zoom)

    def process_area(self, zoom=None):
        """Deprecated."""
        warnings.warn("Method renamed to self.area_at_zoom(zoom).")
        return self.area_at_zoom(zoom)

    def process_bounds(self, zoom=None):
        """Deprecated."""
        warnings.warn("Method renamed to self.bounds_at_zoom(zoom).")
        return self.bounds_at_zoom(zoom)


def validate_values(config, values):
    """
    Validate whether value is found in config and has the right type.

    Parameters
    ----------
    config : dict
        configuration dictionary
    values : list
        list of (str, type) tuples of values and value types expected in config

    Returns
    -------
    True if config is valid.

    Raises
    ------
    Exception if value is not found or has the wrong type.
    """
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary")
    for value, vtype in values:
        if value not in config:
            raise ValueError("%s not given" % value)
        if not isinstance(config[value], vtype):
            raise TypeError("%s must be %s" % (value, vtype))
    return True


def get_hash(x):
    """Return hash of x."""
    if isinstance(x, six.string_types):
        return hash(x)
    elif isinstance(x, dict):
        return hash(yaml.dump(x))


def get_zoom_levels(process_zoom_levels=None, init_zoom_levels=None):
    """Validate and return zoom levels."""
    process_zoom_levels = _validate_zooms(process_zoom_levels)
    if init_zoom_levels is None:
        return process_zoom_levels
    else:
        init_zoom_levels = _validate_zooms(init_zoom_levels)
        if not set(init_zoom_levels).issubset(set(process_zoom_levels)):
            raise MapcheteConfigError(
                "init zooms must be a subset of process zoom")
        return init_zoom_levels


def _config_to_dict(input_config):
    if isinstance(input_config, dict):
        if "config_dir" not in input_config:
            raise MapcheteConfigError("config_dir parameter missing")
        return dict(input_config, mapchete_file=None)
    # from Mapchete file
    elif os.path.splitext(input_config)[1] == ".mapchete":
        with open(input_config, "r") as config_file:
            return dict(
                yaml.safe_load(config_file.read()),
                config_dir=os.path.dirname(os.path.realpath(input_config)),
                mapchete_file=input_config)
    # throw error if unknown object
    else:
        raise MapcheteConfigError(
            "Configuration has to be a dictionary or a .mapchete file.")


def _validate_process_file(config):
    abs_path = os.path.join(config["config_dir"], config["process_file"])
    if not os.path.isfile(abs_path):
        raise MapcheteConfigError("%s is not available" % abs_path)
    try:
        py_compile.compile(abs_path, doraise=True)
        imp.load_source(os.path.splitext(os.path.basename(abs_path))[0], abs_path)
    except py_compile.PyCompileError as e:
        raise MapcheteProcessSyntaxError(e)
    except ImportError as e:
        raise MapcheteProcessImportError(e)
    return abs_path


def _validate_zooms(zooms):
    """
    Return a list of zoom levels.

    Following inputs are converted:
    - int --> [int]
    - dict{min, max} --> range(min, max + 1)
    - [int] --> [int]
    - [int, int] --> range(smaller int, bigger int + 1)
    """
    if isinstance(zooms, dict):
        if any([a not in zooms for a in ["min", "max"]]):
            raise MapcheteConfigError("min and max zoom required")
        zmin = _validate_zoom(zooms["min"])
        zmax = _validate_zoom(zooms["max"])
        if zmin > zmax:
            raise MapcheteConfigError(
                "max zoom must not be smaller than min zoom")
        return range(zmin, zmax + 1)
    elif isinstance(zooms, list):
        if len(zooms) == 1:
            return zooms
        elif len(zooms) == 2:
            zmin, zmax = sorted([_validate_zoom(z) for z in zooms])
            return range(zmin, zmax + 1)
        else:
            raise MapcheteConfigError(
                "when providing zooms as list, just min and max are allowed")
    else:
        return [_validate_zoom(zooms)]


def _validate_zoom(zoom):
    """Assert zoom value is positive integer."""
    if any([not isinstance(zoom, int), zoom < 0]):
        raise MapcheteConfigError("zoom must be a positive integer")
    return zoom


def _validate_bounds(bounds):
    if any([
        not isinstance(bounds, (list, tuple)),
        len(bounds) != 4,
        any([not isinstance(i, (int, float)) for i in bounds])
    ]):
        raise MapcheteConfigError("bounds not valid")
    return bounds


def _raw_at_zoom(config, zooms):
    """Return parameter dictionary per zoom level."""
    params_per_zoom = {}
    for zoom in zooms:
        params = {}
        for name, element in six.iteritems(config):
            if name not in _RESERVED_PARAMETERS:
                out_element = _element_at_zoom(name, element, zoom)
                if out_element is not None:
                    params[name] = out_element
        params_per_zoom[zoom] = params
    return params_per_zoom


def _element_at_zoom(name, element, zoom):
        """
        Return the element filtered by zoom level.

        - An input integer or float gets returned as is.
        - An input string is checked whether it starts with "zoom". Then, the
          provided zoom level gets parsed and compared with the actual zoom
          level. If zoom levels match, the element gets returned.
        TODOs/gotchas:
        - Elements are unordered, which can lead to unexpected results when
          defining the YAML config.
        - Provided zoom levels for one element in config file are not allowed
          to "overlap", i.e. there is not yet a decision mechanism implemented
          which handles this case.
        """
        # If element is a dictionary, analyze subitems.
        if isinstance(element, dict):
            if "format" in element:
                # we have an input or output driver here
                return element
            out_elements = {}
            for sub_name, sub_element in six.iteritems(element):
                out_element = _element_at_zoom(sub_name, sub_element, zoom)
                if name == "input":
                    out_elements[sub_name] = out_element
                elif out_element is not None:
                    out_elements[sub_name] = out_element
            # If there is only one subelement, collapse unless it is
            # input. In such case, return a dictionary.
            if len(out_elements) == 1 and name != "input":
                return next(six.itervalues(out_elements))
            # If subelement is empty, return None
            if len(out_elements) == 0:
                return None
            return out_elements
        # If element is a zoom level statement, filter element.
        elif isinstance(name, six.string_types):
            if name.startswith("zoom"):
                return _filter_by_zoom(
                    conf_string=name.strip("zoom").strip(), zoom=zoom,
                    element=element)
            # If element is a string but not a zoom level statement, return
            # element.
            else:
                return element
        # Return all other types as they are.
        else:
            return element


def _filter_by_zoom(element=None, conf_string=None, zoom=None):
    """Return element only if zoom condition matches with config string."""
    for op_str, op_func in [
        ("=", operator.eq),
        ("<=", operator.le),
        (">=", operator.ge),
        ("<", operator.lt),
        (">", operator.gt),
    ]:
        if conf_string.startswith(op_str):
            if op_func(zoom, _strip_zoom(conf_string, op_str)):
                return element
            else:
                # order of operators is important:
                # prematurely return in cases of "<=" or ">=", otherwise
                # _strip_zoom() cannot parse config strings starting with "<"
                # or ">"
                return None


def _strip_zoom(input_string, strip_string):
    """Return zoom level as integer or throw error."""
    try:
        return int(input_string.strip(strip_string))
    except Exception as e:
        raise MapcheteConfigError("zoom level could not be determined: %s" % e)


def _flatten_tree(tree, old_path=None):
    """Flatten dict tree into dictionary where keys are paths of old dict."""
    flat_tree = []
    for key, value in six.iteritems(tree):
        new_path = "/".join([old_path, key]) if old_path else key
        if isinstance(value, dict) and "format" not in value:
            flat_tree.extend(_flatten_tree(value, old_path=new_path))
        else:
            flat_tree.append((new_path, value))
    return flat_tree


def _unflatten_tree(flat):
    """Reverse tree flattening."""
    tree = {}
    for key, value in six.iteritems(flat):
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
    if "pyramid" not in config:
        warnings.warn("'pyramid' needs to be defined in root config element.")
        config["pyramid"] = dict(
            grid=config["output"]["type"],
            metatiling=config.get("metatiling", 1),
            pixelbuffer=config.get("pixelbuffer", 0))
    if "zoom_levels" not in config:
        warnings.warn(
            "use new config element 'zoom_levels' instead of 'process_zoom', "
            "'process_minzoom' and 'process_maxzoom'")
        if "process_zoom" in config:
            config["zoom_levels"] = config["process_zoom"]
        elif all([
            i in config for i in ["process_minzoom", "process_maxzoom"]
        ]):
            config["zoom_levels"] = dict(
                min=config["process_minzoom"],
                max=config["process_maxzoom"])
        else:
            raise MapcheteConfigError(
                "process zoom levels not provided in config")
    if "bounds" not in config:
        if "process_bounds" in config:
            warnings.warn(
                "'process_bounds' are deprecated and renamed to 'bounds'")
            config["bounds"] = config["process_bounds"]
        else:
            config["bounds"] = None
    if "input" not in config:
        if "input_files" in config:
            warnings.warn(
                "'input_files' are deprecated and renamed to 'input'")
            config["input"] = config["input_files"]
        else:
            raise MapcheteConfigError("no 'input' found")
    elif "input_files" in config:
        raise MapcheteConfigError(
            "'input' and 'input_files' are not allowed at the same time")
    return config
