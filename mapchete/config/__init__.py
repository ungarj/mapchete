"""
Configuration object required to rund a Mapchete process.

Before running a process, a MapcheteConfig object has to be initialized by
either using a Mapchete file or a dictionary holding the process parameters.
Upon creation, all parameters are validated and the InputData objects are
created which are then exposed to the user process.

An invalid process configuration or an invalid process file cause an Exception
when initializing the configuration.
"""

import os
import yaml
import logging
from cached_property import cached_property
from shapely.geometry import box, MultiPolygon

from mapchete.formats import (
    load_output_writer, available_output_formats, load_input_reader)
from mapchete.tile import BufferedTilePyramid
from mapchete.errors import MapcheteConfigError


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# supported tile pyramid types
TILING_TYPES = ["geodetic", "mercator"]

# parameters to be provided in the process configuration
_MANDATORY_PARAMETERS = [
    "process_file",  # the Python file the process is defined in
    "input_files",  # input files for process; can also be "from_command_line"
    "output"  # dictionary configuring the output format
]

# parameters with special functions which cannot be used for user parameters
_RESERVED_PARAMETERS = [
    "process_minzoom",  # minimum zoom where process is valid
    "process_maxzoom",  # maximum zoom where process is valid
    "process_zoom",  # single zoom where process is valid
    "process_bounds",  # process boundaries
    "metatiling",  # metatile size (for both process and output)
    "pixelbuffer",  # buffer around each tile in pixels
    "baselevels"  # enable interpolation from other zoom levels
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
        process zoom level or a pair of minimum and maximum zoom level
    bounds : tuple
        left, bottom, right, top process boundaries in output pyramid
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
    raw : dictionary
        raw process configuration
    mapchete_file : string
        path to Mapchete file
    config_dir : string
        path to configuration directory
    output_type : string
        process output type (``raster`` or ``vector``)
    process_pyramid : ``tilematrix.TilePyramid``
        ``TilePyramid`` used to process data
    output_pyramid : ``tilematrix.TilePyramid``
        ``TilePyramid`` used to write output data
    crs : ``rasterio.crs.CRS``
        object describing the process coordinate reference system
    output : OutputData
        driver specific output object
    process_file : string
        absolute path to process file
    zoom_levels : list
        valid process zoom levels
    baselevels : dictionary
        base zoomlevels, where data is processed; zoom levels not included are
        generated from baselevels
    pixelbuffer : integer
        buffer around process tiles
    metatiling : integer
        process metatiling
    """

    def __init__(
        self, input_config, zoom=None, bounds=None, single_input_file=None,
        mode="continue"
    ):
        """Initialize configuration."""
        try:
            assert mode in ["memory", "readonly", "continue", "overwrite"]
        except Exception:
            raise MapcheteConfigError("invalid process mode")
        LOGGER.info("preparing configuration ...")
        self.mode = mode
        # parse configuration
        self.raw, self.mapchete_file, self.config_dir = self._parse_config(
            input_config, single_input_file=single_input_file)
        assert self.process_file
        # set process delimiters
        self._delimiters = dict(zoom=zoom, bounds=bounds)
        # helper caches
        self._at_zoom_cache = {}
        self._global_process_area = None
        self._prepared_files = {}
        # other properties
        try:
            self.output_type = self.raw["output"]["type"]
        except KeyError:
            raise MapcheteConfigError("no output type given")
        try:
            assert self.raw["output"]["type"] in TILING_TYPES
        except AssertionError:
            raise MapcheteConfigError(
                "output type (geodetic or mercator) is missing")
        self.process_pyramid = BufferedTilePyramid(
            self.output_type, metatiling=self.metatiling,
            pixelbuffer=self.pixelbuffer)
        self.output_pyramid = BufferedTilePyramid(
            self.output_type, metatiling=self.raw["output"]["metatiling"],
            pixelbuffer=self.raw["output"]["pixelbuffer"])
        self.crs = self.process_pyramid.crs
        self._validate()

    @property
    def output(self):
        """Output object of driver."""
        output_params = self.raw["output"]
        if "format" not in output_params:
            raise MapcheteConfigError("output format not specified")
        try:
            assert output_params["format"] in available_output_formats()
        except AssertionError:
            raise MapcheteConfigError(
                "format %s not available in %s" % (
                    output_params["format"], str(available_output_formats())
                ))
        writer = load_output_writer(output_params)
        try:
            assert writer.is_valid_with_config(output_params)
        except AssertionError, e:
            raise ValueError(
                "driver %s not compatible with configuration: %s" %
                (writer.METADATA["driver_name"], e))
        return writer

    @cached_property
    def process_file(self):
        """Absolute path of process file."""
        abs_path = os.path.join(self.config_dir, self.raw["process_file"])
        try:
            assert os.path.isfile(abs_path)
        except Exception:
            raise MapcheteConfigError("%s is not available" % abs_path)
        return abs_path

    @cached_property
    def zoom_levels(self):
        """Determine valid process zoom levels."""
        # Read from raw configuration.
        if "process_zoom" in self.raw:
            zoom = [self.raw["process_zoom"]]
        elif all(
            k in self.raw for k in ("process_minzoom", "process_maxzoom")
        ):
            zoom = [self.raw["process_minzoom"], self.raw["process_maxzoom"]]
        else:
            zoom = []
        # overwrite zoom if provided in additional_parameters
        if self._delimiters["zoom"]:
            zoom = self._delimiters["zoom"]
        # # if zoom still empty, throw exception
        if not zoom:
            raise MapcheteConfigError("No zoom level(s) provided.")
        if isinstance(zoom, int):
            zoom = [zoom]
        if len(zoom) == 1:
            try:
                assert zoom[0] >= 0
            except Exception:
                raise MapcheteConfigError("Zoom level must be greater 0.")
            return zoom
        elif len(zoom) == 2:
            for i in zoom:
                try:
                    assert i >= 0
                except Exception:
                    raise MapcheteConfigError("Zoom levels must be greater 0.")
            if zoom[0] < zoom[1]:
                return range(zoom[0], zoom[1]+1)
            else:
                return range(zoom[1], zoom[0]+1)
        else:
            raise MapcheteConfigError(
                "Zoom level parameter requires one or two value(s).")

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
        try:
            baselevels = self.raw["baselevels"]
        except KeyError:
            return {}
        try:
            minmax = {
                k: v for k, v in baselevels.iteritems() if k in ["min", "max"]}
            assert minmax
            for v in minmax.values():
                assert v >= 0
                assert isinstance(v, int)
        except Exception as e:
            raise MapcheteConfigError(
                "invalid baselevel zoom parameter given: %s" % e)
        try:
            base_min = minmax["min"]
        except KeyError:
            base_min = min(self.zoom_levels)
        try:
            base_max = minmax["max"]
        except KeyError:
            base_max = max(self.zoom_levels)
        try:
            resampling_lower = baselevels["lower"]
        except KeyError:
            resampling_lower = "nearest"
        try:
            resampling_higher = baselevels["higher"]
        except KeyError:
            resampling_higher = "nearest"
        return dict(
            zooms=range(base_min, base_max+1),
            lower=resampling_lower,
            higher=resampling_higher,
        )

    @cached_property
    def pixelbuffer(self):
        """Buffer around process tiles."""
        return self.raw["pixelbuffer"]

    @cached_property
    def metatiling(self):
        """Process metatile size."""
        return self.raw["metatiling"]

    def at_zoom(self, zoom):
        """
        Return configuration parameters snapshot for zoom as dictionary.

        Parameters
        ----------
        zoom : integer
            zoom level

        Returns
        -------
        configuration snapshot : dictionary
            zoom level dependent process configuration
        """
        if zoom not in self._at_zoom_cache:
            self._at_zoom_cache[zoom] = self._at_zoom(zoom)
        return self._at_zoom_cache[zoom]

    def process_area(self, zoom=None):
        """
        Return process bounding box for zoom level.

        Parameters
        ----------
        zoom : integer or list

        Returns
        -------
        process area : shapely geometry
        """
        if zoom:
            return self._process_area(self._delimiters["bounds"], zoom)
        else:
            if not self._global_process_area:
                self._global_process_area = MultiPolygon([
                        self._process_area(self._delimiters["bounds"], z)
                        for z in self.zoom_levels
                    ]).buffer(0)
            return self._global_process_area

    def process_bounds(self, zoom=None):
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
        return self.process_area(zoom).bounds

    def _validate(self):
        self.process_area()
        for zoom in self.zoom_levels:
            self.at_zoom(zoom)

    def _parse_config(self, input_config, single_input_file):
        # from configuration dictionary
        if isinstance(input_config, dict):
            raw = input_config
            mapchete_file = None
            try:
                config_dir = input_config["config_dir"]
            except KeyError:
                raise MapcheteConfigError("config_dir parameter missing")
        # from Mapchete file
        elif os.path.splitext(input_config)[1] == ".mapchete":
            with open(input_config, "r") as config_file:
                raw = yaml.load(config_file.read())
            mapchete_file = input_config
            config_dir = os.path.dirname(os.path.realpath(mapchete_file))
        # throw error if unknown object
        else:
            raise MapcheteConfigError(
                "Configuration has to be a dictionary or a .mapchete file.")
        # check if mandatory parameters are provided
        for param in _MANDATORY_PARAMETERS:
            try:
                assert param in raw
            except AssertionError:
                raise MapcheteConfigError("%s parameter missing" % param)
        # pixelbuffer and metatiling
        raw["pixelbuffer"] = self._set_pixelbuffer(raw)
        raw["output"]["pixelbuffer"] = self._set_pixelbuffer(raw["output"])
        raw["metatiling"] = self._set_metatiling(raw)
        raw["output"]["metatiling"] = self._set_metatiling(
            raw["output"], default=raw["metatiling"])
        try:
            assert raw["metatiling"] >= raw["output"]["metatiling"]
        except AssertionError:
            raise MapcheteConfigError(
                "Process metatiles cannot be smaller than output metatiles.")
        # absolute output path
        raw["output"].update(
            path=os.path.normpath(os.path.join(
                config_dir, raw["output"]["path"]))
        )
        # determine input files
        if raw["input_files"] == "from_command_line":
            if self.mode in ["memory", "continue", "overwrite"]:
                try:
                    assert single_input_file
                except AssertionError:
                    raise MapcheteConfigError(
                        "please provide an input file via command line")
                raw.update(input_files={"input_file": single_input_file})
        # return parsed configuration
        return raw, mapchete_file, config_dir

    def _set_pixelbuffer(self, config_dict):
        if "pixelbuffer" in config_dict:
            assert isinstance(config_dict["pixelbuffer"], int)
            assert config_dict["pixelbuffer"] >= 0
            return config_dict["pixelbuffer"]
        else:
            return 0

    def _set_metatiling(self, config_dict, default=1):
        if "metatiling" in config_dict:
            assert config_dict["metatiling"] in [1, 2, 4, 8, 16]
            return config_dict["metatiling"]
        else:
            return default

    def _at_zoom(self, zoom):
        """
        Return configuration snapshot at zoom level.

        Input files are handled in a special way. They are returned as their
        respective InputData class.
        """
        params = {}
        input_files = {}
        for name, element in self.raw.iteritems():
            if name not in _RESERVED_PARAMETERS:
                out_element = self._element_at_zoom(name, element, zoom)
                if out_element is not None:
                    params[name] = out_element
            if name == "input_files":
                input_files, process_area = self._input_files_at_zoom(
                    name, element, zoom)
        params.update(
            input_files=input_files, output=self.output,
            process_area=process_area)
        return params

    def _input_files_at_zoom(self, name, element, zoom):
        """Get readers and bounding boxes for input files."""
        if element == "from_command_line":
            element = {"input_file": None}
        files_at_zoom = self._element_at_zoom(name, element, zoom)
        assert isinstance(files_at_zoom, dict)
        input_files, input_files_areas = self._parse_input_files(files_at_zoom)
        if input_files_areas:
            process_area = MultiPolygon((input_files_areas)).buffer(0)
        else:
            process_area = box(
                self.process_pyramid.left, self.process_pyramid.bottom,
                self.process_pyramid.right, self.process_pyramid.top)
        return input_files, process_area

    def _parse_input_files(self, files, prefix=""):
        """Return correctly parsed input file groups."""
        input_files = {}
        input_files_areas = []
        for k, v in files.iteritems():
            # "path" within the .mapchete input_files tree
            lk = "/".join([prefix, k])
            if isinstance(v, dict):
                next_files, next_areas = self._parse_input_files(v, prefix=lk)
                input_files[k] = next_files
                input_files_areas.extend(next_areas)
            elif v not in ["none", "None", None, ""]:
                # prepare input files metadata
                if v not in self._prepared_files:
                    # load file reader objects for each file
                    if v.startswith("s3://"):
                        path = v
                    else:
                        path = os.path.normpath(
                            os.path.join(self.config_dir, v))
                    self._prepared_files[v] = load_input_reader(dict(
                        path=path, pyramid=self.process_pyramid,
                        pixelbuffer=self.pixelbuffer))

                # add file reader and file bounding box
                input_files[k] = self._prepared_files[v]
                input_files_areas.append(input_files[k].bbox(
                    out_crs=self.crs))
            else:
                input_files[k] = None
        return input_files, input_files_areas

    def _process_area(self, user_bounds, zoom):
        """Calculate process bounding box."""
        # process_bounds
        try:
            config_bounds = self.raw["process_bounds"]
            bounds = config_bounds
        except KeyError:
            bounds = ()
        # overwrite if bounds are provided explicitly
        if user_bounds:
            # validate bounds
            try:
                assert len(user_bounds) == 4
            except Exception:
                raise MapcheteConfigError("Invalid number of process bounds.")
            bounds = user_bounds

        input_files_bbox = self.at_zoom(zoom)["process_area"]
        if bounds:
            return box(*bounds).intersection(input_files_bbox)
        else:
            return input_files_bbox

    def _element_at_zoom(self, name, element, zoom):
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
            sub_elements = element
            out_elements = {}
            for sub_name, sub_element in sub_elements.iteritems():
                out_element = self._element_at_zoom(
                    sub_name, sub_element, zoom)
                if name == "input_files":
                    out_elements[sub_name] = out_element
                elif out_element is not None:
                    out_elements[sub_name] = out_element
            # If there is only one subelement, collapse unless it is
            # input_files. In such case, return a dictionary.
            if len(out_elements) == 1 and name != "input_files":
                return out_elements.itervalues().next()
            # If subelement is empty, return None
            if len(out_elements) == 0:
                return None
            return out_elements
        # If element is a zoom level statement, filter element.
        elif isinstance(name, str):
            if name.startswith("zoom"):
                cleaned = name.strip("zoom").strip()
                if cleaned.startswith("="):
                    if zoom == _strip_zoom(cleaned, "="):
                        return element
                elif cleaned.startswith("<="):
                    if zoom <= _strip_zoom(cleaned, "<="):
                        return element
                elif cleaned.startswith(">="):
                    if zoom >= _strip_zoom(cleaned, ">="):
                        return element
                elif cleaned.startswith("<"):
                    if zoom < _strip_zoom(cleaned, "<"):
                        return element
                elif cleaned.startswith(">"):
                    if zoom > _strip_zoom(cleaned, ">"):
                        return element
                else:
                    return None
            # If element is a string but not a zoom level statement, return
            # element.
            else:
                return element
        # If element is a number, return as number.
        elif isinstance(element, int):
            return element
        else:
            raise MapcheteConfigError("error while parsing configuration")


def _strip_zoom(input_string, strip_string):
    """Return zoom level as integer or throws error."""
    try:
        element_zoom = input_string.strip(strip_string)
        return int(element_zoom)
    except Exception:
        raise MapcheteConfigError("zoom level could not be determined")
