"""Mapchete configuration."""

import os
import yaml
from cached_property import cached_property
from shapely.geometry import box, MultiPolygon
from tilematrix import TilePyramid

from mapchete.formats import (
    load_output_writer, available_output_formats, load_input_reader)
from mapchete.io.raster import RESAMPLING_METHODS


class MapcheteConfig():
    """
    Process configuration.

    MapcheteConfig reads and parses a Mapchete configuration, verifies the
    parameters, creates the necessary metadata required and provides the
    configuration snapshot for every zoom level.

    - input_config: a Mapchete configuration file or a configuration dictionary
    - zoom: process zoom level or a pair of minimum and maximum zoom level
    - bounds: left, bottom, right, top process boundaries in output pyramid
    - overwrite: overwrite existing output data (default: False)
    - single_input_file: single input file if supported by process
    """

    def __init__(
        self, input_config, zoom=None, bounds=None, overwrite=False,
        single_input_file=None
    ):
        """Initialize configuration."""
        # parse configuration
        self.raw, self.mapchete_file, self.config_dir = self._parse_config(
            input_config)

        # see if configuration is empty
        if self.raw is None:
            raise IOError("mapchete configuration is empty")

        # determine input files
        if single_input_file:
            self.raw.update(
                input_files={"input_file": single_input_file}
            )
        elif "input_files" not in self.raw:
            raise IOError("no input file(s) specified")

        # check if mandatory parameters are provided
        for param in _MANDATORY_PARAMETERS:
            try:
                assert param in self.raw
            except AssertionError:
                raise ValueError("%s parameter missing" % param)

        # set process delimiters
        self._delimiters = dict(zoom=zoom, bounds=bounds)

        # helper caches
        self._at_zoom_cache = {}
        self._global_process_area = None
        self.prepared_input_files = {}

        # other properties
        try:
            assert self.raw["output"]["type"] in TILING_TYPES
        except AssertionError:
            raise ValueError("output type (geodetic or mercator) is missing")
        self.process_pyramid = TilePyramid(
            self.raw["output"]["type"], metatiling=self._process_metatiling)
        self.output_pyramid = TilePyramid(
            self.raw["output"]["type"], metatiling=self._output_metatiling)
        self.crs = self.process_pyramid.crs

    @cached_property
    def output(self):
        """Output data object of driver."""
        output_params = self.raw["output"]
        try:
            assert output_params["format"] in available_output_formats()
        except:
            raise ValueError(
                "format %s not available in %s" % (
                    output_params["format"], str(available_output_formats())
                )
            )
        writer = load_output_writer(
            output_params["format"], self.output_pyramid)
        try:
            assert writer.is_valid_with_config(output_params)
        except AssertionError:
            raise ValueError(
                "driver %s not compatible with configuration" %
                writer.driver_name)
        return writer

    @cached_property
    def process_file(self):
        """Absolute path of process file."""
        try:
            abs_path = os.path.join(self.config_dir, self.raw["process_file"])
        except:
            raise Exception("'process_file' parameter is missing")
        try:
            assert os.path.isfile(abs_path)
        except:
            raise IOError("%s is not available" % abs_path)
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
            zoom = [
                self.raw["process_minzoom"], self.raw["process_maxzoom"]]
        else:
            zoom = []
        # overwrite zoom if provided in additional_parameters
        if self._delimiters["zoom"]:
            zoom = self._delimiters["zoom"]
        # if zoom still empty, throw exception
        if not zoom:
            raise Exception("No zoom level(s) provided.")
        if isinstance(zoom, int):
            zoom = [zoom]
        if len(zoom) == 1:
            return zoom
        elif len(zoom) == 2:
            for i in zoom:
                try:
                    assert i >= 0
                except:
                    raise ValueError("Zoom levels must be greater 0.")
            if zoom[0] < zoom[1]:
                return range(zoom[0], zoom[1]+1)
            else:
                return range(zoom[1], zoom[0]+1)
        else:
            raise ValueError(
                "Zoom level parameter requires one or two value(s)."
                )

    @cached_property
    def baselevel(self):
        """Baselevel setting if available."""
        try:
            baselevel = self.raw["baselevel"]
        except KeyError:
            return {}
        try:
            assert "zoom" in baselevel
            assert isinstance(baselevel["zoom"], int)
            assert baselevel["zoom"] > 0
        except AssertionError:
            raise ValueError("no or invalid baselevel zoom parameter given")
        try:
            baselevel["resampling"]
        except KeyError:
            baselevel.update(resampling="nearest")
        try:
            assert baselevel["resampling"] in RESAMPLING_METHODS
        except:
            raise ValueError("invalid baselevel resampling method given")
        return baselevel

    def at_zoom(self, zoom):
        """Return configuration parameters snapshot for zoom as dictionary."""
        if zoom not in self._at_zoom_cache:
            self._at_zoom_cache[zoom] = _at_zoom(self, zoom)
        return self._at_zoom_cache[zoom]

    def process_area(self, zoom=None):
        """Return process bounding box for zoom level."""
        if zoom:
            return _process_area(
                self, self._delimiters["bounds"], zoom)
        else:
            if not self._global_process_area:
                self._global_process_area = MultiPolygon([
                        _process_area(
                            self, self._delimiters["bounds"], z)
                        for z in self.zoom_levels
                    ]).buffer(0)
            return self._global_process_area

    def process_bounds(self, zoom=None):
        """Return process bounds for zoom level."""
        return self.process_area(zoom).bounds

    @cached_property
    def _process_metatiling(self):
        return self._get_metatile_value("process_metatiling")

    @cached_property
    def _output_metatiling(self):
        return self._get_metatile_value("output_metatiling")

    def _parse_config(self, input_config):
        # from configuration dictionary
        if isinstance(input_config, dict):
            raw = input_config
            mapchete_file = None
            try:
                config_dir = input_config["config_dir"]
            except KeyError:
                raise AttributeError("config_dir parameter missing")
        # from Mapchete file
        elif os.path.splitext(input_config)[1] == ".mapchete":
            with open(input_config, "r") as config_file:
                raw = yaml.load(config_file.read())
            mapchete_file = input_config
            config_dir = os.path.dirname(
                os.path.realpath(mapchete_file)
            )
        # throw error if unknown object
        else:
            raise AttributeError(
                "Configuration has to be a dictionary or a .mapchete file."
                )
        return raw, mapchete_file, config_dir

    def _get_metatile_value(self, metatile_key):
        try:
            return self.raw[metatile_key]
        except:
            pass
        try:
            return self.raw["metatiling"]
        except KeyError:
            return 1


def _at_zoom(mapchete_config, zoom):
    """
    Return configuration snapshot at zoom level.

    Input files are handled in a special way. They are returned as their
    respective InputData class.
    """
    params = {}
    input_files = {}
    for name, element in mapchete_config.raw.iteritems():
        if name not in _RESERVED_PARAMETERS:
            out_element = _element_at_zoom(
                mapchete_config, name, element, zoom)
            if out_element is not None:
                params[name] = out_element
        if name == "input_files":
            if element == "from_command_line":
                element = {"input_file": None}
            input_files = {}
            input_files_areas = []
            element_zoom = _element_at_zoom(
                mapchete_config, name, element, zoom)
            try:
                assert isinstance(element_zoom, dict)
            except AssertionError:
                raise RuntimeError("input_files could not be read from config")
            for file_name, file_at_zoom in element_zoom.iteritems():
                if file_at_zoom:
                    file_reader = load_input_reader(
                        input_file=os.path.join(
                            mapchete_config.config_dir, file_at_zoom),
                        pyramid=mapchete_config.process_pyramid)
                    input_files_areas.append(
                        file_reader.bbox(out_crs=mapchete_config.crs))
                    file_at_zoom = file_reader
                input_files[file_name] = file_at_zoom
                # if file_at_zoom:
                #     input_files_areas.append(prepared_file["area"])
            if input_files_areas:
                process_area = MultiPolygon((
                    input_files_areas
                    )).buffer(0)
            else:
                process_area = box(
                    mapchete_config.process_pyramid.left,
                    mapchete_config.process_pyramid.bottom,
                    mapchete_config.process_pyramid.right,
                    mapchete_config.process_pyramid.top
                    )
    params.update(
        input_files=input_files,
        output=mapchete_config.output,
        process_area=process_area
        )
    return params


def _process_area(mapchete_config, user_bounds, zoom):
    """Calculate process bounding box."""
    # process_bounds
    try:
        config_bounds = mapchete_config.raw["process_bounds"]
        bounds = config_bounds
    except KeyError:
        bounds = ()
    # overwrite if bounds are provided explicitly
    if user_bounds:
        # validate bounds
        try:
            assert len(user_bounds) == 4
        except:
            raise ValueError("Invalid number of process bounds.")
        bounds = user_bounds

    input_files_bbox = mapchete_config.at_zoom(zoom)["process_area"]
    if bounds:
        return box(*bounds).intersection(input_files_bbox)
    else:
        return input_files_bbox


def _element_at_zoom(mapchete_config, name, element, zoom):
    """
    Return the element filtered by zoom level.

    - An input integer or float gets returned as is.
    - An input string is checked whether it starts with "zoom". Then, the
      provided zoom level gets parsed and compared with the actual zoom
      level. If zoom levels match, the element gets returned.

    TODOs/gotchas:
    - Elements are unordered, which can lead to unexpected results when
      defining the YAML config.
    - Provided zoom levels for one element in config file are not allowed to
      "overlap", i.e. there is not yet a decision mechanism implemented
      which handles this case.
    """
    # If element is a dictionary, analyze subitems.
    if isinstance(element, dict):
        sub_elements = element
        out_elements = {}
        for sub_name, sub_element in sub_elements.iteritems():
            out_element = _element_at_zoom(
                mapchete_config, sub_name, sub_element, zoom)
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
    elif isinstance(element, int):
        # If element is a number, return as number.
        return element
    else:
        raise RuntimeError("error while parsing configuration")


def _strip_zoom(input_string, strip_string):
    """Return zoom level as integer or throws error."""
    element_zoom = input_string.strip(strip_string)
    try:
        return int(element_zoom)
    except:
        raise SyntaxError("zoom level could not be determined")


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
    "metatiling",  # metatiling setting (for both process and output)
    "process_metatiling",  # process metatiling setting
    "output_metatiling"  # output metatiling setting (for web default 1)
]
