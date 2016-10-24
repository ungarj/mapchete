"""Mapchete configuration."""

import os
import yaml
from cached_property import cached_property
from shapely.geometry import box, MultiPolygon
from tilematrix import TilePyramid

from mapchete.formats import (
    load_output_writer, available_output_formats, load_input_reader)
from mapchete.io.raster import RESAMPLING_METHODS
from mapchete.tile import BufferedTilePyramid


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
]


class MapcheteConfig(object):
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
            input_config, single_input_file=single_input_file)
        # see if configuration is empty
        if self.raw is None:
            raise IOError("mapchete configuration is empty")
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
        self._prepared_files = {}
        # other properties
        self.output_type = self.raw["output"]["type"]
        try:
            assert self.raw["output"]["type"] in TILING_TYPES
        except AssertionError:
            raise ValueError("output type (geodetic or mercator) is missing")
        self.process_pyramid = BufferedTilePyramid(
            self.output_type, metatiling=self.metatiling,
            pixelbuffer=self.pixelbuffer)
        self.output_pyramid = BufferedTilePyramid(
            self.output_type, metatiling=self.raw["output"]["metatiling"],
            pixelbuffer=self.raw["output"]["pixelbuffer"])
        self.crs = self.process_pyramid.crs
        self.overwrite = overwrite
        self._validate()

    @property
    def output(self):
        """Output data object of driver."""
        output_params = self.raw["output"]
        try:
            assert output_params["format"] in available_output_formats()
        except:
            raise ValueError(
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
        elif all(k in self.raw for k in ("process_minzoom", "process_maxzoom")):
            zoom = [self.raw["process_minzoom"], self.raw["process_maxzoom"]]
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

    @cached_property
    def pixelbuffer(self):
        """Buffer around process tiles."""
        return self.raw["pixelbuffer"]

    @cached_property
    def metatiling(self):
        """Process metatile size."""
        return self.raw["metatiling"]

    def at_zoom(self, zoom):
        """Return configuration parameters snapshot for zoom as dictionary."""
        if zoom not in self._at_zoom_cache:
            self._at_zoom_cache[zoom] = self._at_zoom(zoom)
        return self._at_zoom_cache[zoom]

    def process_area(self, zoom=None):
        """Return process bounding box for zoom level."""
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
        """Return process bounds for zoom level."""
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
                raise AttributeError("config_dir parameter missing")
        # from Mapchete file
        elif os.path.splitext(input_config)[1] == ".mapchete":
            with open(input_config, "r") as config_file:
                raw = yaml.load(config_file.read())
            mapchete_file = input_config
            config_dir = os.path.dirname(os.path.realpath(mapchete_file))
        # throw error if unknown object
        else:
            raise AttributeError(
                "Configuration has to be a dictionary or a .mapchete file."
                )
        # pixelbuffer and metatiling
        raw["pixelbuffer"] = self._set_pixelbuffer(raw)
        raw["output"]["pixelbuffer"] = self._set_pixelbuffer(raw["output"])
        raw["metatiling"] = self._set_metatiling(raw)
        raw["output"]["metatiling"] = self._set_metatiling(
            raw["output"], default=raw["metatiling"])
        try:
            assert raw["metatiling"] >= raw["output"]["metatiling"]
        except AssertionError:
            raise ValueError(
                "Process metatiles cannot be smaller than output metatiles.")
        # absolute output path
        raw["output"].update(
            path=os.path.normpath(os.path.join(
                config_dir, raw["output"]["path"]))
        )
        # determine input files
        if raw["input_files"] == "from_command_line":
            try:
                assert single_input_file
            except AssertionError:
                raise IOError("please provide an input file via command line")
            raw.update(input_files={"input_file": single_input_file})
        elif "input_files" not in raw or raw["input_files"] is None:
            raise IOError("no input file(s) specified")
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

    def _get_metatile_value(self, metatile_key):
        try:
            return self.raw[metatile_key]
        except:
            pass
        try:
            return self.raw["metatiling"]
        except KeyError:
            return 1

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
                if element == "from_command_line":
                    element = {"input_file": None}
                input_files = {}
                input_files_areas = []
                element_zoom = self._element_at_zoom(name, element, zoom)
                try:
                    assert isinstance(element_zoom, dict)
                except AssertionError:
                    raise RuntimeError(
                        "input_files could not be read from config")
                for file_name, file_at_zoom in element_zoom.iteritems():
                    if file_at_zoom:
                        # prepare input files metadata
                        if file_name not in self._prepared_files:
                            # load file reader objects for each file
                            file_reader = load_input_reader(
                                dict(
                                    path=os.path.join(
                                        self.config_dir, file_at_zoom),
                                    pyramid=self.process_pyramid,
                                    pixelbuffer=self.pixelbuffer)
                                )
                            self._prepared_files[file_name] = file_reader
                        # add file reader and file bounding box
                        input_files[file_name] = self._prepared_files[file_name]
                        input_files_areas.append(input_files[file_name].bbox(
                            out_crs=self.crs))
                    else:
                        input_files[file_name] = None
                if input_files_areas:
                    process_area = MultiPolygon((input_files_areas)).buffer(0)
                else:
                    process_area = box(
                        self.process_pyramid.left, self.process_pyramid.bottom,
                        self.process_pyramid.right, self.process_pyramid.top)
        params.update(
            input_files=input_files, output=self.output,
            process_area=process_area)
        return params

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
            except:
                raise ValueError("Invalid number of process bounds.")
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
        - Provided zoom levels for one element in config file are not allowed to
          "overlap", i.e. there is not yet a decision mechanism implemented
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
            raise RuntimeError("error while parsing configuration")


def _strip_zoom(input_string, strip_string):
    """Return zoom level as integer or throws error."""
    try:
        element_zoom = input_string.strip(strip_string)
        return int(element_zoom)
    except:
        raise SyntaxError("zoom level could not be determined")
