#!/usr/bin/env python
"""
Main class to verify and handle process configurations
"""

import yaml
import os
from shapely.geometry import box, MultiPolygon
import warnings
from tilematrix import TilePyramid, MetaTilePyramid
from mapchete import Mapchete
from mapchete.io_utils import MapcheteOutputFormat
from mapchete.io_utils.io_funcs import (reproject_geometry, file_bbox,
    RESAMPLING_METHODS)

_RESERVED_PARAMETERS = [
    "process_file",
    "input_files",
    "output",
    "process_minzoom",
    "process_maxzoom",
    "process_zoom",
    "process_bounds",
    "metatiling"
    ]

_MANDATORY_PARAMETERS = [
    "process_file",
    "input_files",
    "output"
]

class MapcheteConfig(object):
    """
    Creates a configuration object. As model parameters can change per zoom
    level, the method at_zoom(zoom) returns only the parameters at a given
    zoom level.
    """
    def __init__(
        self,
        input_config,
        zoom=None,
        bounds=None,
        overwrite=False,
        input_file=None,
        single_input_file=None
        ):
        # Remove in further versions.
        if input_file:
            warnings.warn("MapcheteConfig: input_file parameter deprecated")
            single_input_file = input_file
        # Check input configuration:
        # Dictionary
        if isinstance(input_config, dict):
            self.raw = input_config
            self.mapchete_file = None
            try:
                self.config_dir = input_config["config_dir"]
            except KeyError:
                raise AttributeError("config_dir parameter missing")
        # Mapchete file
        elif os.path.splitext(input_config)[1] == ".mapchete":
            with open(input_config, "r") as config_file:
                self.raw = yaml.load(config_file.read())
            self.mapchete_file = input_config
            self.config_dir = os.path.dirname(
                os.path.realpath(self.mapchete_file)
            )
        # Error
        else:
            raise AttributeError(
                "Configuration has to be a dictionary or a .mapchete file."
                )

        if self.raw == None:
            raise IOError("mapchete file is empty")
        # get input_files
        if single_input_file:
            self.raw.update(
                input_files={"input_file": single_input_file}
            )
        elif not "input_files" in self.raw:
            raise IOError("no input file(s) specified")
        # mandatory parameters
        _validate_mandatory_params(self.raw)
        # additional parameters
        self._additional_parameters = {
            "zoom": zoom,
            "bounds": bounds
        }
        self.overwrite = overwrite
        # helper caches
        self._at_zoom_cache = {}
        self._global_process_area = None
        self.prepared_input_files = {}
        # other properties
        self.tile_pyramid = MetaTilePyramid(
            TilePyramid(self.output.type),
            self.metatiling
            )

    @property
    def process_file(self):
        """
        Absolute path of process file.
        """
        return _process_file(self)

    @property
    def zoom_levels(self):
        """
        Process zoom levels.
        """
        return _zoom_levels(self, self._additional_parameters["zoom"])

    @property
    def metatiling(self):
        """
        Process metatile setting.
        """
        try:
            metatiling = self.raw["metatiling"]
        except KeyError:
            metatiling = 1
        try:
            assert metatiling in [1, 2, 4, 8, 16]
        except:
            raise Exception("metatiling must be 1, 2, 4, 8 or 16")
        return metatiling

    @property
    def baselevel(self):
        """
        Baselevel setting if available.
        """
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

    @property
    def output(self):
        """
        Process output format.
        """
        return MapcheteOutputFormat(self.raw["output"])

    def at_zoom(self, zoom):
        """
        Returns configuration parameters snapshot for zoom as dictionary.
        """
        if not zoom in self._at_zoom_cache:
            self._at_zoom_cache[zoom] = _at_zoom(self, zoom)
        return self._at_zoom_cache[zoom]

    def process_area(self, zoom=None):
        """
        Returns process bounding box for zoom level.
        """
        if zoom:
            return _process_area(self, self._additional_parameters["bounds"],
                zoom)
        else:
            if not self._global_process_area:
                self._global_process_area = MultiPolygon([
                        _process_area(self,
                            self._additional_parameters["bounds"], zoom)
                        for zoom in self.zoom_levels
                    ]).buffer(0)
            return self._global_process_area

    def process_bounds(self, zoom=None):
        """
        Returns process bounds for zoom level.
        """
        return self.process_area(zoom).bounds

def _validate_mandatory_params(config_dict):
    """
    Returns True if all mandatory parameters are available.
    """
    missing = []
    for param in _MANDATORY_PARAMETERS:
        try:
            assert param in config_dict
        except AssertionError:
            missing.append(param)
    try:
        assert missing == []
    except AssertionError:
        raise AttributeError("missing mandatory parameters:", missing)

def _process_file(mapchete_config):
    """
    Absolute path of process file.
    """
    try:
        mapchete_process_file = mapchete_config.raw["process_file"]
    except:
        raise Exception("'process_file' parameter is missing")
    abs_path = os.path.join(mapchete_config.config_dir, mapchete_process_file)
    try:
        assert os.path.isfile(abs_path)
    except:
        raise IOError("%s is not available" % abs_path)
    return abs_path

def _zoom_levels(mapchete_config, user_zoom):
    """
    Process zoom levels.
    """
    # read from raw configuration
    try:
        config_zoom = mapchete_config.raw["process_zoom"]
        zoom = [config_zoom]
    except KeyError:
        zoom = None
        try:
            minzoom = mapchete_config.raw["process_minzoom"]
            maxzoom = mapchete_config.raw["process_maxzoom"]
            zoom = [minzoom, maxzoom]
        except KeyError:
            zoom = None
    # overwrite zoom if provided in additional_parameters
    if user_zoom:
        zoom = user_zoom
    # if zoom still empty, throw exception
    if not zoom:
        raise Exception("No zoom level(s) provided.")
    if isinstance(zoom, int):
        zoom = [zoom]
    if len(zoom) == 1:
        zoom_levels = zoom
    elif len(zoom) == 2:
        for i in zoom:
            try:
                assert i >= 0
            except:
                raise ValueError("Zoom levels must be greater 0.")
        if zoom[0] < zoom[1]:
            minzoom = zoom[0]
            maxzoom = zoom[1]
        else:
            minzoom = zoom[1]
            maxzoom = zoom[0]
        zoom_levels = range(minzoom, maxzoom+1)
    else:
        raise ValueError(
            "Zoom level parameter requires one or two value(s)."
            )
    return zoom_levels

def _process_area(mapchete_config, user_bounds, zoom):
    """
    Calculates process bounding box.
    """
    ### process_bounds
    try:
        config_bounds = mapchete_config.raw["process_bounds"]
        bounds = config_bounds
    except KeyError:
        bounds = ()
    #### overwrite if bounds are provided explicitly
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

def _at_zoom(mapchete_config, zoom):
    """
    Returns configuration snapshot at zoom level.
    """
    params = {}
    input_files = {}
    for name, element in mapchete_config.raw.iteritems():
        if name not in _RESERVED_PARAMETERS:
            out_element = _element_at_zoom(mapchete_config, name, element, zoom)
            if out_element != None:
                params[name] = out_element
        if name == "input_files":
            if element == "from_command_line":
                element = {"input_file": None}
            input_files = {}
            input_files_areas = []
            element_zoom = _element_at_zoom(mapchete_config, name,
                element, zoom)
            try:
                assert isinstance(element_zoom, dict)
            except AssertionError:
                raise RuntimeError("input_files could not be read from config")
            for file_name, file_at_zoom in element_zoom.iteritems():
                prepared_file = _prepared_file(mapchete_config, file_at_zoom)
                input_files[file_name] = prepared_file["file"]
                if file_at_zoom:
                    input_files_areas.append(prepared_file["area"])
            if input_files_areas:
                process_area = MultiPolygon((
                    input_files_areas
                    )).buffer(0)
            else:
                process_area = box(
                    mapchete_config.tile_pyramid.left,
                    mapchete_config.tile_pyramid.bottom,
                    mapchete_config.tile_pyramid.right,
                    mapchete_config.tile_pyramid.top
                    )
    params.update(
        input_files=input_files,
        output=mapchete_config.output,
        process_area=process_area
        )
    return params

def _element_at_zoom(mapchete_config, name, element, zoom):
    """
    Returns the element filtered by zoom level.
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
            out_element = _element_at_zoom(mapchete_config, sub_name,
                sub_element, zoom)
            if name == "input_files":
                out_elements[sub_name] = out_element
            elif out_element != None:
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
    """
    Returns zoom level as integer or throws error.
    """
    element_zoom = input_string.strip(strip_string)
    try:
        return int(element_zoom)
    except:
        raise SyntaxError("zoom level could not be determined")

def _prepared_file(mapchete_config, input_file):
    """
    Returns validated, absolute paths or Mapchete process objects from input
    files at zoom.
    """
    if not input_file in mapchete_config.prepared_input_files:
        if not input_file:
            prepared = {
                "file": None,
                "area": None
                }
        else:
            abs_path = os.path.join(mapchete_config.config_dir, input_file)
            try:
                assert os.path.isfile(abs_path)
            except AssertionError:
                raise IOError("no such file", abs_path)

            if os.path.splitext(abs_path)[1] == ".mapchete":
                mapchete_file = Mapchete(MapcheteConfig(abs_path))
                prepared = {
                    "file": mapchete_file,
                    "area": reproject_geometry(
                        mapchete_file.config.process_area(),
                        mapchete_file.tile_pyramid.crs,
                        mapchete_config.tile_pyramid.crs
                        )
                    }
            else:
                prepared = {
                    "file": abs_path,
                    "area": file_bbox(abs_path, mapchete_config.tile_pyramid)
                    }

        mapchete_config.prepared_input_files[input_file] = prepared

    return mapchete_config.prepared_input_files[input_file]
