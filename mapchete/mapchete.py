#!/usr/bin/env python
"""
This attempt to generalize geoprocesses is strongly inspired by the structure
used by PyWPS:
http://pywps.wald.intevation.org/documentation/course/process/index.html
"""

from collections import OrderedDict
import yaml
import rasterio
from shapely.geometry import Polygon
from shapely.ops import cascaded_union

def strip_zoom(input_string, strip_string):
    """
    Returns zoom level as integer or throws error.
    """
    element_zoom = input_string.strip(strip_string)
    try:
        return int(element_zoom)
    except:
        raise SyntaxError

def element_at_zoom(name, element, zoom):
    """
    Returns the element filtered by zoom level.
    - An input integer or float gets returned as is.
    - An input string is checked whether it starts with "zoom". Then, the
      provided zoom level gets parsed and compared with the actual zoom level.
      If zoom levels match, the element gets returned.

    TODOs/gotchas:
    - Elements are unordered, which can lead to unexpected results when defining
      the YAML config.
    - Provided zoom levels for one element in config file are not allowed to
      "overlap", i.e. there is not yet a decision mechanism implemented which
      handles this case.
    """
    # If element is a dictionary, analyze subitems.
    if isinstance(element, dict):
        sub_elements = element
        out_elements = {}
        for sub_name, sub_element in sub_elements.iteritems():
            out_element = element_at_zoom(sub_name, sub_element, zoom)
            if name == "input_files":

                out_elements[sub_name] = out_element
            elif out_element != None:
                out_elements[sub_name] = out_element
        # If there is only one subelement, collapse unless it is input_files.
        # In such case, return a dictionary.
        if len(out_elements) == 1 and name != "input_files":
            return out_elements.itervalues().next()
        # If subelement is empty, return None
        if len(out_elements) == 0:
            return None
        return out_elements

    # If element is a zoom level statement, filter element.
    if isinstance(name, str):
        if name.startswith("zoom"):
            cleaned = name.strip("zoom").strip()
            if cleaned.startswith("<="):
                name_zoom = strip_zoom(cleaned, "<=")
                if zoom <= name_zoom:
                    return element
            elif cleaned.startswith(">="):
                name_zoom = strip_zoom(cleaned, ">=")
                if zoom >= name_zoom:
                    return element
            elif cleaned.startswith("<"):
                name_zoom = strip_zoom(cleaned, "<")
                if zoom < name_zoom:
                    return element
            elif cleaned.startswith(">"):
                name_zoom = strip_zoom(cleaned, ">")
                if zoom > name_zoom:
                    return element
            else:
                return None
        # If element is a string but not a zoom level statement, return element.
        else:
            return element
    else:
        # If element is a number, return as number.
        return element


class MapcheteConfig():
    """
    Creates a configuration object. As model parameters can change per zoom
    level, the method at_zoom(zoom) returns only the parameters at a given
    zoom level.
    """
    def __init__(self, config_path):
        try:
            with open(config_path, "r") as config_file:
                self.config = yaml.load(config_file.read())
        except:
            raise
        self.path = config_path

    def at_zoom(self, zoom):
        """
        Returns the configuration parameters at given zoom level.
        """
        params = {}
        for name, element in self.config.iteritems():
            out_element = element_at_zoom(name, element, zoom)
            if out_element != None:
                params[name] = out_element

        return params

    def is_valid_at_zoom(self, zoom):
        """
        Checks if mapchete can run using this configuration. Checks
        - the provision of mandatory parameters:
          - input file(s)
          - output name
          - output format
        - if input files exist and can be read via Fiona or rasterio
        Returns True or False.
        """
        # TODO
        config = self.at_zoom(zoom)
        try:
            assert "input_files" in config
        except:
            return False
        try:
            assert isinstance(config["input_files"], dict)
        except:
            return False
        for input_file, rel_path in config["input_files"].iteritems():
            if rel_path:
                config_dir = os.path.dirname(os.path.realpath(self.path))
                abs_path = os.path.join(config_dir, rel_path)
                try:
                    assert os.path.isfile(os.path.join(abs_path))
                except:
                    return False
        try:
            assert "output_name" in config
        except:
            return False
        try:
            assert "output_format" in config
        except:
            return False
        return True


    def explain_validity_at_zoom(self, zoom):
        """
        For debugging purposes if is_valid_at_zoom() returns False.
        """
        # TODO
        config = self.at_zoom(zoom)
        try:
            assert "input_files" in config
        except:
            return "'input_files' empty for zoom level %s" % zoom
        try:
            assert isinstance(config["input_files"], dict)
        except:
            return "'input_files' invalid at zoom level %s: '%s'" %(
                zoom,
                config["input_files"]
                )
        for input_file, rel_path in config["input_files"].iteritems():
            if rel_path:
                config_dir = os.path.dirname(os.path.realpath(self.path))
                abs_path = os.path.join(config_dir, rel_path)
                try:
                    assert os.path.isfile(os.path.join(abs_path))
                except:
                    return "invalid path '%s'" % abs_path
        try:
            assert "output_name" in config
        except:
            return "output_name not provided"
        try:
            assert "output_format" in config
        except:
            return "output_format not provided"
        return "everything OK"


class MapcheteProcess():
    """
    Main process class. Needs a Mapchete configuration YAML as input.
    """

    def __init__(self, config_path):
        """
        Process initialization.
        """
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""
        self.config = MapcheteConfig(config_path)


import os

def get_clean_configuration(
    process_file,
    config_yaml,
    zoom=None,
    bounds=None,
    output_path=None,
    output_format=None
    ):
    """
    Reads mapchete configuration file as well as the additional parameters (if
    available) and merges them into a unambiguous and complete set of
    configuration parameters.
    - Additional parameters (e.g. from CLI) always overwrite parameters coming
      from the mapchete configuration file.
    - If any parameter is invalid or not available, an exception is thrown.
    - Configuration parameters are returned as a dictionary.
    """

    mapchete_files = {
        "mapchete_process": process_file,
        "mapchete_config": config_yaml
        }
    additional_parameters = {
       "zoom": zoom,
       "bounds": bounds,
       "output_path": output_path,
       "output_format": output_format
       }

    out_config = {}

    # Analyze input parameters #
    ############################

    ## Check mapchete process file
    try:
        assert os.path.isfile(mapchete_files["mapchete_process"])
    except:
        raise IOError("%s is not available" % mapchete_files["mapchete_process"])
    ## Check mapchete config file
    try:
        assert os.path.isfile(mapchete_files["mapchete_config"])
    except:
        raise IOError("%s is not available" % mapchete_files["mapchete_config"])
    ## Read raw configuration.
    with open(mapchete_files["mapchete_config"], "r") as config_file:
        raw_config = yaml.load(config_file.read())

    ### zoom level(s)
    try:
        config_zoom = raw_config["process_zoom"]
        zoom = [config_zoom]
    except:
        zoom = None
        try:
            minzoom = raw_config["process_minzoom"]
            maxzoom = raw_config["process_maxzoom"]
            zoom = [minzoom, maxzoom]
        except:
            zoom = None
    #### overwrite zoom if provided in additional_parameters
    if additional_parameters["zoom"]:
        zoom = additional_parameters["zoom"]
    #### if zoom still empty, throw exception
    if not zoom:
        raise Exception("No zoom level(s) provided.")
    if len(zoom) == 1:
        zoom_levels = zoom
    elif len(zoom) == 2:
        for i in zoom:
            try:
                assert i>=0
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
    out_config["zoom_levels"] = zoom_levels

    ### check overall validity of mapchete configuration object at zoom levels
    config = MapcheteConfig(mapchete_files["mapchete_config"])
    # TODO in MapcheteConfig
    for zoom in zoom_levels:
        try:
            # checks if input files are valid etc.
            assert config.is_valid_at_zoom(zoom)
        except:
            raise Exception(config.explain_validity_at_zoom(zoom))

    ### process_bounds
    try:
        config_bounds = raw_config["process_bounds"]
        bounds = config_bounds
    except:
        bounds = None
    #### overwrite if bounds are provided explicitly
    if additional_parameters["bounds"]:
        # validate bounds
        try:
            assert len(additional_parameters["bounds"]) == 4
        except:
            raise ValueError("Invalid number of process bounds.")
        bounds = additional_parameters["bounds"]
    #### write bounds for every zoom level
    bounds_per_zoom = {}

    for zoom_level in zoom_levels:
        input_files = config.at_zoom(zoom)["input_files"]
        bboxes = []
        for input_file, rel_path in input_files.iteritems():
            if rel_path:
                config_dir = os.path.dirname(os.path.realpath(config_yaml))
                abs_path = os.path.join(config_dir, rel_path)
                with rasterio.open(abs_path, 'r') as raster:
                    left, bottom, right, top = raster.bounds
                    ul = left, top
                    ur = right, top
                    lr = right, bottom
                    ll = left, bottom
                    bboxes.append(Polygon([ul, ur, lr, ll]))
        files_area = cascaded_union(bboxes)
        out_area = files_area
        if bounds:
            left, bottom, right, top = bounds
            ul = left, top
            ur = right, top
            lr = right, bottom
            ll = left, bottom
            user_bbox = Polygon([ul, ur, lr, ll])
            out_area = files_area.intersection(user_bbox)
            try:
                assert out_area.geom_type in [
                    "Polygon",
                    "MultiPolygon",
                    "GeometryCollection"
                    ]
            except:
                # TODO if process area is empty, remove zoom level from zoom
                # level list
                out_area = Polygon()
        bounds_per_zoom[zoom_level] = out_area
    out_config["process_area"] = bounds_per_zoom

    ### output_path

    ### output_format

    return out_config
