#!/usr/bin/env python
"""
This attempt to generalize geoprocesses is strongly inspired by the structure
used by PyWPS:
http://pywps.wald.intevation.org/documentation/course/process/index.html
"""

from collections import OrderedDict
import yaml
import os

from .config_utils import get_clean_configuration

def _strip_zoom(input_string, strip_string):
    """
    Returns zoom level as integer or throws error.
    """
    element_zoom = input_string.strip(strip_string)
    try:
        return int(element_zoom)
    except:
        raise SyntaxError

def _element_at_zoom(name, element, zoom):
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
            out_element = _element_at_zoom(sub_name, sub_element, zoom)
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
                name_zoom = _strip_zoom(cleaned, "<=")
                if zoom <= name_zoom:
                    return element
            elif cleaned.startswith(">="):
                name_zoom = _strip_zoom(cleaned, ">=")
                if zoom >= name_zoom:
                    return element
            elif cleaned.startswith("<"):
                name_zoom = _strip_zoom(cleaned, "<")
                if zoom < name_zoom:
                    return element
            elif cleaned.startswith(">"):
                name_zoom = _strip_zoom(cleaned, ">")
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
            out_element = _element_at_zoom(name, element, zoom)
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

    def __init__(self, config):
        """
        Process initialization.
        """
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""
        self.tile = config["tile"]
        self.tile_pyramid = config["tile_pyramid"]
        zoom, row, col = self.tile
        self.params = config["zoom_levels"][zoom]
        self.config = config
