#!/usr/bin/env python
"""
This attempt to generalize geoprocesses is strongly inspired by the structure
used by PyWPS:
http://pywps.wald.intevation.org/documentation/course/process/index.html
"""

from collections import OrderedDict
import yaml

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
            if out_element != None:
                out_elements[sub_name] = out_element
        if len(out_elements) == 1:
            return out_elements.itervalues().next()
        return out_elements

    if isinstance(name, str):
        # If element is a zoom level statement, filter element.
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
