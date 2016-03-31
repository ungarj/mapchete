#!/usr/bin/env python

import yaml
import os
from shapely.geometry import Polygon
from shapely.ops import cascaded_union

from tilematrix import TilePyramid, MetaTilePyramid, file_bbox

from .mapchete import Mapchete

_reserved_parameters = [
    "process_file",
    "input_files",
    "output_name",
    "output_format",
    "output_type",
    "process_minzoom",
    "process_maxzoom",
    "process_zoom",
    "process_bounds",
    "metatiling"
    ]

class MapcheteConfig():
    """
    Creates a configuration object. As model parameters can change per zoom
    level, the method at_zoom(zoom) returns only the parameters at a given
    zoom level.
    """
    def __init__(
        self,
        mapchete_file,
        zoom=None,
        bounds=None,
        output_path=None,
        output_format=None
    ):
        # read from mapchete file
        try:
            with open(mapchete_file, "r") as config_file:
                self._raw_config = yaml.load(config_file.read())
        except:
            raise
        # get additional parameters
        self._additional_parameters = _additional_parameters = {
            "zoom": zoom,
            "bounds": bounds,
            "output_path": output_path,
            "output_format": output_format
        }
        self.mapchete_file = mapchete_file
        self.process_file = self._get_process_file()
        self.zoom_levels = self._get_zoom_levels()
        self.output_type = self._get_output_type()
        self.output_crs = self._get_output_crs()
        self.output_format = self._get_output_format()
        self.metatiling = self._get_metatiling()
        self.input_files = self._get_input_files()
        self.process_bounds = self._get_process_bounds(bounds)
        self.output_name = self._raw_config["output_name"]
        # TODO add checks & proper dtype
        self.output_bands = self._raw_config["output_bands"]
        self.output_dtype = self._raw_config["output_dtype"]
        if self._raw_config["write_options"]:
            print "herbert"
            self.write_options = self._raw_config["write_options"]
        else:
            self.write_options = None
        # Validate configuration
        for zoom in self.zoom_levels:
            try:
                assert self.is_valid_at_zoom(zoom)
            except:
                raise ValueError(self.explain_validity_at_zoom(zoom))


    def process_area(self, zoom):
        """
        Returns area to be processed at zoom.
        """
        tile_pyramid = MetaTilePyramid(
            TilePyramid(self.output_type),
            self.metatiling
        )
        bboxes = []
        for name, path in self.at_zoom(zoom)["input_files"].iteritems():
            if isinstance(path, Mapchete):
                bbox = path.config.process_area(zoom)
            else:
                bbox = file_bbox(
                    path,
                    tile_pyramid
                    )
            bboxes.append(bbox)
        files_area = cascaded_union(bboxes)
        out_area = files_area
        if self.process_bounds:
            left, bottom, right, top = self.process_bounds
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
                out_area = None
        return out_area


    def at_zoom(self, zoom):
        """
        Returns the processed configuration parameters at given zoom level.
        """
        params = {}
        for name, element in self._raw_config.iteritems():
            if name not in _reserved_parameters:
                out_element = self._element_at_zoom(name, element, zoom)
                if out_element != None:
                    params[name] = out_element
        input_files = {}
        for name, path in self._raw_at_zoom(zoom)["input_files"].iteritems():
            if path == None:
                input_files[name] = path
            else:
                input_files[name] = self.input_files[name]
        params.update(
            input_files=input_files,
            output_name=self.output_name,
            output_format=self.output_format
        )
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
                if isinstance(rel_path, Mapchete):
                    pass
                else:
                    config_dir = os.path.dirname(
                        os.path.realpath(self.mapchete_file)
                        )
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
                if isinstance(rel_path, Mapchete):
                    pass
                else:
                    config_dir = os.path.dirname(
                        os.path.realpath(self.mapchete_file)
                        )
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


    def _get_input_files(self):
        """
        Returns validated, absolute paths or Mapchete process objects from input
        files at zoom.
        """
        all_input_files = self._get_all_items(self._raw_config["input_files"])
        abs_paths = {
            input_file: os.path.join(
                os.path.dirname(os.path.realpath(self.mapchete_file)),
                rel_path
            )
            for input_file, rel_path in all_input_files.iteritems()
            if rel_path
        }
        input_files = {}
        for input_file, abs_path in abs_paths.iteritems():
            if os.path.splitext(abs_path)[1] == ".mapchete":
                input_files[input_file] = Mapchete(MapcheteConfig(abs_path))
            else:
                input_files[input_file] = abs_path
        return input_files


    def _get_all_items(self, raw_config_elem):
        """
        Returns a dictionary of configuration items without zoom filters.
        """
        out_elem = {}
        for name, entry in raw_config_elem.iteritems():
            if isinstance(entry, dict):
                out_elem[name] = self._get_final_item(entry)
            else:
                out_elem[name] = entry
        return out_elem


    def _get_final_item(self, raw_config_elem):
        """
        Returns the last item of a dict tree.
        """
        out_elem = {}
        for name, entry in raw_config_elem.iteritems():
            if isinstance(entry, dict):
                return self._get_final_item(entry)
            else:
                return entry


    def _get_process_bounds(self, bounds):
        """
        Calculates process bounds.
        """
        raw_config = self._raw_config
        ### process_bounds
        try:
            config_bounds = raw_config["process_bounds"]
            bounds = config_bounds
        except:
            bounds = None
        #### overwrite if bounds are provided explicitly
        if self._additional_parameters["bounds"]:
            # validate bounds
            try:
                assert len(self._additional_parameters["bounds"]) == 4
            except:
                raise ValueError("Invalid number of process bounds.")
            bounds = self._additional_parameters["bounds"]
        return bounds


    def _get_output_format(self):
        """
        Validate and return output format
        """
        output_format = self._raw_config["output_format"]
        allowed = ["GTiff", "PNG", "PNG_hillshade"]
        try:
            assert output_format in allowed
        except:
            raise ValueError("invalid output format %s" % output_format)
        return output_format


    def _get_metatiling(self):
        """
        Gets and checks metatiling value
        """
        try:
            metatiling = self._raw_config["metatiling"]
        except:
            metatiling = 1
        try:
            assert metatiling in [1, 2, 4, 8, 16]
        except:
            raise Exception("metatiling must be 1, 2, 4, 8 or 16")
        return metatiling


    def _get_output_crs(self):
        """
        Returns CRS dict.
        """
        type_crs = {
            "geodetic": {'init': (u'epsg:4326')},
            "mercator": {'init': (u'epsg:3857')},
        }
        return type_crs[self.output_type]


    def _get_output_type(self):
        """
        Gets tile pyramid type (either geodetic or mercator).
        """
        raw_config = self._raw_config
        try:
            output_type = raw_config["output_type"]
        except:
            raise Exception("'output_type' parameter is missing")
        try:
            assert output_type in ["geodetic", "mercator"]
        except:
            raise Exception("'output_type' must be either geodetic or mercator")
        return output_type


    def _get_zoom_levels(self):
        """
        Parses zoom levels from raw configuration.
        """
        raw_config = self._raw_config
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
        # overwrite zoom if provided in additional_parameters
        if self._additional_parameters["zoom"]:
            zoom = self._additional_parameters["zoom"]
        # if zoom still empty, throw exception
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
        return zoom_levels


    def _get_process_file(self):
        """
        Gets mapchete process file or raises Exception if it doesn't exist.
        """
        raw_config = self._raw_config
        try:
            mapchete_process_file = raw_config["process_file"]
        except:
            raise Exception("'process_file' parameter is missing")
        rel_path = mapchete_process_file
        config_dir = os.path.dirname(os.path.realpath(self.mapchete_file))
        abs_path = os.path.join(config_dir, rel_path)
        mapchete_process_file = abs_path
        try:
            assert os.path.isfile(mapchete_process_file)
        except:
            raise IOError("%s is not available" % mapchete_process_file)
        return mapchete_process_file


    # configuration parsing functions #
    ###################################
    def _strip_zoom(self, input_string, strip_string):
        """
        Returns zoom level as integer or throws error.
        """
        element_zoom = input_string.strip(strip_string)
        try:
            return int(element_zoom)
        except:
            raise SyntaxError


    def _element_at_zoom(self, name, element, zoom):
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
                out_element = self._element_at_zoom(sub_name, sub_element, zoom)
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
                    name_zoom = self._strip_zoom(cleaned, "<=")
                    if zoom <= name_zoom:
                        return element
                elif cleaned.startswith(">="):
                    name_zoom = self._strip_zoom(cleaned, ">=")
                    if zoom >= name_zoom:
                        return element
                elif cleaned.startswith("<"):
                    name_zoom = self._strip_zoom(cleaned, "<")
                    if zoom < name_zoom:
                        return element
                elif cleaned.startswith(">"):
                    name_zoom = self._strip_zoom(cleaned, ">")
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


    def _raw_at_zoom(self, zoom):
        """
        Returns the raw configuration parameters at given zoom level.
        """
        params = {}
        for name, element in self._raw_config.iteritems():
            out_element = self._element_at_zoom(name, element, zoom)
            if out_element != None:
                params[name] = out_element
        return params
