#!/usr/bin/env python
"""
Main class to verify and handle process configurations
"""

import yaml
import os
from shapely.geometry import box
from shapely.ops import cascaded_union

from tilematrix import TilePyramid, MetaTilePyramid
from mapchete import Mapchete
from mapchete.io_utils import MapcheteOutputFormat
from mapchete.io_utils.io_funcs import (reproject_geometry, file_bbox,
    RESAMPLING_METHODS)

_RESERVED_PARAMETERS = [
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

class MapcheteConfig(object):
    """
    Creates a configuration object. As model parameters can change per zoom
    level, the method at_zoom(zoom) returns only the parameters at a given
    zoom level.
    """
    def __init__(
        self,
        config,
        zoom=None,
        bounds=None,
        overwrite=False,
        input_file=None
    ):
        if isinstance(config, dict):
            self._raw_config = config
            self.mapchete_file = None
            self.config_dir = config["config_dir"]
        else:
            # read from mapchete file
            try:
                with open(config, "r") as config_file:
                    self._raw_config = yaml.load(config_file.read())
                self.mapchete_file = config
            except:
                raise
            self.config_dir = os.path.dirname(
                os.path.realpath(self.mapchete_file)
            )
        if input_file:
            self._raw_config.update(
                input_files={"file": input_file}
            )
        self.input_config = config
        try:
            assert self._assert_mandatory_parameters()
        except:
            raise
        # get additional parameters
        self._additional_parameters = {
            "zoom": zoom,
            "bounds": bounds,
        }
        self.process_file = self._get_process_file()
        self.zoom_levels = self._get_zoom_levels()
        self.metatiling = self._get_metatiling()
        self.input_files = self._get_input_files()
        self.process_bounds = self._get_process_bounds(bounds)
        self.baselevel = self._get_baselevel()

        # Validate configuration
        for zoom in self.zoom_levels:
            try:
                assert self.is_valid_at_zoom(zoom)
            except:
                raise ValueError(self.explain_validity_at_zoom(zoom))

        self.overwrite = overwrite
        self.output = MapcheteOutputFormat(
            self._raw_config["output"]
        )

    def process_area(self, zoom):
        """
        Returns area to be processed at zoom.
        """
        tile_pyramid = MetaTilePyramid(
            TilePyramid(self.output.type),
            self.metatiling
        )
        bboxes = []
        for name, path in self.at_zoom(zoom)["input_files"].iteritems():
            if isinstance(path, Mapchete):
                src_bbox = path.config.process_area(zoom)
                if path.config.output.crs == self.output.crs:
                    bbox = src_bbox
                else:
                    bbox = reproject_geometry(
                        src_bbox,
                        src_crs=path.tile_pyramid.crs,
                        dst_crs=tile_pyramid.crs
                        )
            else:
                if name == "cli":
                    bbox = box(
                        tile_pyramid.left,
                        tile_pyramid.bottom,
                        tile_pyramid.right,
                        tile_pyramid.top
                        )
                else:
                    bbox = file_bbox(
                        path,
                        tile_pyramid
                    )

            bboxes.append(bbox)
        files_area = cascaded_union(bboxes)
        out_area = files_area
        if self.process_bounds:
            user_bbox = box(*self.process_bounds)
            out_area = files_area.intersection(user_bbox)
            try:
                assert out_area.geom_type in [
                    "Polygon",
                    "MultiPolygon",
                    "GeometryCollection"
                    ]
            except AssertionError:
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
            if name not in _RESERVED_PARAMETERS:
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
            output=MapcheteOutputFormat(params["output"])
        )
        return params


    def is_valid_at_zoom(self, zoom):
        """
        Checks if mapchete can run using this configuration. Checks
        - the provision of mandatory parameters:
          - input file(s)
        - if input files exist and can be read via Fiona or rasterio
        Returns True or False.
        """
        config = self.at_zoom(zoom)
        try:
            assert "input_files" in config
        except AssertionError:
            return False
        try:
            assert isinstance(config["input_files"], dict)
        except AssertionError:
            return False
        for rel_path in config["input_files"].values():
            if rel_path:
                if isinstance(rel_path, Mapchete):
                    pass
                else:
                    abs_path = os.path.join(self.config_dir, rel_path)
                    try:
                        assert os.path.isfile(os.path.join(abs_path))
                    except AssertionError:
                        return False
        try:
            assert "output" in config
        except AssertionError:
            return False
        return True


    def explain_validity_at_zoom(self, zoom):
        """
        For debugging purposes if is_valid_at_zoom() returns False.
        """
        config = self.at_zoom(zoom)
        try:
            assert "input_files" in config
        except AssertionError:
            return "'input_files' empty for zoom level %s" % zoom
        try:
            assert isinstance(config["input_files"], dict)
        except AssertionError:
            return "'input_files' invalid at zoom level %s: '%s'" %(
                zoom,
                config["input_files"]
                )
        for rel_path in config["input_files"].values():
            if rel_path:
                if isinstance(rel_path, Mapchete):
                    pass
                else:
                    abs_path = os.path.join(self.config_dir, rel_path)
                    try:
                        assert os.path.isfile(os.path.join(abs_path))
                    except AssertionError:
                        return "invalid path '%s'" % abs_path
        try:
            assert "output_name" in config
        except AssertionError:
            return "output_name not provided"
        try:
            assert "output_format" in config
        except AssertionError:
            return "output_format not provided"
        return "everything OK"


    def _get_input_files(self):
        """
        Returns validated, absolute paths or Mapchete process objects from input
        files at zoom.
        """
        if self._raw_config["input_files"] == "cli":
            self._raw_config["input_files"] = {"cli": None}
        all_input_files = self._get_all_items(self._raw_config["input_files"])
        if not all_input_files:
            raise ValueError("no input files specified.")
        abs_paths = {
            input_file: os.path.join(
                self.config_dir,
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
        for entry in raw_config_elem.values():
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
        except KeyError:
            bounds = ()
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
        allowed = ["GTiff", "PNG", "PNG_hillshade", "GeoJSON", "postgis"]
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
        except KeyError:
            metatiling = 1
        try:
            assert metatiling in [1, 2, 4, 8, 16]
        except:
            raise Exception("metatiling must be 1, 2, 4, 8 or 16")
        return metatiling


    def _get_zoom_levels(self):
        """
        Parses zoom levels from raw configuration.
        """
        raw_config = self._raw_config
        try:
            config_zoom = raw_config["process_zoom"]
            zoom = [config_zoom]
        except KeyError:
            zoom = None
            try:
                minzoom = raw_config["process_minzoom"]
                maxzoom = raw_config["process_maxzoom"]
                zoom = [minzoom, maxzoom]
            except KeyError:
                zoom = None
        # overwrite zoom if provided in additional_parameters
        if self._additional_parameters["zoom"]:
            zoom = self._additional_parameters["zoom"]
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
        abs_path = os.path.join(self.config_dir, rel_path)
        mapchete_process_file = abs_path
        try:
            assert os.path.isfile(mapchete_process_file)
        except:
            raise IOError("%s is not available" % mapchete_process_file)
        return mapchete_process_file


    def _get_baselevel(self):
        """
        If provided, reads baselevel parameter and validates subparameters.
        """
        try:
            baselevel = self._raw_config["baselevel"]
        except KeyError:
            return None

        try:
            assert "zoom" in baselevel
            assert isinstance(baselevel["zoom"], int)
            assert baselevel["zoom"] > 0
        except:
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
            # If there is only one subelement, collapse unless it is
            # input_files. In such case, return a dictionary.
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
                if cleaned.startswith("="):
                    name_zoom = self._strip_zoom(cleaned, "=")
                    if zoom == name_zoom:
                        return element
                elif cleaned.startswith("<="):
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
            # If element is a string but not a zoom level statement, return
            # element.
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


    def _assert_mandatory_parameters(self):
        """
        Asserts all mandatory parameters are provided.
        """
        mandatory_parameters = [
            "process_file",
            "input_files",
            "output"
            # "output_name",
            # "output_format",
            # "output_type",
        ]
        diff = set(mandatory_parameters).difference(set(self._raw_config))
        if len(diff) == 0:
            return True
        else:
            raise ValueError("%s parameters missing in %s" %(
                diff,
                self.input_config
                )
            )
