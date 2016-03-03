#!/usr/bin/env python

import os
import yaml
import rasterio
from shapely.geometry import Polygon
from shapely.ops import cascaded_union

from tilematrix import file_bbox, TilePyramid, MetaTilePyramid
import mapchete

def get_clean_configuration(
    mapchete_file,
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
    Minimum structure:
    {
        'metatiling': metatiling value,
        'process_file': '<path/to/process_file>',
        'output_format': 'GTIFF, PNG, ...',
        'output_type': 'geodetic/mercator',
        'output_name': '<output/path>',
        'zoom_levels': {
            5: {
                'process_area': <shapely Polygon>,
                'input_files': {
                    'file2': 'path/to/file',
                    'file1': 'path/to/file'
                },
            6: {
                ...
                }
            }
        }
    }
    """

    additional_parameters = {
        "zoom": zoom,
        "bounds": bounds,
        "output_path": output_path,
        "output_format": output_format
        }

    reserved_parameters = [
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

    out_config = {}

    # Analyze input parameters #
    ############################

    ## Check mapchete config file
    try:
        assert os.path.isfile(mapchete_file)
    except:
        raise IOError("%s is not available" % mapchete_file)
    ## Read raw configuration.
    with open(mapchete_file, "r") as config_file:
        raw_config = yaml.load(config_file.read())

    ## Check mapchete process file
    try:
        mapchete_process_file = raw_config["process_file"]
    except:
        raise Exception("'process_file' parameter is missing")
    rel_path = mapchete_process_file
    config_dir = os.path.dirname(os.path.realpath(mapchete_file))
    abs_path = os.path.join(config_dir, rel_path)
    mapchete_process_file = abs_path
    try:
        assert os.path.isfile(mapchete_process_file)
    except:
        raise IOError("%s is not available" % mapchete_process_file)
    # out_config["process_file"] = mapchete_process_file

    ## TilePyramid SRS
    try:
        output_type = raw_config["output_type"]
    except:
        raise Exception("'output_type' parameter is missing")
    try:
        assert output_type in ["geodetic", "mercator"]
    except:
        raise Exception("'output_type' must be either geodetic or mercator")

    type_crs = {
        "geodetic": {'init': (u'epsg:4326')},
        "mercator": {'init': (u'epsg:3857')},
    }
    output_crs = type_crs[output_type]
    # out_config["output_type"] = output_type

    ## metatiling
    try:
        metatiling = raw_config["metatiling"]
    except:
        metatiling = 1
    try:
        assert metatiling in [1, 2, 4, 8, 16]
    except:
        raise Exception("metatiling must be 1, 2, 4, 8 or 16")
    # out_config["metatiling"] = metatiling

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
    # out_config["zoom_levels"] = zoom_levels

    ### check overall validity of mapchete configuration object at zoom levels
    config = mapchete.MapcheteConfig(mapchete_file)
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
    #### get input files and combined process area for every zoom level
    base_tile_pyramid = TilePyramid(output_type)
    tile_pyramid = MetaTilePyramid(base_tile_pyramid, metatiling)
    bounds_per_zoom = {}
    input_files_per_zoom = {}
    for zoom_level in zoom_levels:
        input_files = config.at_zoom(zoom_level)["input_files"]
        bboxes = []
        abs_input_files = {}
        for input_file, rel_path in input_files.iteritems():
            if rel_path:
                config_dir = os.path.dirname(os.path.realpath(mapchete_file))
                abs_path = os.path.join(config_dir, rel_path)
                if os.path.splitext(abs_path)[1] == ".mapchete":
                    subprocess_config = get_clean_configuration(abs_path)
                    sub_zooms = subprocess_config["zoom_levels"]
                    bbox = sub_zooms[zoom_level]["process_area"]
                else:
                    bbox = file_bbox(
                        abs_path,
                        tile_pyramid
                        )
                bboxes.append(bbox)
            else:
                abs_path = None
            abs_input_files[input_file] = abs_path
        input_files_per_zoom[zoom_level] = abs_input_files
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
                out_area = None
        bounds_per_zoom[zoom_level] = out_area

    out_config["process_file"] = mapchete_process_file
    out_config["output_name"] = raw_config["output_name"]
    out_config["output_format"] = raw_config["output_format"]
    out_config["output_type"] = output_type
    out_config["metatiling"] = metatiling
    out_config["zoom_levels"] = {}
    for zoom_level in zoom_levels:
        if bounds_per_zoom[zoom_level]:
            zoom_config = {}
            zoom_config["process_area"] = bounds_per_zoom[zoom_level]
            zoom_config["input_files"] = input_files_per_zoom[zoom_level]
            for parameter, value in config.at_zoom(zoom).iteritems():
                if parameter in reserved_parameters:
                    pass
                else:
                    zoom_config[parameter] = value
            out_config["zoom_levels"][zoom_level] = zoom_config
        try:
            assert zoom_config["input_files"]
        except:
            raise ValueError("no intersecting input files for this zoom level")


    ### output_path

    ### output_format

    return out_config
