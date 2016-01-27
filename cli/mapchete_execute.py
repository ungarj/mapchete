#!/usr/bin/env python

import os
import sys
import argparse
import imp
import yaml

from mapchete import *

def main(args):

    parser = argparse.ArgumentParser()
    parser.add_argument("process", type=str)
    parser.add_argument("config_yaml", type=str)
    parser.add_argument("--zoom", "-z", type=int, nargs='*', )
    parser.add_argument("--bounds", "-b", type=float, nargs='*', default=[None, None, None, None])
    parsed = parser.parse_args(args)

    try:
        mapchete_execute(
            parsed.process,
            parsed.config_yaml,
            zoom=parsed.zoom,
            bounds=parsed.bounds
            )
    except Exception as e:
        #sys.exit(e)
        raise


    # process_name = os.path.splitext(os.path.basename(process_file))[0]
    #
    # # Load source process from python file and initialize.
    # new_process = imp.load_source(process_name + ".Process", process_file)
    # user_defined_process = new_process.Process(config_yaml)
    #
    # print "processing", user_defined_process.identifier
    #
    # # Determine tiles to be processed, depending on:
    # # - zoom level and
    # # - input files bounds OR user defined bounds
    #
    # for zoom in range(0, 13):
    #     print zoom, user_defined_process.execute(zoom)

def mapchete_execute(
    process_file,
    config_yaml,
    zoom=None,
    bounds=None,
    output_path=None,
    output_format=None
    ):
    """
    Executes mapchete process according to configuration.
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

    config = get_clean_configuration(
        mapchete_files,
        additional_parameters
    )

    print config


def get_clean_configuration(
    mapchete_files,
    additional_parameters
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

    out_config = {}

    # Analyze input parameters #
    ############################

    ## Check mapchete process file
    assert os.path.isfile(mapchete_files["mapchete_process"])
    ## Check mapchete config file
    assert os.path.isfile(mapchete_files["mapchete_config"])
    ## Read raw configuration.
    with open(mapchete_files["mapchete_config"], "r") as config_file:
        raw_config = yaml.load(config_file.read())
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
    try:
        config_bounds = raw_config["process_bounds"]
        bounds = config_bounds
    except:
        bounds = None

    ## Check if mandatory parameters are available:

    ### zoom level(s)
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
    # for zoom in zoom_level:
    #     try:
    #         # checks if input files are valid etc.
    #         assert config.is_valid_at_zoom(zoom)
    #     except:
    #         raise Exception(config.explain_validity_at_zoom(zoom))

    ### process_bounds
    #### overwrite bounds if provided in additional_parameters
    if not all(v is None for v in additional_parameters["bounds"]):
        bounds = additional_parameters["bounds"]
    else:
        # TODO read all input files and return union of bounding boxes
        pass
    #### raise execption if bounds are empty
    if all(v is None for v in bounds):
        raise Exception("No process bounds parameters could be found.")
    #### raise exception if one of the bounds values is None
    if None in bounds:
        raise ValueError("Invalid bounds parameter(s).")
    #### raise exception if there are not exactly 4 bounds values
    if not len(additional_parameters["bounds"]) == 4:
        raise ValueError("Invalid number of process bounds.")
    all_bounds = {}
    for zoom_level in zoom_levels:
        all_bounds[zoom_level] = additional_parameters["bounds"]
    out_config["process_bounds"] = all_bounds

    ### output_path

    ### output_format

    return out_config


if __name__ == "__main__":
    main(sys.argv[1:])
