#!/usr/bin/env python

"""Create dummy Mapchete and python process files."""

import os
from string import Template
from shutil import copyfile
from yaml import dump
import pkg_resources

FORMAT_MANDATORY = {
    "GTiff": {
        "bands": None,
        "dtype": None
        },
    "PNG": {
        "bands": None,
        "dtype": None
        },
    "PNG_hillshade": {
        "bands": 4,
        "dtype": "uint8"
        },
    "GeoJSON": {
        "schema": {}
        },
    "PostGIS": {
        "schema": {},
        "db_params": {
            "host": None,
            "port": None,
            "db": None,
            "user": None,
            "password": None,
            "table": None
            }
        }
    }


def create_empty_process(args):
    """
    Create an empty Mapchete and process file in a given directory.

    ``Namespace`` object should have the following attributes:
        * process_file : name of the python file
        * mapchete_file : name of the mapchete process configuration file
        * out_format : optional output format
        * out_path : optional output path
        * pyramid_type : optional output type
        * force : if True, it will replace already existing files

    Parameters
    ----------
    args : argparse.Namespace
    """
    if os.path.isfile(args.process_file) or os.path.isfile(args.mapchete_file):
        if not args.force:
            raise IOError("file(s) already exists")

    out_path = args.out_path if args.out_path else os.path.join(
        os.getcwd(), "output")

    # copy process file template to target directory
    process_template = pkg_resources.resource_filename(
        "mapchete.static", "process_template.py")
    process_file = os.path.join(os.getcwd(), args.process_file)
    copyfile(process_template, process_file)

    # modify and copy mapchete file template to target directory
    mapchete_template = pkg_resources.resource_filename(
        "mapchete.static", "mapchete_template.mapchete")

    output_options = {
        'format': args.out_format,
        'path': out_path,
        'type': args.pyramid_type
        }
    output_options.update(FORMAT_MANDATORY[args.out_format])

    substitute_elements = {
        'process_file': process_file,
        'output': dump({'output': output_options}, default_flow_style=False)
        }
    with open(mapchete_template, 'r') as config_template:
        config = Template(config_template.read())
        customized_config = config.substitute(substitute_elements)
    with open(args.mapchete_file, 'w') as target_config:
        target_config.write(customized_config)
