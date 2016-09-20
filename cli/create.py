#!/usr/bin/env python

import os
from string import Template
import pkg_resources
from shutil import copyfile
from yaml import dump

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
    Creates an empty mapchete file as well as an empty process file in a given
    directory. This facilitates starting a new process.
    args should be an argparse Namespace object with:
    - args.process_file: name of the python file
    - args.mapchete_file: name of the mapchete process configuration file
    - args.out_format: optional output format
    - args.out_path: optional output path
    - args.pyramid_type: optional output type
    - args.force: if True, it will replace already existing files
    """
    if (os.path.isfile(args.process_file) or \
        os.path.isfile(args.mapchete_file)) and not args.force:
        raise IOError("file(s) already exists")

    if args.out_path:
        out_path = args.out_path
    else:
        out_path = os.path.join(os.getcwd(), "output")

    # copy process file template to target directory
    process_template = pkg_resources.resource_filename(
        "static", "process_template.py")
    process_file = os.path.join(os.getcwd(), args.process_file)
    copyfile(process_template, process_file)

    # modify and copy mapchete file template to target directory
    mapchete_template = pkg_resources.resource_filename(
        "static", "mapchete_template.mapchete")

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
