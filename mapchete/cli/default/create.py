"""Create dummy Mapchete and python process files."""

import click
from importlib_resources import files
import os
from string import Template
from shutil import copyfile
from oyaml import dump

from mapchete.cli import utils

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


@click.command(help="Create a new process.")
@utils.arg_create_mapchete_file
@utils.arg_process_file
@utils.arg_out_format
@utils.opt_out_path
@utils.opt_pyramid_type
@utils.opt_force
def create(
    mapchete_file,
    process_file,
    out_format,
    out_path=None,
    pyramid_type=None,
    force=False
):
    """Create an empty Mapchete and process file in a given directory."""
    if os.path.isfile(process_file) or os.path.isfile(mapchete_file):
        if not force:
            raise IOError("file(s) already exists")

    out_path = out_path if out_path else os.path.join(os.getcwd(), "output")

    # copy file template to target directory
    # Reads contents with UTF-8 encoding and returns str.
    process_template = str(files("mapchete.static").joinpath("process_template.py"))
    process_file = os.path.join(os.getcwd(), process_file)
    copyfile(process_template, process_file)

    # modify and copy mapchete file template to target directory
    mapchete_template = str(
        files("mapchete.static").joinpath("mapchete_template.mapchete")
    )

    output_options = dict(
        format=out_format, path=out_path, **FORMAT_MANDATORY[out_format]
    )

    pyramid_options = {'grid': pyramid_type}

    substitute_elements = {
        'process_file': process_file,
        'output': dump({'output': output_options}, default_flow_style=False),
        'pyramid': dump({'pyramid': pyramid_options}, default_flow_style=False)
    }
    with open(mapchete_template, 'r') as config_template:
        config = Template(config_template.read())
        customized_config = config.substitute(substitute_elements)
    with open(mapchete_file, 'w') as target_config:
        target_config.write(customized_config)
