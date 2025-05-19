"""Create dummy Mapchete and python process files."""

import os
from shutil import copyfile
from string import Template

import click
import tilematrix
from importlib_resources import files
from oyaml import dump

from mapchete.cli import options
from mapchete.formats import available_output_formats
from mapchete.io import MPath

FORMAT_MANDATORY = {
    "GTiff": {"bands": None, "dtype": None},
    "PNG": {"bands": None, "dtype": None},
    "PNG_hillshade": {"bands": 4, "dtype": "uint8"},
    "GeoJSON": {"schema": {}},
    "PostGIS": {
        "schema": {},
        "db_params": {
            "host": None,
            "port": None,
            "db": None,
            "user": None,
            "password": None,
            "table": None,
        },
    },
}


def _to_mpath(ctx, param, value):
    if value:
        return MPath.from_inp(value)


@click.command(help="Create a new process.")
@click.option("--mapchete-file", type=click.Path(), callback=_to_mpath, prompt=True)
@click.option("--process-file", type=click.STRING, callback=_to_mpath, prompt=True)
@click.option(
    "--out-format", type=click.Choice(available_output_formats()), prompt=True
)
@click.option(
    "--out-path",
    type=click.Path(),
    default=MPath.from_inp("output"),
    help="Output path.",
    prompt=True,
)
@click.option(
    "--pyramid-type",
    "-pt",
    type=click.Choice(tilematrix._conf.PYRAMID_PARAMS.keys()),
    default="geodetic",
    help="Output pyramid type. (default: geodetic)",
    prompt=True,
)
@options.opt_force
def create(
    mapchete_file,
    process_file,
    out_format,
    out_path=None,
    pyramid_type=None,
    force=False,
):
    """Create an empty Mapchete and process file in a given directory."""
    if process_file.exists() or mapchete_file.exists():
        if not force:
            raise IOError("file(s) already exists")

    out_path = out_path if out_path else MPath.from_inp(os.getcwd()) / "output"

    # copy file template to target directory
    # Reads contents with UTF-8 encoding and returns str.
    process_template = str(files("mapchete.static").joinpath("process_template.py"))
    copyfile(process_template, process_file)

    # modify and copy mapchete file template to target directory
    mapchete_template = (
        MPath.from_inp(files("mapchete.static")) / "mapchete_template.mapchete"
    )

    output_options = dict(
        format=out_format, path=str(out_path), **FORMAT_MANDATORY[out_format]
    )

    pyramid_options = {"grid": pyramid_type}

    substitute_elements = {
        "process_file": str(process_file),
        "output": dump({"output": output_options}, default_flow_style=False),
        "pyramid": dump({"pyramid": pyramid_options}, default_flow_style=False),
    }
    config = Template(mapchete_template.read_text())
    customized_config = config.substitute(substitute_elements)
    with mapchete_file.open("w") as target_config:
        target_config.write(customized_config)
