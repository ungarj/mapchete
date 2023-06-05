"""CLI to list processes."""

import logging

import click

from mapchete.cli import options
from mapchete.processes import process_names_docstrings

logger = logging.getLogger(__name__)


@click.command(help="List available processes.")
@click.option(
    "--process_name", "-n", type=click.STRING, help="Print docstring of process."
)
@options.opt_debug
def processes(process_name=None, docstrings=False, debug=False):
    """List available processes."""
    processes = process_names_docstrings(process_name=process_name)
    click.echo("%s processes found" % len(processes))
    for process_info in processes:
        _print_process_info(process_info, print_docstring=process_name is not None)


def _print_process_info(process_info, print_docstring=False):
    name, docstring = process_info
    # print process name
    click.echo(click.style(name, bold=print_docstring, underline=print_docstring))
    # print process docstring
    if print_docstring:
        click.echo(docstring)
