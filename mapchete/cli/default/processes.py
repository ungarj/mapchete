"""CLI to list processes."""

import click
import logging
import pkg_resources

from mapchete.cli import utils

logger = logging.getLogger(__name__)


@click.command(help="List available processes.")
@click.option(
    "--process_name", "-n", type=click.STRING, help="Print docstring of process."
)
@click.option(
    "--docstrings", is_flag=True, help="Print docstrings of all processes."
)
@utils.opt_debug
def processes(process_name=None, docstrings=False, debug=False):
    """List available processes."""
    # get all registered processes
    processes = list(pkg_resources.iter_entry_points("mapchete.processes"))

    # print selected process
    if process_name:
        for v in processes:
            logger.debug("try to load %s", v)
            process_module = v.load()
            if process_name == process_module.__name__:
                _print_process_info(process_module, docstrings=True)
    else:
        # print all processes
        click.echo("%s processes found" % len(processes))
        loaded_processes = [v.load() for v in processes]
        loaded_processes.sort(key=lambda x: x.__name__)
        for process_module in loaded_processes:
            _print_process_info(process_module, docstrings=docstrings)


def _print_process_info(process_module, docstrings=False):
    click.echo(
        click.style(
            process_module.__name__,
            bold=docstrings,
            underline=docstrings
        )
    )
    if docstrings:
        click.echo(process_module.execute.__doc__)
