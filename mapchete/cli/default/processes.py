"""CLI to list processes."""

import click
import pkg_resources


@click.command(help="List available processes.")
@click.option(
    "--process_name", "-n", type=click.STRING, help="Print docstring of process."
)
@click.option(
    "--docstrings", is_flag=True, help="Print docstrings of all processes."
)
def processes(process_name=None, docstrings=False):

    # get all registered processes
    processes = list(pkg_resources.iter_entry_points("mapchete.processes"))

    # print selected process
    if process_name:
        for v in processes:
            process_module = v.load()
            if process_name == process_module.__name__:
                _print_process_info(process_module, docstrings=True)
    else:
        # print all processes
        click.echo("%s processes found" % len(processes))
        for v in processes:
            process_module = v.load()
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
