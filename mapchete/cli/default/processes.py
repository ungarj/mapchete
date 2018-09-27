"""CLI to list processes."""

import click
import pkg_resources
import pydoc


class c:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


@click.command(help="List available processes.")
def processes():
    processes = list(pkg_resources.iter_entry_points("mapchete.processes"))
    print("%s processes found" % len(processes))
    for v in processes:
        process = v.load()
        print(c.BOLD + process.__name__ + c.END)
        # print(process.__doc__)
        # print(pydoc.render_doc(process.execute, "%s"))
        print(process.execute.__doc__)
