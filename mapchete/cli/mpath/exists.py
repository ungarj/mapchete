import click

from mapchete.cli import options
from mapchete.path import MPath


@click.command(help="Check whether path exists.")
@options.arg_path
def exists(path: MPath):
    click.echo(path.exists())
