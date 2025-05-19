import json
import click

from mapchete.cli import options
from mapchete.path import MPath


@click.command(help="Print contents of file as JSON.")
@options.arg_path
@click.option("--indent", "-i", type=click.INT, default=4)
def read_json(path: MPath, indent: int = 4):
    try:
        click.echo(json.dumps(path.read_json(), indent=indent))
    except Exception as exc:  # pragma: no cover
        raise click.ClickException(str(exc))
