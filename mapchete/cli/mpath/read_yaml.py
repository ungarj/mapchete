import click

from mapchete.cli import options
from mapchete.path import MPath


@click.command(help="Print contents of file as YAML.")
@options.arg_path
@options.opt_src_fs_opts
def read_yaml(path: MPath, **_):
    try:
        click.echo(path.read_yaml())
    except Exception as exc:  # pragma: no cover
        raise click.ClickException(str(exc))
