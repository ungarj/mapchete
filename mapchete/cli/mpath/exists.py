import click

from mapchete.cli import options
from mapchete.path import MPath


@click.command(help="Check whether path exists.")
@options.arg_path
@options.opt_src_fs_opts
def exists(path: MPath, **_):
    click.echo(path.exists())
