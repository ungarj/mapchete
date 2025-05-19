import click

from mapchete.cli import options
from mapchete.path import MPath


@click.command(help="Remove path.")
@options.arg_path
@options.opt_src_fs_opts
@options.opt_recursive
@options.opt_force
def rm(path: MPath, recursive: bool = False, force: bool = False, **_):
    try:
        if force or click.confirm(
            f"do you really want to permanently delete {str(path)}?"
        ):
            path.rm(recursive=recursive)
    except Exception as exc:  # pragma: no cover
        raise click.ClickException(str(exc))
