import logging
from typing import List, Optional, Union

import click

from mapchete.cli import options
from mapchete.path import MPath

logger = logging.getLogger(__name__)


@click.command(help="List path contents.")
@options.arg_path
@options.opt_src_fs_opts
@click.option(
    "--date-format", type=click.STRING, default="%y-%m-%d %H:%M:%S", show_default=True
)
@click.option("--absolute-paths", is_flag=True)
@click.option("--spacing", type=click.INT, default=4, show_default=True)
@click.option("--max-depth", type=click.INT, default=None, show_default=True)
@options.opt_recursive
def ls(
    path: MPath,
    date_format: str = "%y-%m-%d %H:%M:%S",
    absolute_paths: bool = False,
    spacing: int = 4,
    recursive: bool = False,
    max_depth: Optional[int] = None,
    **_,
):
    size_column_width = 10

    def _print_rows(
        directories: List[MPath],
        files: List[MPath],
        last_modified_column_width: int = 0,
        size_column_width: int = 10,
        spacing: int = 4,
    ):
        for subpath in directories:
            click.echo(
                _row(
                    columns=[
                        "",
                        "",
                        (
                            f"{str(subpath)}/"
                            if absolute_paths
                            else f"{subpath.relative_to(path)}/"
                        ),
                    ],
                    widths=[last_modified_column_width, size_column_width, None],
                    spacing=spacing,
                )
            )
        for subpath in files:
            click.echo(
                _row(
                    columns=[
                        subpath.last_modified().strftime(date_format),
                        subpath.pretty_size(),
                        str(subpath) if absolute_paths else subpath.relative_to(path),
                    ],
                    widths=[last_modified_column_width, size_column_width, None],
                    spacing=spacing,
                )
            )

    try:
        last_modified_column_width = len(date_format)
        click.echo(
            _row(
                columns=["last modified", "size", "path"],
                widths=[last_modified_column_width, size_column_width, None],
                spacing=spacing,
                underlines=True,
            )
        )
        if recursive:
            for root, _, files in path.walk(absolute_paths=True, maxdepth=max_depth):
                _print_rows(
                    [root],  # type: ignore
                    files,  # type: ignore
                    last_modified_column_width=last_modified_column_width,
                    spacing=spacing,
                )
        else:
            directories = []
            files = []
            for subpath in path.ls(absolute_paths=True):
                if subpath.is_directory():  # type: ignore
                    directories.append(subpath)
                else:
                    files.append(subpath)
            _print_rows(
                directories,
                files,
                last_modified_column_width=last_modified_column_width,
                spacing=spacing,
            )

    except Exception as exc:  # pragma: no cover
        raise click.ClickException(str(exc))


def _row(
    columns: List[str],
    widths: List[Union[int, None]],
    spacing: int = 4,
    underlines: bool = False,
) -> str:
    def _column(text: str = "", width: Optional[int] = None):
        if width is None or len(text) > width:
            width = len(text)
        return text + " " * (width - len(text))

    def _column_underline(text: str = "", width: int = 4, symbol: str = "-") -> str:
        if width is None or len(text) > width:
            width = len(text)
        return symbol * len(text) + " " * (width - len(text))

    space = " " * spacing

    out = space.join([_column(column, width) for column, width in zip(columns, widths)])

    if underlines:
        out += "\n"
        out += space.join(
            [_column_underline(column, width) for column, width in zip(columns, widths)]  # type: ignore
        )

    return out
