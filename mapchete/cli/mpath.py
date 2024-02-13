import json
from typing import List, Optional, Union

import click

from mapchete.cli import options
from mapchete.path import MPath
from mapchete.pretty import pretty_bytes


@click.group()
def mpath():
    pass


@mpath.command(help="Check whether path exists.")
@options.arg_path
def exists(path: MPath):
    click.echo(path.exists())


@mpath.command(help="List path contents")
@options.arg_path
@click.option(
    "--date-format", type=click.STRING, default="%y-%m-%d %H:%M:%S", show_default=True
)
@click.option("--absolute-paths", is_flag=True)
@click.option("--spacing", type=click.INT, default=4, show_default=True)
def ls(
    path: MPath,
    date_format: str = "%y-%m-%d %H:%M:%S",
    absolute_paths: bool = False,
    spacing: int = 4,
):
    directories = []
    files = []
    last_modified_column_width = len(date_format)
    size_column_width = 0
    for subpath in path.ls(absolute_paths=absolute_paths):
        if subpath.is_directory():
            directories.append(subpath)
        else:
            size_column_width = max(
                len(pretty_bytes(subpath.size())), size_column_width
            )
            files.append(subpath)

    click.echo(
        _row(
            columns=["last modified", "size", "path"],
            widths=[last_modified_column_width, size_column_width, None],
            spacing=spacing,
            underlines=True,
        )
    )

    for subpath in directories:
        click.echo(
            _row(
                columns=[
                    "",
                    "",
                    str(subpath) if absolute_paths else subpath.relative_to(path),
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
                    pretty_bytes(subpath.size()),
                    str(subpath) if absolute_paths else subpath.relative_to(path),
                ],
                widths=[last_modified_column_width, size_column_width, None],
                spacing=spacing,
            )
        )


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

    def _column_underline(text: str = "", width: int = 4, symbol: str = "-"):
        if width is None or len(text) > width:
            width = len(text)
        return symbol * len(text) + " " * (width - len(text))

    space = " " * spacing

    out = space.join([_column(column, width) for column, width in zip(columns, widths)])

    if underlines:
        out += "\n"
        out += space.join(
            [_column_underline(column, width) for column, width in zip(columns, widths)]
        )

    return out


@mpath.command(help="Print contents of file as text.")
@options.arg_path
def read_text(path: MPath):
    try:
        click.echo(path.read_text())
    except Exception as exc:
        raise click.ClickException(exc)


@mpath.command(help="Print contents of file as JSON.")
@options.arg_path
@click.option("--indent", "-i", type=click.INT, default=4)
def read_json(path: MPath, indent: int = 4):
    try:
        click.echo(json.dumps(path.read_json(), indent=indent))
    except Exception as exc:
        raise click.ClickException(exc)
