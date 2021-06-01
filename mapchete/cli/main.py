"""Mapchete command line tool with subcommands."""

import click
from click_plugins import with_plugins

from mapchete import __version__
from mapchete._registered import commands


@with_plugins(commands)
@click.version_option(version=__version__, message="%(version)s")
@click.group()
def main():
    pass
