"""
Mapchete command line tool with subcommands.
"""

from pkg_resources import iter_entry_points

import click
from click_plugins import with_plugins


from mapchete import __version__ as version


@with_plugins(iter_entry_points('mapchete.cli.commands'))
@click.version_option(version=version, message='%(version)s')
@click.group()
def main():
    pass
