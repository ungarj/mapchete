import logging

import click
from click_plugins import with_plugins

from mapchete import __version__
from mapchete.registered import mpath_commands

logger = logging.getLogger(__name__)


@with_plugins(mpath_commands)
@click.version_option(version=__version__, message="%(version)s")
@click.group()
def mpath():
    pass
