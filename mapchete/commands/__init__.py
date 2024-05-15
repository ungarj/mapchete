"""
This package contains easy to access functions which otherwise would have to be called via the CLI.
This should make the use from within other scripts, notebooks, etc. easier.
"""

from mapchete.commands.convert import convert
from mapchete.commands.cp import cp
from mapchete.commands.execute import execute
from mapchete.commands.index import index
from mapchete.commands.rm import rm

__all__ = ["convert", "cp", "execute", "index", "rm"]
