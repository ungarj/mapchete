"""
This package contains easy to access functions which otherwise would have to be called via the CLI.
This should make the use from within other scripts, notebooks, etc. easier.
"""
from mapchete.commands._convert import convert
from mapchete.commands._cp import cp
from mapchete.commands._execute import execute
from mapchete.commands._index import index
from mapchete.commands._rm import rm


__all__ = ["convert", "cp", "execute", "index", "rm"]
