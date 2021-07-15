"""
This package contains easy to access functions which otherwise would have to be called via the CLI.
This should make the use from within other scripts, notebooks, etc. easier.
"""
from mapchete.commands._cp import cp


__all__ = ["cp"]
