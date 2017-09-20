"""Errors and Warnings."""


class MapcheteProcessImportError(ImportError):
    """Raised when a module of a mapchete process cannot be imported."""


class MapcheteProcessSyntaxError(SyntaxError):
    """Raised when mapchete process file cannot be imported."""


class MapcheteProcessException(Exception):
    """Raised when a mapchete process execution fails."""


class MapcheteProcessOutputError(ValueError):
    """Raised when a mapchete process output is invalid."""


class MapcheteConfigError(ValueError):
    """Raised when a mapchete process configuration is invalid."""


class MapcheteDriverError(Exception):
    """Raised when a mapchete process configuration is invalid."""


class MapcheteEmptyInputTile(Exception):
    """Generic exception raised by a driver if input tile is empty."""
