"""Errors and Warnings."""


class MapcheteProcessImportError(ImportError):
    """Raised when a module of a mapchete process cannot be imported."""


class MapcheteProcessSyntaxError(SyntaxError):
    """Raised when mapchete process file cannot be imported."""


class MapcheteProcessException(Exception):
    """Raised when a mapchete process execution fails."""


class MapcheteTaskFailed(Exception):
    """Raised when a task fails."""


class MapcheteProcessOutputError(ValueError):
    """Raised when a mapchete process output is invalid."""


class MapcheteConfigError(ValueError):
    """Raised when a mapchete process configuration is invalid."""


class MapcheteDriverError(Exception):
    """Raised on input or output driver errors."""


class MapcheteEmptyInputTile(Exception):
    """Generic exception raised by a driver if input tile is empty."""


class MapcheteNodataTile(Exception):
    """Indicates an empty tile."""


class Empty(MapcheteNodataTile):
    """Short alias for MapcheteNodataTile."""


class GeometryTypeError(TypeError):
    """Raised when geometry type does not fit."""


class MapcheteIOError(IOError):
    """Raised when mapchete cannot read a file."""


class JobCancelledError(Exception):
    """Raised when Job gets cancelled."""


class NoTaskGeometry(TypeError):
    """Raised when Task has no assigned geo information."""


class ReprojectionFailed(RuntimeError):
    """Raised when geometry cannot be reprojected."""


class NoGeoError(AttributeError):
    """Raised when object does not contain geographic information."""


class NoCRSError(AttributeError):
    """Raised when object does not contain a CRS."""
