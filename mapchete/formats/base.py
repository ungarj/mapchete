"""
Main base classes for input and output formats.

When writing a new driver, please inherit from these classes and implement
the respective interfaces.
"""

from tilematrix import Tile


class InputData():
    """Template class handling geographic input data."""

    def __init__(self):
        """Initialize relevant input information."""
        print "InputData initialized"
        self.pixelbuffer = None
        self.crs = None
        self.srid = None
        self.data_type = None  # vector or raster
        self.file_extensions = []  # provide file extensions if applicable

    def open(self):
        """Return InputTile class."""
        raise NotImplementedError

    def bbox(self, out_crs=None):
        """Return data bounding box."""
        raise NotImplementedError

    def exists():
        """Check if data or file even exists."""
        raise NotImplementedError


class InputTile(Tile):
    """Target Tile representation of input data."""

    def __init__(self):
        """Initialize."""
        print "InputTile initialized"
        self.pixelbuffer = None

    def read(self):
        """Read reprojected & resampled input data."""
        raise NotImplementedError

    def is_emtpy(self):
        """Check if there is data within this tile."""
        raise NotImplementedError


class OutputData():
    """Template class handling process output data."""

    def __init__(self, output_pyramid):
        """Initialize."""
        self.driver_name = None
        self.data_type = None
        self.mode = "w"
        self.output_pyramid = output_pyramid
        self.pixelbuffer = None
        self.crs = None
        self.srid = None

    def write(self, process_tile, data, overwrite=False):
        """Write data from one or more process tiles."""
        raise NotImplementedError

    def is_valid_with_config(self, config):
        """Check if output format is valid with other process parameters."""
        raise NotImplementedError

    def output_from_process_tile(self, process_tile, for_web=False):
        """Convert process tiles to output tiles."""
        raise NotImplementedError


class OutputTile(Tile):
    """Represents output for a tile."""

    def __init__(self):
        """Initialize."""
        self.pixelbuffer = None

    def exists():
        """Check if output already exists."""
        raise NotImplementedError

    def read(self):
        """Read reprojected & resampled input data."""
        raise NotImplementedError

    def is_emtpy(self):
        """Check if there is data within this tile."""
        raise NotImplementedError
