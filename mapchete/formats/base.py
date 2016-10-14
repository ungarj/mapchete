"""
Main base classes for input and output formats.

When writing a new driver, please inherit from these classes and implement
the respective interfaces.
"""

from tilematrix import TilePyramid


class InputData(object):
    """Template class handling geographic input data."""

    METADATA = {
        "driver_name": None,
        "data_type": None,
        "mode": "r"
    }

    def __init__(self, input_params):
        """Initialize relevant input information."""
        self.pyramid = input_params["pyramid"]
        self.pixelbuffer = input_params["pixelbuffer"]
        self.crs = self.pyramid.crs
        self.srid = self.pyramid.srid

    def open(self, tile, **kwargs):
        """Return InputTile class."""
        raise NotImplementedError

    def bbox(self, out_crs=None):
        """Return data bounding box."""
        raise NotImplementedError

    def exists(self):
        """Check if data or file even exists."""
        raise NotImplementedError


class InputTile(object):
    """Target Tile representation of input data."""

    def __init__(self, tile, **kwargs):
        """Initialize."""

    def read(self):
        """Read reprojected & resampled input data."""
        raise NotImplementedError

    def is_emtpy(self):
        """Check if there is data within this tile."""
        raise NotImplementedError

    def __enter__(self):
        """Required for 'with' statement."""
        return self

    def __exit__(self, t, v, tb):
        """Clean up."""
        pass


class OutputData(object):
    """Template class handling process output data."""

    METADATA = {
        "driver_name": None,
        "data_type": None,
        "mode": "w"
    }

    def __init__(self, output_params):
        """Initialize."""
        self.pixelbuffer = output_params["pixelbuffer"]
        self.pyramid = TilePyramid(
            output_params["type"], metatiling=output_params["metatiling"])
        self.crs = self.pyramid.crs
        self.srid = self.pyramid.srid

    @property
    def driver_name(self):
        """Name of driver."""
        raise NotImplementedError

    @property
    def data_type(self):
        """Either 'raster' or 'vector'."""
        raise NotImplementedError

    def write(self, process_tile, overwrite=False):
        """Write data from one or more process tiles."""
        raise NotImplementedError

    def is_valid_with_config(self, config):
        """Check if output format is valid with other process parameters."""
        raise NotImplementedError

    def output_from_process_tile(self, process_tile, for_web=False):
        """Convert process tiles to output tiles."""
        raise NotImplementedError


class OutputTile(object):
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
