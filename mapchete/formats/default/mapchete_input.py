"""Use another Mapchete process as input."""

from mapchete import Mapchete
from mapchete.config import MapcheteConfig
from mapchete.formats.base import InputData
from mapchete.io.vector import reproject_geometry


class InputData(InputData):
    """Main input class."""

    METADATA = {
        "driver_name": "Mapchete",
        "data_type": None,
        "mode": "r",
        "file_extensions": ["mapchete"]
    }

    def __init__(self, input_params):
        """Initialize."""
        super(InputData, self).__init__(input_params)
        self.path = input_params["path"]
        if self.path:
            self.process = Mapchete(MapcheteConfig(self.path))

    def open(self):
        """Return InputTile."""
        raise NotImplementedError

    def bbox(self, out_crs=None):
        """Return data bounding box."""
        return reproject_geometry(
            self.process.config.process_area(),
            src_crs=self.process.config.crs,
            dst_crs=self.pyramid.crs)


class InputTile(InputData):
    """Target Tile representation of input data."""

    def __init__(self):
        """Initialize."""
        raise NotImplementedError
        self.pixelbuffer = None

    def read(self, bands=None):
        """Read reprojected and resampled numpy array for current Tile."""
        raise NotImplementedError
