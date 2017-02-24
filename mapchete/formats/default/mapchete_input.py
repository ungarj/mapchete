"""Use another Mapchete process as input."""

from mapchete import Mapchete
from mapchete.config import MapcheteConfig
from mapchete.formats import base
from mapchete.io.vector import reproject_geometry


class InputData(base.InputData):
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
            self.process = Mapchete(MapcheteConfig(self.path, mode="readonly"))
        else:
            raise IOError("no path to .mapchete file provided")

    def open(self, tile, **kwargs):
        """Return InputTile."""
        try:
            return self.process.config.output.open(
                tile, self.process, **kwargs)
        except:
            raise NotImplementedError(
                "output driver from input mapchete does not support reading")

    def bbox(self, out_crs=None):
        """Return data bounding box."""
        return reproject_geometry(
            self.process.config.process_area(),
            src_crs=self.process.config.crs,
            dst_crs=self.pyramid.crs)
