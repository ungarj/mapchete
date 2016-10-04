"""GeoTIFF process output."""
from mapchete.formats.base import OutputData


class OutputData(OutputData):
    """Main output class."""

    def __init__(self, output_pyramid=None):
        """Initialize."""
        self.driver_name = "GTiff"
        self.data_type = "raster"
        self.mode = "w"

    def write(self, process_tile, data, overwrite=False):
        """Write process output into GeoTIFFs."""
        raise NotImplementedError

    def is_valid_with_config(self, config):
        """Check if output format is valid with other process parameters."""
        assert isinstance(config, dict)
        assert "bands" in config
        assert isinstance(config["bands"], int)
        assert "path" in config
        assert isinstance(config["path"], str)
        assert "dtype" in config
        assert isinstance(config["dtype"], str)
        return True
