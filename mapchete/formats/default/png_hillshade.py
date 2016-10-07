"""PNG process output."""

import os

from mapchete.formats.base import OutputData


class OutputData(OutputData):
    """Main output class."""

    METADATA = {
        "driver_name": "PNG_hillshade",
        "data_type": "raster",
        "mode": "w"
    }

    def __init__(self, output_params):
        """Initialize."""
        super(OutputData, self).__init__(output_params)
        self.path = output_params["path"]
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def write(self, process_tile, data, overwrite=False):
        """Write process output into PNGs."""
        print "write PNG"

    def is_valid_with_config(self, config):
        """Check if output format is valid with other process parameters."""
        assert isinstance(config, dict)
        assert "bands" in config
        assert isinstance(config["bands"], int)
        assert config["bands"] in range(1, 5)
        assert "path" in config
        assert isinstance(config["path"], str)
        return True

    def load(self, output_params):
        """Initialize further properties using configuration."""
        pass
