"""Handles output pyramids using GeoTIFFS."""

import os
import types

from mapchete import BufferedTile
from mapchete.formats import base
from mapchete.io.vector import write_vector_window


class OutputData(base.OutputData):
    """Main output class."""

    METADATA = {
        "driver_name": "GeoJSON",
        "data_type": "vector",
        "mode": "w"
    }

    def __init__(self, output_params):
        """Initialize."""
        super(OutputData, self).__init__(output_params)
        self.path = output_params["path"]
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        self.file_extension = ".geojson"
        self.output_params = output_params

    def write(self, process_tile, overwrite=False):
        """Write process output into GeoTIFFs."""
        if process_tile.data is None:
            return
        assert isinstance(process_tile.data, (list, types.GeneratorType))
        if isinstance(process_tile.data, types.GeneratorType):
            process_tile.data = list(process_tile.data)
        # Convert from process_tile to output_tiles
        for tile in self.pyramid.intersecting(process_tile):
            # skip if file exists and overwrite is not set
            out_path = self.get_path(tile)
            if os.path.exists(out_path) and not overwrite:
                return
            self.prepare_path(tile)
            out_tile = BufferedTile(tile, self.pixelbuffer)
            # write_from_tile(buffered_tile, profile, out_tile, out_path)
            write_vector_window(
                in_tile=process_tile, out_schema=self.output_params["schema"],
                out_tile=out_tile, out_path=out_path)


    def is_valid_with_config(self, config):
        """Check if output format is valid with other process parameters."""
        assert isinstance(config, dict)
        try:
            assert "schema" in config
            assert isinstance(config["schema"], dict)
        except AssertionError:
            raise ValueError("no output schema found or invalid schema")
        try:
            assert "properties" in config["schema"]
            assert isinstance(config["schema"]["properties"], dict)
        except AssertionError:
            raise ValueError(
                "no output properties (feature attribute columns) specified")
        try:
            assert "geometry" in config["schema"]
        except AssertionError:
            raise ValueError("no output geometry type given")
        try:
            assert config["schema"]["geometry"] in [
                "Geometry", "Point", "MultiPoint", "Line", "MultiLine",
                "Polygon", "MultiPolygon"]
        except AssertionError:
            raise ValueError("no invalid output geometry type")
        assert "path" in config
        assert isinstance(config["path"], str)
        try:
            assert config["type"] == "geodetic"
        except AssertionError:
            raise ValueError(
                "GeoJSON output can only be geodetic (EPSG 4326).")
        return True

    def get_path(self, tile):
        """Determine target file path."""
        zoomdir = os.path.join(self.path, str(tile.zoom))
        rowdir = os.path.join(zoomdir, str(tile.row))
        return os.path.join(rowdir, str(tile.col) + self.file_extension)

    def prepare_path(self, tile):
        """Create directory and subdirectory if necessary."""
        zoomdir = os.path.join(self.path, str(tile.zoom))
        if not os.path.exists(zoomdir):
            os.makedirs(zoomdir)
        rowdir = os.path.join(zoomdir, str(tile.row))
        if not os.path.exists(rowdir):
            os.makedirs(rowdir)
