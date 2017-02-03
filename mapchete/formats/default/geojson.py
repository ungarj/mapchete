"""Handles output pyramids using GeoTIFFS."""

import os
import types
import fiona

from mapchete.tile import BufferedTile
from mapchete.formats import base
from mapchete.io.vector import write_vector_window


class OutputData(base.OutputData):
    """Main output class."""

    METADATA = {
        "driver_name": "GeoJSON",
        "data_type": "vector",
        "mode": "rw"
    }

    def __init__(self, output_params):
        """Initialize."""
        super(OutputData, self).__init__(output_params)
        self.path = output_params["path"]
        self.file_extension = ".geojson"
        self.output_params = output_params

    def read(self, output_tile):
        """Read process output."""
        if self.tiles_exist(output_tile):
            with fiona.open(self.get_path(output_tile), "r") as src:
                output_tile.data = list(src)
        else:
            output_tile.data = self.empty(output_tile)
        return output_tile

    def write(self, process_tile):
        """Write process output into GeoTIFFs."""
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        if process_tile.data is None:
            return
        assert isinstance(process_tile.data, (list, types.GeneratorType))
        if isinstance(process_tile.data, types.GeneratorType):
            process_tile.data = list(process_tile.data)
        # Convert from process_tile to output_tiles
        for tile in self.pyramid.intersecting(process_tile):
            # skip if file exists and overwrite is not set
            out_path = self.get_path(tile)
            self.prepare_path(tile)
            out_tile = BufferedTile(tile, self.pixelbuffer)
            write_vector_window(
                in_tile=process_tile, out_schema=self.output_params["schema"],
                out_tile=out_tile, out_path=out_path)

    def tiles_exist(self, process_tile):
        """Check whether all output tiles of a process tile exist."""
        return all(
            os.path.exists(self.get_path(tile))
            for tile in self.pyramid.intersecting(process_tile)
        )

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

    def empty(self, tile):
        """Return emtpy list."""
        return []

    def open(self, tile, process):
        """Open process output as input for other process."""
        return InputTile(tile, process)


class InputTile(base.InputTile):
    """Target Tile representation of output data."""

    def __init__(self, tile, process):
        """Initialize."""
        self.tile = tile
        self.process = process
        self._cache = {}

    def read(self, validity_check=True, no_neighbors=False):
        """
        Read data from process output.

        validity_check: run geometry validity check (default: True)

        Returns a GeoJSON-like list of features.
        """
        if no_neighbors:
            raise NotImplementedError()
        return self._from_cache(validity_check=validity_check)

    def is_empty(self, validity_check=True):
        """Return true if no tiles are available."""
        if self._from_cache(validity_check=validity_check) == []:
            return False
        else:
            return True

    def _from_cache(self, validity_check=True):
        if validity_check not in self._cache:
            self._cache[validity_check] = self.process.get_raw_output(
                self.tile).data
        return self._cache[validity_check]

    def __enter__(self):
        """Enable context manager."""
        return self

    def __exit__(self, t, v, tb):
        """Clear cache on close."""
        del self._cache
