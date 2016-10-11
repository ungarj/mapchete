"""Mapchete."""

import os
import py_compile
import logging
import logging.config
import traceback
import imp
import numpy as np
import numpy.ma as ma
from collections import namedtuple
from cached_property import cached_property
from tilematrix import Tile

import mapchete

LOGGER = logging.getLogger("mapchete")


class Mapchete(object):
    """
    Main entry point to every processing job.

    From here, the process tiles can be determined and executed.
    """

    def __init__(self, config):
        """Initialize Mapchete job."""
        self.config = config
        self.output = config.output
        try:
            py_compile.compile(self.config.process_file, doraise=True)
        except:
            raise
        self.process_name = os.path.splitext(
            os.path.basename(self.config.process_file)
        )[0]

    def get_process_tiles(self, zoom=None):
        """
        Return process tiles.

        Tiles intersecting with the input data bounding boxes as well as
        process bounds, if provided, are considered process tiles. This is to
        avoid iterating through empty tiles.

        - zoom: zoom level
        """
        try:
            if zoom or zoom == 0:
                assert isinstance(zoom, int)
                bbox = self.config.process_area(zoom)
                for tile in self.config.process_pyramid.tiles_from_geom(
                    bbox, zoom
                ):
                    yield tile
            else:
                for zoom in reversed(self.config.zoom_levels):
                    bbox = self.config.process_area(zoom)
                    for tile in self.config.process_pyramid.tiles_from_geom(
                        bbox, zoom
                    ):
                        yield tile
        except Exception:
            LOGGER.error(
                "error getting work tiles: %s" % traceback.print_exc())
            raise

    def execute(self, process_tile, overwrite=True):
        """
        Run the Mapchete process and write output.

        Execute, write and return process_tile with data.
        - process_tile: Member of the process tile pyramid (not necessarily
            the output pyramid, if output has a different metatiling setting)
        - overwrite: overwrite existing data (default: True)
        """
        if isinstance(process_tile, Tile):
            process_tile = BufferedTile(
                process_tile, pixelbuffer=self.config.process_pixelbuffer)
        elif isinstance(process_tile, BufferedTile):
            pass
        else:
            raise ValueError("invalid process_tile type for execute()")
        # Do nothing if tile exists or overwrite is turned off.
        if not overwrite and all(
            tile.exists() for tile in self.output.tiles(process_tile)
        ):
            return (process_tile.id, "exists", None)

        # TODO If baselevel is active and zoom is outside of baselevel,
        # interpolate.

        # Otherwise, load process source and execute.
        try:
            new_process = imp.load_source(
                self.process_name + "Process", self.config.process_file)
            tile_process = new_process.Process(
                config=self.config, tile=process_tile,
                params=self.config.at_zoom(process_tile.zoom)
            )
        except:
            return process_tile.id, "failed", traceback.print_exc()
        try:
            # Actually run process.
            process_data = tile_process.execute()
        except:
            return process_tile.id, "failed", traceback.print_exc()
        finally:
            tile_process = None
        # Analyze proess output.
        if isinstance(process_data, str):
            if process_data == "empty":
                return (process_tile.id, "empty", None)
            else:
                return (process_tile.id, "custom", process_data)
        elif isinstance(
            process_data, (dict, tuple, np.ndarray, ma.MaskedArray)
        ):
            process_tile.data = process_data
            return process_tile
        else:
            raise RuntimeError(
                "not a valid process output: %s" % type(process_data))

    def read(self, output_tile):
        """
        Read from written process output.

        Return output_tile with appended data.
        - output_tile: Member of the output tile pyramid (not necessarily
            the process pyramid, if output has a different metatiling setting)
        """
        raise NotImplementedError

    def write(self, process_tile, overwrite=True):
        """
        Write data into output format.

        - process_tile: the process_tile with appended data
        - overwrite: overwrite existing data (default: True)
        """
        # Use self.config.output.write() function
        try:
            self.config.output.write(process_tile, overwrite=True)
        except:
            raise

    def get_view_output(self, output_tile, overwrite=True):
        """
        Get output as a file object.

        read() or execute() (and optional write()), convert to view output.
        Return output_tile with appended output data.
        - output_tile: Member of the output tile pyramid (not necessarily
            the process pyramid, if output has a different metatiling setting)
        - overwrite: overwrite existing data (default: True)
        """
        raise NotImplementedError

    def get_raw_output(self, output_tile, overwrite=True):
        """
        Get output raw data.

        read() or execute() (and optional write()). Return output_tile with
        appended output data.
        - output_tile: Member of the output tile pyramid (not necessarily
            the process pyramid, if output has a different metatiling setting)
        - overwrite: overwrite existing data (default: True)
        """
        raise NotImplementedError


class BufferedTile(Tile):
    """A special tile with fixed pixelbuffer."""

    def __init__(self, tile, pixelbuffer=0):
        """Initialize."""
        Tile.__init__(self, tile.tile_pyramid, tile.zoom, tile.row, tile.col)
        self._tile = tile
        self.pixelbuffer = pixelbuffer
        self.data = None

    @cached_property
    def profile(self):
        """Return a rasterio profile dictionary."""
        out_meta = self.output.profile
        out_meta.update(
            width=self.width,
            height=self.height,
            transform=None,
            affine=self.affine
            )
        return out_meta

    @cached_property
    def height(self):
        """Return buffered height."""
        return self._tile.shape(pixelbuffer=self.pixelbuffer)[0]

    @cached_property
    def width(self):
        """Return buffered width."""
        return self._tile.shape(pixelbuffer=self.pixelbuffer)[1]

    @cached_property
    def affine(self):
        """Return buffered Affine."""
        return self._tile.affine(pixelbuffer=self.pixelbuffer)

    @cached_property
    def bounds(self):
        """Return buffered bounds."""
        return self._tile.bounds(pixelbuffer=self.pixelbuffer)

    @cached_property
    def bbox(self):
        """Return buffered bounding box."""
        return self._tile.bbox(pixelbuffer=self.pixelbuffer)


class MapcheteProcess(object):
    """
    Actual process running on a tile.

    In fact, it is a Tile.
    Its attributes and methods can be accessed via "self" from within a
    Mapchete process Python file.
    """

    def __init__(self, tile, config=None, params=None):
        """Initialize Mapchete process."""
        self.identifier = ""
        self.title = ""
        self.version = ""
        self.abstract = ""
        self.tile = tile
        self.tile_pyramid = tile.tile_pyramid
        self.params = params
        self.config = config

    def write(self, data, **kwargs):
        """Deprecated."""
        raise DeprecationWarning(
            "Please return process output data instead of using self.write().")

    def open(self, input_file, **kwargs):
        """
        Return appropriate InputTile object.

        - input_file: file path or file name from configuration file
        - **kwargs: driver specific parameters (e.g. resampling)
        """
        if input_file not in self.params["input_files"]:
            raise ValueError(
                "%s not found in config as input file" % input_file)
        return self.params["input_files"][input_file].open(self.tile, **kwargs)

    def hillshade(
        self, elevation, azimuth=315.0, altitude=45.0, z=1.0, scale=1.0
    ):
        """
        Calculate hillshading from elevation data.

        Returns an array with the same shape as the input array.
        - elevation: input array
        - azimuth: horizontal angle of light source (315: North-West)
        - altitude: vertical angle of light source (90 would result in slope
                    shading)
        - z: vertical exaggeration
        - scale: scale factor of pixel size units versus height units (insert
                 112000 when having elevation values in meters in a geodetic
                 projection)
        """
        return mapchete.commons.hillshade(
            elevation, self, azimuth, altitude, z, scale)

    def contours(
        self, elevation, interval=100, field='elev'
    ):
        """
        Extract contour lines from elevation data.

        Returns contours as GeoJSON-like pairs of properties and geometry.
        - elevation: input array
        - interval: elevation value interval
        - field: output field name containing elevation value
        """
        return mapchete.commons.contours(
            elevation, self.tile, interval=interval,
            pixelbuffer=self.pixelbuffer, field=field)

    def clip(
        self, array, geometries, inverted=False, clip_buffer=0
    ):
        """
        Return input array clipped by geometries.

        - inverted: bool, invert clipping
        - clip_buffer: int (in pixels), buffer geometries befor applying clip
        """
        return mapchete.commons.clip_array_with_vector(
            array, self.tile.affine(self.pixelbuffer), geometries,
            inverted=inverted, clip_buffer=clip_buffer*self.tile.pixel_x_size)
